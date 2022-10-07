import boto3
from flask import Flask, redirect, url_for, request, send_file,json
import asyncio
from io import BytesIO
import calendar
import pandas as pd
import numpy as np
import time
import asyncio
import nest_asyncio
from bson import ObjectId
from pymongo import MongoClient
from geopy.distance import geodesic
from sklearn.cluster import KMeans
from datetime import datetime
from threading import Thread
from zipcode_config import mongoDB


from flask_cors import CORS, cross_origin

from routes.Trip_verification_route import tvr_bp

collection_zipCodeClustering=mongoDB['zipCodeClustering']
collection_user=mongoDB['user']

app = Flask(__name__)

# CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
cors = CORS(app, resources={r"/process/*": {"origins": "*"}})



@app.route('/')
@cross_origin()
def home():
    return "<center> <h2>Please use /process </h2></center> "



@app.route('/process/getFileStatus')
@cross_origin()
def getFileStatus():

    origin_ = request.origin
    if "qaap" in origin_:
        fileData_df = pd.DataFrame(list(collection_zipCodeClustering.find({"origin":"https://qaap.whizzard.in"})))
        fileData_df.drop(fileData_df.columns[0], axis=1, inplace=True)
        returnFilesStatus=fileData_df.to_json(orient="records")

        response = app.response_class(
            response=returnFilesStatus,
            status=200,
            mimetype='application/json'
        )
        return response
    if 'adminpanel' in origin_:
        fileData_df = pd.DataFrame(list(collection_zipCodeClustering.find({"origin":"https://adminpanel.whizzard.in"})))
        fileData_df.drop(fileData_df.columns[0], axis=1, inplace=True)
        returnFilesStatus = fileData_df.to_json(orient="records")
        response = app.response_class(
            response=returnFilesStatus,
            status=200,
            mimetype='application/json'
        )

        return response



@app.route('/process/zipCode', methods=['GET', 'POST'])
@cross_origin()
def process():
    print('---------------------------------------',1)

    # if 'https://qaap.whizzard.in' in request.origin:
    origin_ = request.origin


    now = datetime.now()  # current date and time

    date_time = now.strftime("%d_%m_%Y_%H_%M_%S")
    keyName = 'Output_{0}.xlsx'.format(date_time)
    url = 'https://zip-code-clustering.s3.amazonaws.com/' + keyName

    max_dist = float(request.form['max_dist'])

    clusteringData = request.files['file']

    userId=request.form['uploaded_by']

    uploaded_on = now.strftime("%d-%m-%Y %H:%M:%S")
    print('---------------------------------------',request.form)
    findUserResult = collection_user.find_one(ObjectId(userId))
    print('---------------------------------------',findUserResult)

    uploaded_by=findUserResult['fullName']

    directory_df = pd.read_excel('Indian_pincodes_geocoded.xlsx')
    directory_df = directory_df.dropna()
    pincode_lat_directory = dict(zip(directory_df.pincode, directory_df.lat))
    pincode_long_directory = dict(zip(directory_df.pincode, directory_df.long))
    df = pd.read_excel(clusteringData)


    if (('Pin Code' in df.columns) and ('Daily Volume' in df.columns)):
        print(1)

        pincode_check = pd.to_numeric(df['Pin Code'], errors='coerce').notnull().all()
        pincode_valid = (df['Pin Code'].astype(str).map(len) == 6).all()
        dailyVolume_check = pd.to_numeric(df['Daily Volume'], errors='coerce').notnull().all()

        if dailyVolume_check:
            dailyVolume_valid = not (df['Daily Volume']>500).any()
        else:
            dailyVolume_valid = False

        if pincode_check and dailyVolume_check and pincode_valid and dailyVolume_valid:
            df['pincode_lat'] = df["Pin Code"].map(pincode_lat_directory)
            df['pincode_long'] = df["Pin Code"].map(pincode_long_directory)
            df = df.dropna()
            df = df.reset_index(drop=True)
            kmeans_df = df[['pincode_lat', 'pincode_long']]

            dataForCollection = {"name": keyName, "url": url, "status": "Pending"}

            mongoDB['zipCodeClustering'].update_one({'fileName': keyName}, {"$set": {'url': url,
                                                                                "status": "Pending",
                                                                                "userId":userId,
                                                                                "uploaded_by":uploaded_by,
                                                                                "uploaded_on":uploaded_on,
                                                                                "origin":origin_}}, upsert=True)

            thread = Thread(target=clustering_loop, args=(kmeans_df, df, max_dist, keyName))
            thread.daemon = True
            thread.start()

            response = app.response_class(
                response=json.dumps(dataForCollection),
                status=200,
                mimetype='application/json'
            )

            return response
        else:
            print(2)
            if (not pincode_check) or (not pincode_valid):
                dataForCollection = {"error": "Invalid Input",
                                     "type": "Invalid Pincode Found "}
                response = app.response_class(
                    response=json.dumps(dataForCollection),
                    status=400,
                    mimetype='application/json'
                )
                return response
            elif (not dailyVolume_check) or (not dailyVolume_valid):
                dataForCollection = {"error": "Invalid Input",
                                     "type": "Invalid Daily Volume Found "}
                response = app.response_class(
                    response=json.dumps(dataForCollection),
                    status=400,
                    mimetype='application/json'
                )
                return response

            else:
                dataForCollection = {"error": "Invalid Input",
                                     "type": "BAD REQUEST"}
                response = app.response_class(
                    response=json.dumps(dataForCollection),
                    status=400,
                    mimetype='application/json'
                )
                return response
    else:
        print(3)
        dataForCollection={"error":"Please use \'Pin Code\' and \'Daily Volume\' as column name.",
                           "type":"Input Column names incorrect"}
        response = app.response_class(
            response=json.dumps(dataForCollection),
            status=400,
            mimetype='application/json'
        )

        return response

def clustering_loop(k_df,t_df,max_d,k_name):


    clusters = range(1, len(k_df))
    geo_distances = []
    for k in clusters:
        model = KMeans(n_clusters=k)
        model.fit(k_df)
        prediction = model.predict(k_df)
        centroid_lats = get_cluster_centroids(model, prediction)[0]
        centroid_longs = get_cluster_centroids(model, prediction)[1]
        cent_df = pd.DataFrame()
        cent_df['pincode_lat'] = k_df['pincode_lat']
        cent_df['pincode_long'] = k_df['pincode_long']
        cent_df['Cent_lat'] = centroid_lats
        cent_df['Cent_long'] = centroid_longs
        geodist = geodesic_distance(cent_df)
        geo_distances.append(max(geodist))
    output = [idx for idx, element in enumerate(geo_distances) if clustering_condition(element, max_d)]
    ideal_clusters = output[0]+1
    final_model = KMeans(ideal_clusters)
    final_model.fit(k_df)
    prediction = final_model.predict(k_df)

    # Append the prediction to the original dataframe
    t_df["Cluster"] = prediction
    t_df["Cent_lat"] = get_cluster_centroids(final_model, prediction)[0]
    t_df["Cent_long"] = get_cluster_centroids(final_model, prediction)[1]
    t_df['dist_to_centroid'] = geodesic_distance(t_df)
    df = t_df.sort_values(by=['Cluster'])
    pivot_vol = pd.pivot_table(df, values='Daily Volume', index=['Cluster'], aggfunc=np.sum)
    pivot_vol['Daily Volume'] = pivot_vol['Daily Volume'].astype(int)
    pivot_vol = pivot_vol.sort_values(by=['Daily Volume'], ascending=False)

    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    # Write each dataframe to a different worksheet.
    pivot_vol.to_excel(writer, sheet_name='Cluster Daily Volumes')
    df.to_excel(writer, sheet_name='Cluster Info')

    writer.close()

    # zip-code-clustering bucket

    # go back to the beginning of the stream
    output.seek(0)

    # uploading to output to s3 and updating the
    s3 = boto3.resource('s3')
    bucket_name = 'zip-code-clustering'
    url = 'https://zip-code-clustering.s3.amazonaws.com/' + k_name
    s3.Bucket(bucket_name).put_object(Key=k_name, Body=output)

    dataForCollection = {"name": k_name, "url": url,"status":"done"}

    mongoDB['zipCodeClustering'].update_one({'fileName': k_name}, {"$set": {'url': url,"status":"Done"}}, upsert=True)

def clustering_condition(x, max_dist):
    return x <= max_dist


def get_cluster_centroids(model, prediction):
    centroid_lat = []
    centroid_long = []
    unique_labels = list(set(model.labels_))

    cent_lat = {}
    cent_long = {}

    for i in range(len(model.cluster_centers_)):
        centroid_lat.append(model.cluster_centers_[i][0])
    for i in range(len(model.cluster_centers_)):
        centroid_long.append(model.cluster_centers_[i][1])

    cent_lat = dict(zip(unique_labels, centroid_lat))
    cent_long = dict(zip(unique_labels, centroid_long))

    center_latitude = list(pd.Series(prediction).map(cent_lat))
    center_longitude = list(pd.Series(prediction).map(cent_long))

    return center_latitude, center_longitude

def geodesic_distance(df):
    distances = []

    for i in range(len(df)):
        geo_dist = geodesic((df['pincode_lat'][i], df['pincode_long'][i]), (df['Cent_lat'][i], df['Cent_long'][i])).km
        distances.append(geo_dist)
    return distances

app.register_blueprint(tvr_bp, url_prefix = '/process/tvr')


nest_asyncio.apply()

if __name__ == '__main__':
    app.run(threaded=True,port=5600, debug=True)
