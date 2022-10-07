import os
from pickle import TRUE
import pandas as pd
import concurrent.futures
from sqlalchemy import true
import yagmail
import unittest, time, re

from flask import Flask, redirect, url_for, request, send_file, json, Response
from bson import json_util
from urllib import response
from datetime import datetime
from datetime import timedelta
from zipcode_config import mongoDB
from bson.objectid import ObjectId
from functools import reduce
from bson.json_util import dumps, loads
import numpy as np

collection_tripSummaryReport = mongoDB.tripSummaryReport
collection_site = mongoDB.site
collection_user = mongoDB.user
collection_vehicle = mongoDB.vehicle
collection_userShift = mongoDB.userShift
collection_userProfile = mongoDB.userProfile
collection_beneficiary = mongoDB.beneficiary

def send_response(body):
    return Response(body, mimetype='application/json')

def roleReplase(roleNum):
    if roleNum == 1:
        return 'ASSOCIATE'
    elif roleNum == 5:
        return 'DRIVER'
    elif roleNum == 10:
        return 'DRIVER_AND_ASSOCIATE'
    elif roleNum == 19:
        return 'PROCESS_ASSOCIATE'
    elif roleNum == 20:
        return 'SITE_SUPERVISOR'
    elif roleNum == 25:
        return 'SHIFT_LEAD'
    elif roleNum == 26:
        return 'HUB_MANAGER'
    elif roleNum == 30:
        return 'CLUSTER_MANAGER'
    elif roleNum == 31:
        return 'OPS_MANAGER'
    elif roleNum == 35:
        return 'CITY_MANAGER'
    elif roleNum == 45:
        return 'SUPER_USER'

def tvr_process():
    # if 'https://qaap.whizzard.in' in request.origin:
    origin_ = request.origin

    fromDateStr = request.form['fromDateStr']
    fromDateTimeChe = datetime.strptime(fromDateStr, '%Y-%m-%d')
    fromDateTime = datetime.strptime(fromDateStr, '%Y-%m-%d')

    toDateStr = request.form['toDateStr']
    toDateTimeChe = datetime.strptime(toDateStr, '%Y-%m-%d')
    date_temp = datetime.strptime(toDateStr, '%Y-%m-%d')
    toDateTime = date_temp + timedelta(days=1)

    from_date = fromDateTimeChe.strftime("%Y-%B-%d")
    to_string = toDateTimeChe.strftime("%Y-%B-%d")

    monthString = ""
    yearString = ""
    print('-----------------------------', fromDateTimeChe, toDateTimeChe)

    c = [
            'Ops Manager', 'Cluster Manager', 'Date', 'Station', 'Vehicle Type', 'Name', 'Phone Number', 'Role',
            'Client Employee Id', 'Vehicle Number', 'Attendance', 'Model', 'Trip Type', 'ClientUserId', 'Trip Number',
            'Expected Start Time', 'Attendance Marked Time', 'Shift Started Time', 'ActualStartTime - ExpectedStartTime',
            'Shift Status', 'Ended At Site', 'Shift Ended By Supervisor / Reason', 'TripSheet Id', 'Route', 'Load', 'Touch Point', 'Packages',
            'Total Deliveries', 'Rejected Count', 'TouchPointsNotCovered', 'CustomerReturnsPickedUp Count',
            'CustomerReturnsCancelled Count', 'SMD Delivered Count', 'SMD Rejected Count', 'Mattress Delivered Count',
            'Mattress Rejected Count', 'Furniture Delivered Count', 'furnitureRejectedCount', 'C_Returns(Picked_Up)',
            'Shift Ended Time', 'Starting KM', 'Ending KM', 'Shift Duration', 'Total KM', 'Status', 'Shift Type', 'Beneficiary Name',
            'Account Number', 'PAN', 'Amount', 'Payment Mode', 'Verified By', 'Penalty', 'Penalty Reason', 'ShortCash','Created By',
            'Remarks', 'Rejected Reasons', 'Rejected By'
        ]

    l = []
    df_tripSummaryReport = pd.DataFrame(list(collection_tripSummaryReport.find({"reportDate": {"$gte": fromDateTime, "$lte": toDateTime}})))

    def loadData(row):
        site_id = row['siteId']
        find_result_site = collection_site.find_one(ObjectId(site_id))

        try:
            find_result_operations_manager = collection_user.find_one({"deleted": False, "status": "ACTIVATED", "siteIds": {"$in": [site_id]}, "role": {"$in": [31]}})
            om_name = find_result_operations_manager['fullName']
        except Exception as e:
            om_name = ''

        try:
            find_result_cluster_manager = collection_user.find_one({"deleted": False, "status": "ACTIVATED", "siteIds": {"$in": [site_id]}, "role": {"$in": [30]}})
            cluster_manager_name = find_result_cluster_manager['fullName']
        except Exception as e:
            cluster_manager_name = ''

        date_str = row['reportDate'].strftime('%Y-%m-%d')

        try:
            station = find_result_site['siteCode']
        except Exception as e:
            station = ''

        vehicleType = row['vehicleType']

        try:
            find_result_user = collection_user.find_one(ObjectId(row['userId']))
            user_name = find_result_user['fullName']
            mobile = find_result_user['userName']
        except Exception as e:
            user_name = ''
            mobile = ''
        role = row['role']

        clientEmployeeId = row['clientEmployeeId']

        userId = row['userId']
        find_result_vehicle = collection_vehicle.find_one({"deleted": False, "mappedUserIds": {"$in": [userId]}})

        try:
            vehicleNumber = find_result_vehicle['vehicleRegistrationNumber']
        except Exception as e:
            vehicleNumber = ''
        try:
            model = find_result_vehicle['model']
        except Exception as e:
            model = ''
        attendanceFlag = row['attendance']
        if attendanceFlag:
            attendance = 'Present'
        else:
            attendance = 'Absent'

        tripType = row['tripType']

        clientUserId = row['clientUserId']

        tripNumber = row['tripNumber']

        try:
            expectedStartTime = row['expectedStartTime'].strftime('%d/%m/%Y %H:%M %p')  # .strftime('%H:%M %p')
        except Exception as e:
            expectedStartTime = ''
        try:
            attendanceMarkedTime = row['attendanceMarkedTime'].strftime('%d/%m/%Y %H:%M %p')
        except Exception as e:
            attendanceMarkedTime = ''
        try:
            shiftStartedTime = row['shiftStartedTime'].strftime('%d/%m/%Y %H:%M %p')
        except Exception as e:
            shiftStartedTime = ''

        try:
            actualMinusExpectedTime_delta = row['shiftStartedTime'] - row['expectedStartTime']

            days = actualMinusExpectedTime_delta.days
            seconds = actualMinusExpectedTime_delta.seconds

            hours = seconds // 3600

            minutes = (seconds // 60) % 60

            actualMinusExpectedTime = "{0} days {1} hours {2} minutes".format(days, hours, minutes)
        except Exception as e:
            actualMinusExpectedTime = ''

        shiftId = row['shiftId']

        find_result_userShift = collection_userShift.find_one(ObjectId(shiftId))  # ,{'_id':0,'status':1}
        shiftStatus = find_result_userShift['status']
        if find_result_userShift['endedAtSite']:
            endedAtSite = "Yes"
        else:
            endedAtSite = "No"

        IsshiftEndedBySupervisor = False
        try:
            find_shiftEndedBy = collection_user.find_one(ObjectId(row['shiftEndedBy']), {'fullName'})
            shiftEndedBy_name = find_shiftEndedBy['fullName']
            IsshiftEndedBySupervisor = True
        except Exception as e:
            shiftEndedBy_name = ''
        shiftEndingReason = row['reasonToEndShift']

        if IsshiftEndedBySupervisor:
            shiftEndedBySupervisor_Reason = shiftEndedBy_name + " / " + str(shiftEndingReason)

        else:
            shiftEndedBySupervisor_Reason = ''

        tripSheetId = row['tripSheetId']
        route = row['route']
        load = row['load']

        touchPoints = row['touchPoints']

        # try:
        #    packages=find_result_userShift['deliveredPackagesInfo']
        #    packages_df=pd.DataFrame(packages)
        #    print('d')
        # except Exception as e:
        #    packages=''

        totalDeliveries = row['totalDeliveries']

        rejectedCount = row['rejectedCount']

        touchPointsNotCovered = row['touchPointsNotCovered']

        customerReturnsPickedUpCount = row['customerReturnsPickedUpCount']

        customerReturnsCancelledCount = row['customerReturnsCancelledCount']

        smdDeliveredCount = row['smdDeliveredCount']

        smdRejectedCount = row['smdRejectedCount']

        mattressDeliveredCount = row['mattressDeliveredCount']

        mattressRejectedCount = row['mattressRejectedCount']

        furnitureDeliveredCount = row['furnitureDeliveredCount']

        furnitureRejectedCount = row['furnitureRejectedCount']

        customerReturnsPickedUpCount = row['customerReturnsPickedUpCount']

        try:
            shiftEndedTime = row['shiftEndedTime'].strftime('%d/%m/%Y %H:%M %p')
        except Exception as e:
            shiftEndedTime = ''

        startingKm = row['startingKM']

        endingKm = row['endingKm']

        try:
            shiftDuration_delta = row['shiftEndedTime'] - row['shiftStartedTime']

            days = shiftDuration_delta.days
            seconds = shiftDuration_delta.seconds

            hours = seconds // 3600

            minutes = (seconds // 60) % 60

            shiftDuration = "{0} hours {1} minutes".format(hours, minutes)
        except Exception as e:
            shiftDuration = ''

        totalKM = endingKm - startingKm

        verified = row['verified']
        rejected = row['rejected']

        if verified:
            status = 'verfied'

        if not verified:
            status = 'Un-Verified'

        if rejected:
            status = 'rejected'

        shiftType = row['tripType']

        # beneficiary
        find_result_userProfile = collection_userProfile.find_one({"userId": userId})
        try:
            isBeneficiary = find_result_userProfile['beneficiary']
            if isBeneficiary:

                beneficiaryId = find_result_userProfile['beneficiaryId']

                find_result_beneficiary = collection_beneficiary.find_one(ObjectId(beneficiaryId))
                beneficiaryName = find_result_beneficiary['beneficiaryName']
                account_no = find_result_beneficiary['bankAccountNumber']
                pan = find_result_beneficiary['panNumber']

            else:
                # no beneficiary attached to this user
                beneficiaryName = ''
                account_no = ''
                pan = ''
        except Exception as e:
            # no beneficiary
            beneficiaryName = ''
            account_no = ''
            pan = ''

        amount = ''
        paymentMode = ''

        verifiedBy = row['verifiedBy']

        Penalty = row['penalty']

        penaltyReason = row['penaltyReason']

        shortCash = row['shortCash']

        try:
            findshiftCreatedBy = collection_user.find_one(ObjectId(find_result_userShift['updatedBy']))
            shiftCreatedBy = findshiftCreatedBy['fullName']
        except Exception as e:
            shiftCreatedBy = ''

        remarks = row['reasonToEndShift']

        rejectedReason = row['rejectionReasons']

        try:
            findrejectedBy = collection_user.find_one(ObjectId(row['rejectedBy']))
            rejectedBy = findrejectedBy['fullName']
        except Exception as e:
            rejectedBy = ''

        totalattempts = row['attempts']

        sellerReturnDeliveredCount = row['srDeliveredCount']

        sellerReturnsAttemptedCount = row['srAttemptedCount']

        sellerReturnsRejectedCount = row['srRejectedCount']
        sellerPickUpPickedUpCount = row['spPickedCount']

        sellerPickUpCancelledCount = row['spCancelledCount']

        packageDetails = f'totalDeliveries: {totalDeliveries} ,' \
                         f'attempts: {totalattempts}, rejectedCount: {rejectedCount},' \
                         f'customerReturnsPickedUpCount: {customerReturnsPickedUpCount}, ' \
                         f'customerReturnsCancelledCount: {customerReturnsCancelledCount}, sellerReturnsDeliveredCount: {sellerReturnDeliveredCount}, ' \
                         f'sellerReturnsAttemptedCount: {sellerReturnsAttemptedCount}, sellerReturnsRejectedCount: {sellerReturnsRejectedCount}, ' \
                         f'sellerPickUpPickedUpCount: {sellerPickUpPickedUpCount}, sellerPickUpCancelledCount: {sellerPickUpCancelledCount}'.replace(
            'nan', '0')

        l.append([
                om_name, cluster_manager_name, date_str, station, vehicleType, user_name, mobile, role, clientEmployeeId, vehicleNumber,
                attendance, model, tripType, clientUserId, tripNumber, expectedStartTime, attendanceMarkedTime, shiftStartedTime,
                actualMinusExpectedTime, shiftStatus, endedAtSite, shiftEndedBySupervisor_Reason, tripSheetId, route, load, touchPoints, packageDetails,
                totalDeliveries, rejectedCount, touchPointsNotCovered, customerReturnsPickedUpCount, customerReturnsCancelledCount,
                smdDeliveredCount, smdRejectedCount, mattressDeliveredCount, mattressRejectedCount, furnitureDeliveredCount,
                furnitureRejectedCount, customerReturnsPickedUpCount, shiftEndedTime, startingKm, endingKm, shiftDuration, totalKM,
                status, shiftType, beneficiaryName, account_no, pan, amount, paymentMode, verifiedBy, Penalty, penaltyReason, shortCash,
                shiftCreatedBy, remarks, rejectedReason, rejectedBy
            ])

    e_temp = []
    start = datetime.now()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        res = [executor.submit(loadData, row) for index, row in df_tripSummaryReport.iterrows()]

    end = datetime.now()

    time_taken = end - start
    print('Time: ', time_taken)

    main_df = pd.DataFrame(l, columns=c)

    fpath_list = []
    my_path = "/home/durgaprasad/code/python/whizzard-python/dump/tvr_c_files/"
    fileName = 'TripVerificationReport_'.format(monthString)
    filename_local = "{0}.csv.gz".format(fileName)
    print(main_df)
    main_df.to_csv(my_path + filename_local, index = False, compression="gzip")
    print(filename_local + " Exported")

    # adding filepath in list
    fpath_list.append(my_path + filename_local)

    yag = yagmail.SMTP(user = "analytics@whizzard.in", password = "anshu0000", host = 'smtp.gmail.com')
    # TO = 'analyticsteam@whizzard.in'
    TO = 'durgaprasad.kolli@whizzard.in'
    # # for testing
    CC = ['durgaprasad.kolli@whizzard.in']
    SUBJECT = "Trip Verification Report from {0} to {1} month {2}, year {3}".format(from_date, to_string, monthString, yearString)
    sending_name = 'Durgaprasad'
    CONTENTS = ['Hey {0}, \n\nPFA report\n\nIf any issue, please contact me.\n\nRegards,\n-Anshuman\ndurgaprasad.kolli@whizzard.in'.format(sending_name)]
    yag.send(to = TO, cc = CC, subject = SUBJECT, contents = CONTENTS + fpath_list)
    yag.send(to = TO, subject = SUBJECT, contents = CONTENTS + fpath_list)

    print("The End", my_path + filename_local, my_path, filename_local)

    os.remove(my_path + filename_local)
    print("File Deleted: " + str(my_path + filename_local))

    dataCode = 'Mail Sent'

    response = {
        'response': dataCode,
        'status': 200,
        'mimetype': 'application/json'
    }

    return response


def trip_Filter_process():
    fromDateStr = request.json['fromDateStr']
    fromDateTimeChe = datetime.strptime(fromDateStr, '%Y-%m-%d')
    fromDateTime = datetime.strptime(fromDateStr, '%Y-%m-%d')

    toDateStr = request.json['toDateStr']
    toDateTimeChe = datetime.strptime(toDateStr, '%Y-%m-%d')
    date_temp = datetime.strptime(toDateStr, '%Y-%m-%d')
    toDateTime = date_temp + timedelta(days=1)

    from_date = fromDateTimeChe.strftime("%Y-%B-%d")
    to_string = toDateTimeChe.strftime("%Y-%B-%d")

    monthString = ""
    yearString = ""

    appendList = []
    rmIds = []
    for rmId in request.json['rmIds']:
        omIds.append(ObjectId(rmId))
    df_rm_users = collection_user.find({'_id': {'$in': rmIds}},{'_id':1,"status":1,"deleted":1,"role":1,"siteIds":1,"fullName":1})
    def_rm_sites = []
    for rms in df_rm_users:
        for i in rms['siteIds']:
            def_rm_sites.append(rms['siteIds'])
    # print('1-------------', dumps(def_rm_sites))
    if len(def_rm_sites):
        appendList.append(def_rm_sites)

    omIds = []
    for id in request.json['omIds']:
        omIds.append(ObjectId(id))
    df_om_user = collection_user.find({'_id': {'$in': omIds}},{'_id':1,"status":1,"deleted":1,"role":1,"siteIds":1,"fullName":1})
    def_om_sites = []
    for oms in df_om_user:
        for j in oms['siteIds']:
            def_om_sites.append(j)
    # print('2-------------', def_om_sites)
    if len(def_om_sites):
        appendList.append(def_om_sites)

    # df_bu_site = collection_site.find({'businessUnit': {'$in': request.json['businessUnits']}},{'_id':1,"businessUnit":1,"name":1,"siteCode":1})
    df_bu_site = collection_site.find({'businessUnit': {'$in': request.json['businessUnits']}},{'_id':1})
    def_bu_sites = []
    for bu in df_bu_site:
        def_bu_sites.append(str(bu['_id']))
    # print('3-------------', dumps(def_bu_sites))
    if len(def_bu_sites):
        appendList.append(def_bu_sites)

    if len(request.json['siteIds']):
        appendList.append(request.json['siteIds'])

    # print(appendList, type(appendList), len(appendList))
    # dataArray = reduce(np.intersect1d, ([[1, 3, 4, 3], [3, 1, 2, 1], [6, 3, 1, 4, 2], [1,2,3,2,2,4,5]]))
    dataArray = reduce(np.intersect1d, (appendList))
    b = np.array(dataArray)
    fArray = []
    for k in dataArray:
        fArray.append(k)
    print(fArray, type(fArray))

    omSiteIds = []
    omFilterName = []
    om_sites = []
    for userSites in df_om_user:
        omFilterName.append(userSites['fullName'])
        for siteId in userSites['siteIds']:
            om_sites.append(siteId)
            for om in request.json['siteIds']:
                if om == siteId:
                    omSiteIds.append(siteId)

    c = [
        'Trip Date', 'UniqueShiftId', 'Site Code', 'Business', 'Regional Manager', 'Ops Manager', 'Cluster Manager', 'Role', 'Name', 'Phone Number', 'Client Employee Id',
        'Attendance', 'Trip Type', 'Vehicle Type', 'Vehicle Number', 'Payment Plan', 'Shift Type', 'Shift Status', 'Verification Status', 'Expected Start Time',
        'Expected End Time', 'Attendance Marked Time', 'Shift Started Time', 'Shift Ended Time', 'Starting KM', 'Ending KM', 'Total KM - User Entered', 'Total KM - System Captured',
        'Ended At Site', 'Verified By Role', 'Verified By', 'Verified At', 'Rejected By User Role', 'Rejected By UserName', 'Rejected On', 'Rejected Reasons', 'Created By', 'Ended By',
        'Reason', 'Beneficiary Name', 'Account Number', 'PAN', 'IFSC Code', 'Amount', 'Payment Mode'
    ]

    l=[]
    # df_tripSummaryReport = pd.DataFrame(list(collection_tripSummaryReport.find({"reportDate": {"$gte": fromDateTime, "$lte": toDateTime}, 'siteId':{"$in": omSiteIds}, })))
    df_tripSummaryReport = pd.DataFrame(list(collection_tripSummaryReport.find({"reportDate": {"$gte": fromDateTime, "$lte": toDateTime}, 'siteId':{"$in": fArray}, })))
    df_tripSummaryReport.fillna('NA', inplace = True)
    def loadData(row):
        tripDateStr = row['tripDateStr']
        # print(trip_date, '<<<<--trip_date-->>>>')

        uniqueShiftId = row['uniqueShiftId']
        # print(uniqueShiftId, '<<<<--uniqueShiftId-->>>>')

        site_id = row['siteId']
        find_result_site = collection_site.find_one(ObjectId(site_id))
        try:
            siteCode = find_result_site['siteCode']
            businessUnit = find_result_site['businessUnit']
        except Exception as e:
            siteCode = 'NA'
            businessUnit = 'NA'
        # print(siteCode, '<<<<--siteCode-->>>>')
        # print(businessUnit, '<<<<--businessUnit-->>>>')

        rmUserName = row['rmUserName']
        # print(rmUserName, '<<<<--rmUserName-->>>>')

        try:
            find_result_operations_manager = collection_user.find_one({"deleted": False, "status": "ACTIVATED", "siteIds": {"$in": [site_id]}, "role": {"$in": [31]}})
            omUserName = find_result_operations_manager['fullName']
        except Exception as e:
            omUserName = 'NA'
        # print(omUserName, '<<<<--omUserName-->>>>')

        try:
            find_result_cluster_manager = collection_user.find_one({"deleted": False, "status": "ACTIVATED", "siteIds": {"$in": [site_id]}, "role": {"$in": [30]}})
            clmUserName = find_result_cluster_manager['fullName']
        except Exception as e:
            clmUserName = 'NA'
        # print(clmUserName, '<<<<--clmUserName-->>>>')

        role = roleReplase(row['role'])
        # print(role, '<<<<--role-->>>>')

        try:
            find_result_user = collection_user.find_one(ObjectId(row['userId']))
            userName = find_result_user['fullName']
            phoneNumber = find_result_user['userName']
        except Exception as e:
            userName = ''
            phoneNumber = ''
        # print(userName, '<<<<--userName-->>>>')
        # print(phoneNumber, '<<<<--phoneNumber-->>>>')

        clientEmployeeId = row['clientEmployeeId']
        # print(clientEmployeeId, '<<<<--clientEmployeeId-->>>>')

        if row['shiftStartedTime'] and row['shiftEndedTime']:
            attendance = 'Present'
        else:
            attendance = 'Absent'
        # print(attendance, '<<<<--attendance-->>>>')

        tripType = row['tripType']
        # print(tripType, '<<<<--tripType-->>>>')

        vehicleType = row['vehicleType']
        # print(vehicleType, '<<<<--vehicleType-->>>>')

        vehicleRegNumber = row['vehicleRegNumber']
        # print(vehicleRegNumber, '<<<<--vehicleRegNumber-->>>>')

        planName = row['planName']
        # print(planName, '<<<<--planName-->>>>')

        if row['unRegisteredUserAdhocShift'] == True:
            unRegisteredUserAdhocShift = 'LiteUser'
        elif row['unRegisteredUserAdhocShift'] == False:
            unRegisteredUserAdhocShift = 'Regular'
        else:
            unRegisteredUserAdhocShift = '--'
        # print(unRegisteredUserAdhocShift, '<<<<--unRegisteredUserAdhocShift-->>>>')

        shiftStatus = row['shiftStatus']
        # print(shiftStatus, '<<<<--shiftStatus-->>>>')

        if row['rejected'] == True:
            verificationStatus = 'Rejected'
        elif row['verified'] == True:
            verificationStatus = 'Verified'
        else:
            verificationStatus = 'Un-Verified'
        # print(verificationStatus, '<<<<--verificationStatus-->>>>')

        try:
            expectedStartTime = row['expectedStartTime'].strftime('%d/%m/%Y %H:%M %p')  # .strftime('%H:%M %p')
        except Exception as e:
            expectedStartTime = ''
        # print(expectedStartTime, '<<<<--expectedStartTime-->>>>')

        try:
            expectedEndTime = row['expectedEndTime'].strftime('%d/%m/%Y %H:%M %p')  # .strftime('%H:%M %p')
        except Exception as e:
            expectedEndTime = ''
        # print(expectedEndTime, '<<<<--expectedEndTime-->>>>')

        try:
            attendanceMarkedTime = row['attendanceMarkedTime'].strftime('%d/%m/%Y %H:%M %p')
        except Exception as e:
            attendanceMarkedTime = ''
        # print(attendanceMarkedTime, '<<<<--attendanceMarkedTime-->>>>')

        try:
            shiftStartedTime = row['shiftStartedTime'].strftime('%d/%m/%Y %H:%M %p')
        except Exception as e:
            shiftStartedTime = ''
        # print(shiftStartedTime, '<<<<--shiftStartedTime-->>>>')

        try:
            shiftEndedTime = row['shiftEndedTime'].strftime('%d/%m/%Y %H:%M %p')
        except Exception as e:
            shiftEndedTime = ''
        # print(shiftEndedTime, '<<<<--shiftEndedTime-->>>>')

        startingKm = row['startingKM']
        # print(startingKm, '<<<<--startingKm-->>>>')

        endingKm = row['endingKm']
        # print(endingKm, '<<<<--endingKm-->>>>')

        if row['endingKm'] and row['startingKM']:
            totalKMUserEntered = row['endingKm'] - row['startingKM']
        else:
            totalKMUserEntered = 0
        # print(totalKMUserEntered, '<<<<--totalKMUserEntered-->>>>')

        systemCalculatedTripDistance = row['systemCalculatedTripDistance']
        # print(systemCalculatedTripDistance, '<<<<--systemCalculatedTripDistance-->>>>')

        shiftId = row['shiftId']
        find_result_userShift = collection_userShift.find_one(ObjectId(shiftId))  # ,{'_id':0,'status':1}
        shiftStatus = find_result_userShift['status']
        if find_result_userShift['endedAtSite']:
            endedAtSite = "Yes"
        else:
            endedAtSite = "No"
        # print(endedAtSite, '<<<<--endedAtSite-->>>>')
        
        verifiedByRole = roleReplase(row['verifiedByRole'])
        # print(verifiedByRole, '<<<<--verifiedByRole-->>>>')

        verifiedBy = row['verifiedBy']
        # print(verifiedBy, '<<<<--verifiedBy-->>>>')

        verifiedAt = row['verifiedAt']
        # print(verifiedAt, '<<<<--verifiedAt-->>>>')

        if row['rejected']:
            rejectedByUserRole = roleReplase(row['rejectedByUserRole'])
            rejectedByUserName = row['rejectedByUserName']
            rejectedOn = row['rejectedOn']
            rejectionReasons = row['rejectionReasons'][0]
        else:
            rejectedByUserRole = 'NA'
            rejectedByUserName = 'NA'
            rejectedOn = 'NA'
            rejectionReasons = 'NA'
        # print(rejectedByUserRole, '<<<<--rejectedByUserRole-->>>>')
        # print(rejectedByUserName, '<<<<--rejectedByUserName-->>>>')
        # print(rejectedOn, '<<<<--rejectedOn-->>>>')
        # rejectionReasons = row['rejectionReasons']
        # print(rejectionReasons, type(rejectionReasons), '<<<<--rejectionReasons-->>>>')

        shiftCreatedBy = row['shiftCreatedBy']
        # print(shiftCreatedBy, '<<<<--shiftCreatedBy-->>>>')

        shiftEndedByUserName = row['shiftEndedByUserName']
        # print(shiftEndedByUserName, '<<<<--shiftEndedByUserName-->>>>')

        if 'reasonToEndShift' in row:
            reasonToEndShift = row['reasonToEndShift']
        else:
            reasonToEndShift = ''
        # print(reasonToEndShift, '<<<<--reasonToEndShift-->>>>')

        userId = row['userId']

        find_result_userProfile = collection_userProfile.find_one({"userId": userId})
        try:
            isBeneficiary = find_result_userProfile['beneficiary']
            if isBeneficiary:
                beneficiaryId = find_result_userProfile['beneficiaryId']
                find_result_beneficiary = collection_beneficiary.find_one(ObjectId(beneficiaryId))
                beneficiaryName = find_result_beneficiary['beneficiaryName']
                accountNumber = find_result_beneficiary['bankAccountNumber']
                pan = find_result_beneficiary['panNumber']

            else:
                # no beneficiary attached to this user
                beneficiaryName = ''
                accountNumber = ''
                pan = ''
        except Exception as e:
            # no beneficiary
            beneficiaryName = ''
            accountNumber = ''
            pan = ''
        # print(beneficiaryName, '<<<<--beneficiaryName-->>>>')
        # print(accountNumber, '<<<<--accountNumber-->>>>')
        # print(pan, '<<<<--pan-->>>>')

        IFSCCode = row['ifscCode']
        # print(IFSCCode, '<<<<--IFSCCode-->>>>')

        tripAmount = row['tripAmount']
        # print(tripAmount, '<<<<--tripAmount-->>>>')

        adhocPaymentMode = row['adhocPaymentMode']
        # print(adhocPaymentMode, '<<<<--adhocPaymentMode-->>>>')

        l.append([
            tripDateStr, uniqueShiftId, siteCode, businessUnit, rmUserName, omUserName, clmUserName, role, userName, phoneNumber, clientEmployeeId,
            attendance, tripType, vehicleType, vehicleRegNumber, planName, unRegisteredUserAdhocShift, shiftStatus, verificationStatus, expectedStartTime,
            expectedEndTime, attendanceMarkedTime, shiftStartedTime, shiftEndedTime, startingKm, endingKm, totalKMUserEntered, systemCalculatedTripDistance,
            endedAtSite, verifiedByRole, verifiedBy, verifiedAt, rejectedByUserRole, rejectedByUserName, rejectedOn, rejectionReasons, shiftCreatedBy, shiftEndedByUserName,
            reasonToEndShift, beneficiaryName, accountNumber, pan, IFSCCode, tripAmount, adhocPaymentMode
        ])


    e_temp = []
    start = datetime.now()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        res = [executor.submit(loadData, row) for index, row in df_tripSummaryReport.iterrows()]

    end = datetime.now()

    time_taken = end - start
    print('Time: ', time_taken)

    main_df = pd.DataFrame(l, columns = c)

    fpath_list = []
    my_path = "/home/durgaprasad/code/python/whizzard-python/dump/tvr_c_files/"
    fileName = 'TripVerificationReport_'.format(monthString)
    filename_local = "{0}.csv.gz".format(fileName)
    main_df.to_csv(my_path + filename_local, index = False, compression="gzip")
    print(filename_local + " Exported")

    # adding filepath in list
    fpath_list.append(my_path + filename_local)

    yag = yagmail.SMTP(user = "analytics@whizzard.in", password = "anshu0000", host = 'smtp.gmail.com')
    # TO = 'analyticsteam@whizzard.in'
    TO = 'durgaprasad.kolli@whizzard.in'
    # # for testing
    CC = ['durgaprasad.kolli@whizzard.in']
    
    SUBJECT = "Trip Verification Report from {0} till {1} , Ops Manager({2})".format(from_date, to_string, omFilterName)
    sending_name = 'Durgaprasad'
    CONTENTS = ['Hey {0}, \n\nPFA report\n\nIf any issue, please contact me.\n\nRegards,\n-Anshuman\ndurgaprasad.kolli@whizzard.in'.format(sending_name)]
    # yag.send(to = TO, cc = CC, subject = SUBJECT, contents = CONTENTS + fpath_list)
    # yag.send(to = TO, subject = SUBJECT, contents = CONTENTS + fpath_list)

    print("The End", my_path + filename_local, my_path, filename_local)

    # os.remove(my_path + filename_local)
    print("File Deleted: " + str(my_path + filename_local))

    dataCode = 'Mail Sent'

    response = {
        'response': dataCode,
        # 'users': omFilterName,
        'status': 200,
        'mimetype': 'application/json'
    }

    # response = json_util.dumps(omSiteIds)
    # return send_response(response)
    return response