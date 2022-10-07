from ast import And, IsNot
import os
from pydoc import cli
from pymongo import MongoClient # import mongo client to connect  
import pprint
from flask_pymongo import PyMongo
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from urllib import parse
from urllib.request import urlopen
from urllib.request import Request, urlopen
import urllib.parse

dotenv_path = find_dotenv('environ.env')
load_dotenv(dotenv_path=dotenv_path)
user_name_analytics = os.environ.get("DB_USER")
pass_word_analytics = os.environ.get("DB_PASSWORD")
host_analytics = os.environ.get("HOST")
port_analytics = os.environ.get("PORT")
db_name_analytics = os.environ.get("DB_NAME")


if user_name_analytics and pass_word_analytics:
    client = MongoClient(f'mongodb://{user_name_analytics}:{pass_word_analytics}@{host_analytics}:{port_analytics}/{db_name_analytics}')
    db = client['whizzard']
else:
    client = MongoClient()
    mongoDB = client['wizzard']

print('<<<<<<<<<< ---- Mongo DB Connected: ', mongoDB, ' ---- >>>>>>>>>>')