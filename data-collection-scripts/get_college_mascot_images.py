import pandas as pd
import requests
import time 
import random
import sqlite3 
import json
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_PATH=os.getenv('DATABASE_PATH')

img_url = "https://colleges.wearecollegetennis.com/{ita_team_id}/Logo"
img_storage_dir = "C:/Users/karti/Documents/Projects/College tennis predictions/data/raw_data/img/"

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Exception as e:
        print(e)

    return conn

conn = create_connection(DATABASE_PATH)

df = pd.read_sql("SELECT * FROM college", conn)
team_id_list = df['ita_team_id']

for ita_team_id in team_id_list:
    try:
        img_blob = requests.get(img_url.format(ita_team_id=ita_team_id), timeout=5).content
        with open(img_storage_dir + ita_team_id + '.png', 'wb') as img_file:
             img_file.write(img_blob)
    except Exception as e:
        print(e)