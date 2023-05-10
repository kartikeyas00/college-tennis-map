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

def run_query(uri, query, variables, statusCode, headers):
    request = requests.post(uri, json={'query': query, 
                                       "variables": variables}, 
                            headers=headers)
    if request.status_code == statusCode:
        return request.json()
    else:
        raise Exception(f"Unexpected status code returned: {request.status_code}")

# Collect data for 2022-2023 season with season id 63977e81-421d-43cc-9932-eccdfa245b87

url = "https://prd-itat-kube.clubspark.pro/mesh-api/graphql"
file_path = "raw_data/college_roster_details_with_tennisid.json"
query = """
query getRosterMembers($rosterId: String!, $role: RosterRoleEnum!, $seasonId: String!) {
  getRosterMembers(rosterId: $rosterId, role: $role, seasonId: $seasonId) {
    personId
    tennisId
    standardGivenName
    standardFamilyName
    nationalityCode
    class
    avatarUrl
    __typename
  }
}

"""
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36"
    }
results = {}

for index, row in df.iterrows():
    print(index)
    variables = {
        "rosterId": row['ita_team_id'].lower(),
        "role": "PLAYER",
        "seasonId": "63977e81-421d-43cc-9932-eccdfa245b87"
        }
    try:
        result = run_query(url, query, variables, 200, headers)['data']['getRosterMembers']
    except Exception as e:
        result = ['Nothing']
    results[row['ita_team_id']] = result
    
    time.sleep(random.randint(1,3))
    
with open(file_path, 'w') as f:
    json.dump(results, f)