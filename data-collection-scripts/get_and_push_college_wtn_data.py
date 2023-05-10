import requests
import sqlite3 
import pandas as pd
import logging
from datetime import date
from dotenv import load_dotenv
import os
import urllib.parse
from bs4 import BeautifulSoup
import json

#### This script will aggregate(sum) top 6 WTN from a team. If a WTN is not given 
#### then it will be treates as 0.

load_dotenv()
LOGS_PATH = os.getenv('LOGS_PATH')
DATABASE_PATH=os.getenv('DATABASE_PATH')

logging.basicConfig(
    filename= f"{LOGS_PATH}get_and_push_college_wtn_data ({str(date.today())})",
    level=logging.DEBUG,
    format=" %(asctime)s -%(levelname)s - %(message)s",
)
#logging.info("------------------------------------------------------------")
#logging.info("Program Start...")
#logging.info("Set App Parameters...")

def get_wtn_sum(data):
    
    wtn_list = []
    for x in data:
        if x['worldTennisNumbers']:
            for i in x['worldTennisNumbers']:
                if  i['type']=='SINGLE':
                    wtn_list.append(i['tennisNumber'])
    
    wtn_list = sorted(wtn_list)[0:6]
    return sum(wtn_list)
    

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


def run_query(uri, query, variables, statusCode, headers):
    
    sa_key = '854607c20a27454ca63b9dd0747087f3'
    sa_api = 'https://api.scrapingant.com/v2/general'
    qParams = {'url': uri, 'x-api-key': sa_key}
    reqUrl = f'{sa_api}?{urllib.parse.urlencode(qParams)}'
    request = requests.post(reqUrl, json={'query': query, 
                                       "variables": variables}, 
                            headers=headers)
    if request.status_code == statusCode:
        soup = BeautifulSoup(request.content, 'html.parser')
        return json.loads(soup.text)
    else:
        raise Exception(f"Unexpected status code returned: {request.status_code}")

url = 'https://prd-itf-public.clubspark.pro/'
query = """
query persons($ids: [PersonIDInput]) {
  persons(filter: {ids: $ids}, pageArgs: {limit: 30}) {
    items {
      tennisID
      worldTennisNumbers {
        tennisNumber
        isRanked
        confidence
        type
        __typename
      }
      __typename
    }
    __typename
  }
}
"""

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36"
    }

df_schools = pd.read_sql("SELECT * FROM college", conn)

wtn_weekly_data = []

for index, row in df_schools.iterrows():
    ita_team_id  = row['ita_team_id']
    
    df_roster = pd.read_sql(f"SELECT * from roster where team_id = '{ita_team_id}'", con=conn)['tennis_id'].dropna()
    ids = []
    for tennis_id in df_roster:
        ids.append(
            {"identifier": tennis_id,
            "type": "TennisID"
                }
            )
    variables = {
        "ids":ids
        }
    try:
        result = run_query(url, query, variables, 200, headers)['data']['persons']['items']
        wtn_sum = get_wtn_sum(result)
        wtn_weekly_data.append({'ita_team_id':ita_team_id,
                                'wtn':    wtn_sum
            })
    except Exception as e:
        print(e)
        #logging.error(repr(e))
    
    

#logging.info("Complete transforming data into the list of dict so that it can be inserted to SQL")

#logging.info("Insert data to the Sqlite")


sql = ''' INSERT INTO weeklyTeamWTN (ita_team_id, wtn)
          VALUES(?,?) '''           
i = 1
for row in wtn_weekly_data:
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(row.values()))
        conn.commit()
    except Exception as e:
        logging.error(repr(e))
    i = i + 1           

conn.close()

logging.info("Sqlite connection closed!")
