import requests
import sqlite3 
import pandas as pd
import logging
from datetime import date
from dotenv import load_dotenv
import os


load_dotenv()
LOGS_PATH = os.getenv('LOGS_PATH')
DATABASE_PATH=os.getenv('DATABASE_PATH')

logging.basicConfig(
    filename= f"{LOGS_PATH}get_and_push_college_utr_data ({str(date.today())})",
    level=logging.DEBUG,
    format=" %(asctime)s -%(levelname)s - %(message)s",
)
logging.info("------------------------------------------------------------")
logging.info("Program Start...")
logging.info("Set App Parameters...")
    
    

url = "https://api.universaltennis.com/v2/search/colleges?top=10000&skip=0&utrType=verified&utrTeamType=singles&schoolClubSearch=true&searchOrigin=searchPage&sort=school.power6%3Adesc"

logging.info("Request UTR data...")
data = requests.get(url)
data = data.json()['hits']

df = pd.DataFrame(columns=['utr_id','utr'])

if data:

    for datum in data:
        df_temp = pd.DataFrame(columns=['utr_id','utr'])
        df_temp['utr_id'] = [datum['id']]
        df_temp['utr'] = datum['sorts']
        df = df.append(df_temp)
    
    logging.info("Complete massaging data into the sql format")

if not data:
    logging.error('No data is returned by the UTR')



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
        logging.error(repr(e))

    return conn

logging.info("Insert data to the Sqlite")
conn = create_connection(DATABASE_PATH)
sql = ''' INSERT INTO weeklyTeamUTR (utr_id, 
                            utr
                                )
              VALUES(?,?) '''
i = 1
for row in df.itertuples(index=False,name=None):
    try:
        cur = conn.cursor()
        cur.execute(sql, row)
        conn.commit()
    except Exception as e:
        logging.error(repr(e))
    i = i + 1

logging.info("Complete data insertion")

conn.close()
logging.info("Sqlite connection closed!")
