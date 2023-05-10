import sqlite3 
import pandas as pd
from dotenv import load_dotenv
import os


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
df = pd.read_csv('raw_data/utr_ita_final_scrubbed_data.csv')
df = df.drop(columns=['id', 'timestamp', 'url'])
sql = ''' INSERT INTO college(utr_id,
                                name,
                                division,
                                gender,
                                conference,
                                city,
                                state,
                                latLong,
                                ita_website_url,
                                team_url,
                                google_map_link,
                                twitter_link,
                                facebook_link,
                                instagram_link,
                                location,
                                ita_school_id,
                                ita_team_id
                                )
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
i = 1
for row in df.itertuples(index=False,name=None):
    try:
        cur = conn.cursor()
        cur.execute(sql, row)
        conn.commit()
    except Exception as e:
        print(e)
    print(i)
    i = i + 1

conn.close()