import pandas as pd
import json
import sqlite3
from dotenv import load_dotenv
import os


load_dotenv()
DATABASE_PATH=os.getenv('DATABASE_PATH')

raw_data_path = 'raw_data/college_roster_details_with_tennisid.json'

raw_data_file = open(raw_data_path)

data = json.load(raw_data_file)


df = pd.DataFrame(columns=[
    'team_id',
    'person_id',
    'tennis_id',
    'class',
    'nationality_code',
    'family_name',
    'given_name'
    ])

for key, value in data.items():
    if data[key] and data[key][0]!='Nothing':
        for datum in data[key]:
            df_temp = pd.DataFrame(columns=[
                'team_id',
                'person_id',
                'tennis_id',
                'class',
                'nationality_code',
                'family_name',
                'given_name'
                ])
            df_temp['team_id'] = [key]
            df_temp['person_id'] = [datum['personId']]
            df_temp['tennis_id'] = [datum['tennisId']]
            df_temp['class'] = [datum['class']]
            df_temp['nationality_code'] = [datum['nationalityCode']]
            df_temp['family_name'] = [datum['standardFamilyName']]
            df_temp['given_name'] = [datum['standardGivenName']]
            df = df.append(df_temp)
raw_data_file.close()
           

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
sql = ''' INSERT INTO roster(team_id, 
                                person_id, 
                                tennis_id,
                                class, 
                                nationality_code, 
                                family_name, 
                                given_name
                                )
              VALUES(?,?,?,?,?,?,?) '''
i = 1
for row in df.itertuples(index=False,name=None):
    try:
        cur = conn.cursor()
        cur.execute(sql, row)
        conn.commit()
    except Exception as e:
        print(e)
        print(f'{i} did not work')
    print(i)
    i = i + 1

conn.close()
