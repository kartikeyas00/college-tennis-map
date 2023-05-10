import pandas as pd
import json
import pytz 
from datetime import datetime
import sqlite3
from dotenv import load_dotenv
import os


load_dotenv()
DATABASE_PATH=os.getenv('DATABASE_PATH')

raw_data_path = 'raw_data/college_matches.json'

raw_data_file = open(raw_data_path)

data = json.load(raw_data_file)

df = pd.DataFrame(columns=['match_id', 'team_1', 'team_1_score','team_1_did_win',
                           'team_2', 'team_2_score','team_2_did_win','gender',
                           'home_team', 'is_conference_match','start_date_time'])
index = 0
for datum in data:
    print(index)
    df_temp = pd.DataFrame(columns=['match_id', 'team_1', 'team_1_score','team_1_did_win',
                               'team_2', 'team_2_score','team_2_did_win','gender',
                               'home_team', 'start_date_time'])
    df_temp['match_id'] = [datum['id']]
    df_temp['team_1'] = [list(filter(lambda x: x['sideNumber']==1, datum['teams']))[0]['id']]
    df_temp['team_1_score'] = [list(filter(lambda x: x['sideNumber']==1, datum['teams']))[0]['score']]
    df_temp['team_1_did_win'] = [list(filter(lambda x: x['sideNumber']==1, datum['teams']))[0]['didWin']]
    df_temp['team_2'] =[list(filter(lambda x: x['sideNumber']==2, datum['teams']))[0]['id']]
    df_temp['team_2_score'] = [list(filter(lambda x: x['sideNumber']==2, datum['teams']))[0]['score']]
    df_temp['team_2_did_win'] = [list(filter(lambda x: x['sideNumber']==2, datum['teams']))[0]['didWin']]
    df_temp['gender'] = [datum['gender']]
    df_temp['home_team'] = [datum['homeTeam']['id'] if datum['homeTeam'] else None]
    df_temp['is_conference_match'] = [datum['isConferenceMatch']]
    df_temp['start_date_time'] = [(pytz.timezone(datum['startDateTime']['timezoneName'] if datum['startDateTime']['timezoneName'] else 'UTC').
                                  localize(datetime.strptime(datum['startDateTime']['dateTimeString'], '%Y-%m-%dT%H:%M:%S.%fZ'))).strftime('%Y-%m-%d %H:%M:%S%z')]
    df=df.append(df_temp)
    index +=1
    
#df.to_csv(raw_data_path.replace('college_matches.json','college_matches.csv'), index=False)

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
sql = ''' INSERT INTO matches(match_id, 
                                team_1, 
                                team_1_score, 
                                team_1_did_win, 
                                team_2, 
                                team_2_score, 
                                team_2_did_win, 
                                gender, 
                                home_team, 
                                is_conference_match, 
                                start_date_time
                                )
              VALUES(?,?,?,?,?,?,?,?,?,?,?) '''
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
raw_data_file.close()