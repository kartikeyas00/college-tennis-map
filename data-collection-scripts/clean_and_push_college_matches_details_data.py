import pandas as pd
import json
import sqlite3
from dotenv import load_dotenv
import os


load_dotenv()
DATABASE_PATH=os.getenv('DATABASE_PATH')

raw_data_path_1 = 'raw_data/college_matches_details_1.json'
raw_data_path_2 = 'raw_data/college_matches_details_2.json'


raw_data_file_1 = open(raw_data_path_1)
raw_data_file_2 = open(raw_data_path_2)

data_1 = json.load(raw_data_file_1)
data_2 = json.load(raw_data_file_2)
data = data_1+data_2

df = pd.DataFrame(columns=[
    'match_id', 'scoring_format', 'match_type', 'position',
    'team_1_did_win', 'team_1_score', 'team_2_did_win', 'team_2_score', 
    ])
error_data = []
i = 0
for datum in data:
    if i%1000 == 0:print(i)
    match_id = datum['id']
    scoring_format=datum['scoringFormat']
    for tie in datum['tieMatchUps']:
        if tie['status']!='COMPLETED':
            continue
        try:
            df_ties = pd.DataFrame(columns=[
                'match_id', 'scoring_format', 'match_type', 'position',
                'team_1_did_win', 'team_1_score', 'team_2_did_win', 'team_2_score', 
                ])
            df_ties['match_id'] = [match_id]
            df_ties['scoring_format'] = [scoring_format]
            df_ties['match_type'] = [tie['type']]
            df_ties['position'] = [tie['collectionPosition']]
            df_ties['team_1_did_win'] = [tie['side1']['didWin']]
            df_ties['team_1_score'] = [tie['side1']['score']['scoreString']]
            df_ties['team_2_did_win'] = [tie['side2']['didWin']]
            df_ties['team_2_score'] = [tie['side2']['score']['scoreString']]
            df = df.append(df_ties)
        except Exception as e:
            print(e)
            error_data.append(datum)
    i = i+1

raw_data_file_1.close()
raw_data_file_2.close()

df = pd.read_csv('raw_data/college_matches_details.csv')


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
sql = ''' INSERT INTO matchesDetails(match_id, 
                                scoring_format, 
                                match_type, 
                                position, 
                                team_1_did_win, 
                                team_1_score, 
                                team_2_did_win, 
                                team_2_score
                                )
              VALUES(?,?,?,?,?,?,?,?) '''
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
