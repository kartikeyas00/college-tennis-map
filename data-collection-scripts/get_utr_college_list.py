import requests
import sqlite3 
from dotenv import load_dotenv
import os


load_dotenv()
DATABASE_PATH=os.getenv('DATABASE_PATH')

url = "https://api.universaltennis.com/v2/search/colleges?top=10000&skip=0&utrType=verified&utrTeamType=singles&schoolClubSearch=true&searchOrigin=searchPage&sort=school.power6%3Adesc"

data = requests.get(url)
data = data.json()['hits']

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

sql = ''' INSERT INTO college(utr_id,name,division,gender,conference,city,state,latLong,url)
              VALUES(?,?,?,?,?,?,?,?,?) '''
i = 1
for datum in data:
    print(i)
    utr_id = datum['id']
    name = datum['source']['school']['name']
    division  =  datum['source']['school']['conference']['division']['divisionName'] if datum['source']['school']['conference'] else None
    gender  = datum['source']['gender']
    conference  = datum['source']['school']['conference']['conferenceName'] if datum['source']['school']['conference'] else None
    city = datum['source']['location']['cityName']
    state = datum['source']['location']['stateName']
    latLong = ','.join(map(str,datum['source']['location']['latLng']))
    url = datum['source']['url']
    
    college = (utr_id,name,division,gender,conference,city,state,latLong,url)
    cur = conn.cursor()
    cur.execute(sql, college)
    conn.commit()
    i = i + 1
    
conn.close()