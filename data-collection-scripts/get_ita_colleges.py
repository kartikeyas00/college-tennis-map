import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex
import json
import random
import time

urls = ["https://www.wearecollegetennis.com/d1-men/",
        "https://www.wearecollegetennis.com/d1-women/",
        "https://www.wearecollegetennis.com/d2-men/",
        "https://www.wearecollegetennis.com/d2-women/",
        "https://www.wearecollegetennis.com/d3-men/",
        "https://www.wearecollegetennis.com/d3-women/",
        "https://www.wearecollegetennis.com/naia-men/",
        "https://www.wearecollegetennis.com/naia-women/",
        "https://www.wearecollegetennis.com/juco-men/",
        "https://www.wearecollegetennis.com/juco-women/"]
data = []
for url in urls:
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find_all("ul", class_="fa-ul")[2:]
    
    
    
    for r in results:
        
        r_list = r.find_all('li')
        for li in r_list:
            try:
                temp ={}
                temp['name'] = li.find_all('a')[0].text
                temp['ita_website_url'] = li.find_all('a')[0]['href']
                temp['team_url'] = li.find_all('a')[1]['href']
                temp['google_map_link'] = li.find_all('a')[2]['href']
                temp['twitter_link'] = li.find_all('a')[3]['href']
                temp['facebook_link'] = li.find_all('a')[4]['href']
                temp['instagram_link'] = li.find_all('a')[5]['href']
                temp['location'] = li.find_all('span')[1].text
                
                data.append(temp)
            except Exception as e:
                print(e)


def get_school_and_team_ids(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    scripts = soup.find(attrs={'id':'content'}).find('script')
    pattern = regex.compile(r'\{(?:[^{}]|(?R))*\}')
    id_dict = json.loads(pattern.findall(scripts.text)[1])
    return id_dict['schoolId'], id_dict['teamId']

c = 0
for d in data:
    try:
        schoolId, teamId = get_school_and_team_ids(d['ita_website_url'])
        d['schoolId'] = schoolId
        d['teamId'] = teamId
    except Exception as e:
        print(e)
        print(d['name'])
        d['schoolId'] = ''
        d['teamId'] = ''
    time.sleep(random.randint(1,5 ))
    print(c)
    c=c+1
df = pd.DataFrame(data)
df.to_csv('raw_data/college_details.csv', index=False)