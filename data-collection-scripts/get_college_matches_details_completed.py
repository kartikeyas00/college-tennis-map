import json 
import pandas as pd
import datetime
import requests
import time 
import random

df = pd.read_json('raw_data/college_matches.json')
df['match_id'] = df['id']
df['match_date'] = df['startDateTime'].apply(lambda x: datetime.datetime.strptime(x['dateTimeString'], "%Y-%m-%dT%H:%M:%S.%fZ"))
df['home_team'] = df['homeTeam'].apply(lambda x: x['id'] if x else None)
df['team_1_id'] = df['teams'].apply(lambda x: x[0]['id'])
df['team_1_did_win'] = df['teams'].apply(lambda x: x[0]['didWin'])
df['team_1_score'] = df['teams'].apply(lambda x: x[0]['score'])
df['team_2_id'] = df['teams'].apply(lambda x: x[1]['id'])
df['team_2_did_win'] = df['teams'].apply(lambda x: x[1]['didWin'])
df['team_2_score'] = df['teams'].apply(lambda x: x[1]['score'])
df['is_conference_match'] = df['isConferenceMatch']
df = df.drop(columns=['id', 'startDateTime', 'homeTeam', 'teams', 'isConferenceMatch','webLinks', '__typename',])
df = df.loc[6155:]

def run_query(uri, query, variables, statusCode, headers):
    request = requests.post(uri, json={'query': query, 
                                       "variables": variables}, 
                            headers=headers)
    if request.status_code == statusCode:
        return request.json()
    else:
        raise Exception(f"Unexpected status code returned: {request.status_code}")
url = "https://tournamentdesk.wearecollegetennis.com/tournamentdesk-api/graphql"
file_path = "raw_data/college_matches_details_2.json"

query = """
query dualMatch($id: ID!) {
  dualMatch(id: $id) {
    id
    contextualInfo {
      result
      didWin
      homeTeam
      thisTeam {
        name
        id
        __typename
      }
      opponent {
        name
        id
        __typename
      }
      __typename
    }
    startDateTime {
      timezoneName
      noScheduledTime
      dateTimeString
      __typename
    }
    homeTeam {
      name
      id
      sideNumber
      __typename
    }
    teams {
      name
      id
      score
      sideNumber
      division
      conference
      logo {
        url
        width
        __typename
      }
      abbreviation
      __typename
    }
    location
    scoringFormat
    isConferenceMatch
    isNeutralLocation
    doublesOrderFinish
    singlesOrderFinish
    tieMatchUps {
      id
      type
      status
      side1 {
        participants {
          firstName
          lastName
          personId
          __typename
        }
        score {
          scoreString
          sets {
            setScore
            tiebreakScore
            tiebreakSet
            didWin
            __typename
          }
          __typename
        }
        teamAbbreviation
        didWin
        __typename
      }
      side2 {
        participants {
          firstName
          lastName
          personId
          __typename
        }
        score {
          scoreString
          sets {
            setScore
            tiebreakScore
            tiebreakSet
            didWin
            __typename
          }
          __typename
        }
        teamAbbreviation
        didWin
        __typename
      }
      collectionPosition
      collectionId
      __typename
    }
    __typename
  }
}
"""
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36"
    }
results = []

for index, row in df.iterrows():
    print(index)
    variables = {
  "id": row['match_id']
}
    result = run_query(url, query, variables, 200, headers)['data']['dualMatch']
    results.append(result)
    time.sleep(random.randint(1,2))

with open(file_path, 'w') as f:
    json.dump(results, f)