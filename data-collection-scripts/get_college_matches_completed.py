import requests
import json

url = "https://prd-itat-kube-tournamentdesk-api.clubspark.pro/"
file_path = "raw_data/college_matches.json"

def run_query(uri, query, variables, statusCode, headers):
    request = requests.post(uri, json={'query': query, 
                                       "variables": variables}, 
                            headers=headers)
    if request.status_code == statusCode:
        return request.json()
    else:
        raise Exception(f"Unexpected status code returned: {request.status_code}")
        
query = \
"""
query dualMatchesPaginated($skip: Int!, $limit: Int!, $filter: DualMatchesFilter, $sort: DualMatchesSort) {
  dualMatchesPaginated(skip: $skip, limit: $limit, filter: $filter, sort: $sort) {
    totalItems
    items {
      id
      startDateTime {
        timezoneName
        noScheduledTime
        dateTimeString
        __typename
      }
      homeTeam {
        name
        abbreviation
        id
        division
        conference
        region
        score
        didWin
        sideNumber
        __typename
      }
      teams {
        name
        abbreviation
        id
        division
        conference
        region
        score
        didWin
        sideNumber
        __typename
      }
      isConferenceMatch
      gender
      webLinks {
        name
        url
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

results = []
skip =0
while skip < 16300:
    variables = {
      "skip": skip,
      "limit": 100,
      "sort": {
        "field": "START_DATE",
        "direction": "DESCENDING"
      },
      "filter": {
        "seasonStarting": "2021",
        "isCompleted": True
      }
    }

    result = run_query(url, query, variables, 200, headers)['data']['dualMatchesPaginated']['items']
    results.extend(result)
    skip = skip + 100
    print(skip)
with open(file_path, 'w') as f:
    json.dump(results, f)