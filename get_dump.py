#!/usr/bin/env python
# coding: utf-8

import os
import json
import requests
import pandas as pd
from itertools import chain
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO
from argparse import ArgumentParser
from utils import get_date, get_path

# parse arguments
ap = ArgumentParser()
ap.add_argument('--client_id', type=str)
ap.add_argument('--client_secret', type=str)
ap.add_argument('--start', type=str, default=None)
ap.add_argument('--end', type=str, default=None)
args = ap.parse_args()
client_id = args.client_id
client_secret = args.client_secret
start = args.start
end = args.end

# 0. Set things
########################################################

start_timestamp, end_timestamp = get_date(start, end)
dump_path = get_path(start_timestamp, end_timestamp, 'dumps')


# 1. Major parameters
########################################################

# queries only include Twitter, don't change
platforms = ["twitter", "facebook"]

# 10.000 is the max allowed by the API. Do not change this !!!!
query_size_limit = 10000

# don't change
collect_retweets = True


# 2. Authentication for the API (this remains unchanged)
########################################################

api_url = "https://radarly.linkfluence.com/1.0/"

# set your credentials
credentials = {
  "client_id": client_id,
  "client_secret": client_secret
}

data = 'grant_type=client_credentials&code=xxxxxx&client_id='
data += credentials["client_id"] + '&client_secret='
data += credentials["client_secret"] + '&scope=listening'

token_byte = requests.post(
    'https://oauth.linkfluence.com/oauth2/token',
    headers={'Content-Type': 'application/x-www-form-urlencoded'},
    data=data)

token = json.loads(token_byte.content.decode("utf-8"))["access_token"]

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + token}

# project ID // https://radarly.linkfluence.com/dashboard#/project/5560/
# This is the id of the radarly project dataset,remains unchanged
PID = 5760

# 3. Assembling a query
########################################################

# These are defined in the Linkfluence dashboard
queries = {
    'SoMe4DemDutch': 398400,
    'SoMe4DemFrench': 398394,
    'SoMe4DemGerman': 398397,
    'SoMe4DemItalian': 398396,
    'SoMe4DemPolish': 398398,
    # # 'SoMe4DemRomanian': 398399, # fails...
    'SoMe4DemSpanish': 398395,
}

query_list = []
for query, query_id in queries.items():
    query_list.append({"id": query_id, "include": True})

# 4. Retrieving documents with the query
########################################################

search_endpoint = api_url + "projects/" + str(PID) + "/inbox/search.json"

ts_end = pd.to_datetime(end_timestamp)

for platform in platforms:

    print('\n**Platform: %s' % platform)

    dfs = []
    for query_name, query_id in queries.items():
        print('Query: %s' % query_name)
        query_ended = False
        cell_start_timestamp = start_timestamp
        while not query_ended:
            payload = {
              "from": cell_start_timestamp,
              "to": end_timestamp,
              "platforms": [platform],
              "focuses": [{"id": query_id, "include": True}],
              "limit": query_size_limit,
              "sortOrder": "asc",
              "sortBy": "date",
              "flag": {'rt': collect_retweets}
              }

            req = requests.post(search_endpoint, headers=headers, json=payload)
            data = req.json()
            df = pd.json_normalize(data['hits'])
            df['query'] = query_name
            df['platform'] = platform
            df['language'] = query_name.replace('SoMe4Dem', '')
            # normalize url column name
            if platform == 'facebook':
                df['externalUrl'] = df['url.normalized']
            ts_col = df['date'].apply(pd.to_datetime)
            df = df.rename(columns={'user.id': f'{platform}_id'})
            dfs.append(df)
            print('%s to %s' % (df['date'].min(), df['date'].max()))
            if len(data['hits']) < query_size_limit or ts_col.max() > ts_end:
                query_ended = True
            else:
                cell_start_timestamp = df.loc[ts_col.argmax(), 'date']

platform_df = pd.concat(dfs, axis=0, ignore_index=True)

# emotions
emotions = list(set(list(chain(*platform_df['emotion']))))
for emotion in emotions:
    platform_df[emotion] = platform_df['emotion'].apply(
        lambda x: True if emotion in x else False)

platform_df.to_csv(dump_path, index=False)
print(f'Data saved at\n{dump_path}')
