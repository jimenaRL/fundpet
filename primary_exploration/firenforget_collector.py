#!/usr/bin/env python
# coding: utf-8


import requests
import json
import os
import pandas as pd
from itertools import chain

from argparse import ArgumentParser

# parse arguments
ap = ArgumentParser()
ap.add_argument('--client_id', type=str)
ap.add_argument('--client_secret', type=str)
args = ap.parse_args()
client_id = args.client_id
client_secret = args.client_secret

# Major parameters
########################################################

# Queries only include Twitter, don't change
platforms = ["twitter"]

# These dates should cover the collection launched on Linkfluence dashboard
start_timestamp = "2023-02-01T00:00:00-00:00"
end_timestamp = "2023-04-20T23:59:59-00:00"

# 10.000 is the max allowed by the API. Do not change this !!!!
query_size_limit = 10000

collect_only_retweets = False  # don't change

########################################################
########################################################
########################################################
########################################################

# Authentication for the API (this remains unchanged)
########################################################

api_url = "https://radarly.linkfluence.com/1.0/"

# set your credentials
credentials = {
  "client_id": client_id,
  "client_secret": client_secret
}

data = 'grant_type=client_credentials'
data += '&code=xxxxxx'
data += f'&client_id={credentials["client_id"]}'
data += f'&client_secret={credentials["client_secret"]}'
data += '&scope=listening'

token_byte = requests.post(
    'https://oauth.linkfluence.com/oauth2/token',
    headers={
        'Content-Type': 'application/x-www-form-urlencoded'
    },
    data=data
)

token = json.loads(token_byte.content.decode("utf-8"))["access_token"]

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + token
}

# project ID // https://radarly.linkfluence.com/dashboard#/project/5560/
# This is the id of the radarly project dataset, and remains unchanged
PID = 5760

# Assembling a query
########################################################

# These are defined in the Linkfluence dashboard
queries = {
#     'GermanFundraising':   385879,
#     'GermanPetitioning':   385878,
#     'FrenchFundraising':   385881,
#     'FrenchPetitioning':   385880,
#     'DutchFundraising':    385883,
#     'DutchPetitioning':    385882,
#     'PolishFundraising':   385885,
#     'PolishPetitioning':   385884,
#     'RomanianFundraising': 385887,
#     'RomanianPetitioning': 385886,
#     'ItalianFundraising':  385889,
#     'ItalianPetitioning':  385888,
#     'SpanishFundraising':  385891,
    # 'SpanishPetitioning':  385890,
}

query_list = []
for query, query_id in queries.items():
    query_list.append({"id": query_id, "include": True})

# Retrieving documents with the query
########################################################

search_endpoint = api_url + "projects/" + str(PID) + "/inbox/search.json"

########################################################
########################################################
########################################################
# Retrieving documents with the query
########################################################

ts_end = pd.to_datetime(end_timestamp)

for platform in platforms:
    print('\n**Platform: %s' % platform)
    for query_name, query_id in queries.items():
        query_df = []
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
                "flag": {'rt': collect_only_retweets}
            }

            req = requests.post(
                search_endpoint,
                headers=headers,
                json=payload
            )

            data = req.json()
            if "hits" in data:
                df = pd.json_normalize(data['hits'])
                df['query'] = query_name
                ts_col = df['date'].apply(pd.to_datetime)
                query_df.append(df)
                print(
                    'Window from %s to %s' % (df['date'].min(), df['date'].max()))

                if len(data['hits']) < query_size_limit or ts_col.max() > ts_end:
                    query_ended = True
                else:
                    cell_start_timestamp = df.loc[ts_col.argmax(), 'date']
            else:
                query_ended = True

        pd.concat(
            query_df,
            axis=0,
            ignore_index=True) \
            .rename(columns={'user.id': 'twitter_id'}) \
            .to_csv(
                '%s_%s_%s_start_%s_end_%s.csv.zip' % (
                    query_name,
                    query_id,
                    platform,
                    start_timestamp,
                    end_timestamp),
                index=False,
                compression='zip')


    # platform_df = pd.concat(
    #    dfs,
    #    axis=0,
    #    ignore_index=True) \
    #    .rename(columns={'user.id': 'twitter_id'})

    # Emotions
    # emotions = list(set(list(chain(*platform_df['emotion']))))
    # for emotion in emotions:
    #    platform_df[emotion] = platform_df['emotion'].apply(
    #        lambda x: True if emotion in x else False)

    # platform_df.to_csv(
    #    'full_%s__start_%s_end_%s.csv.zip' % (
    #     platform, start_timestamp, end_timestamp),
    #    index=False,
    #    compression='zip')

    # platform_df[col_keep+emotions].to_csv(
    #     'short_%s__start_%s_end_%s.csv.zip' % (
    #         platform, start_timestamp, end_timestamp),
    #     index=False,
    #     compression='zip')

    print('the end')
