#!/usr/bin/env python
# coding: utf-8

import os
import json
import requests
import pandas as pd
from itertools import chain
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO
from utils import get_date, get_path


def getDump(
    config,
    query,
    query_id,
    client_id,
    client_secret,
    platform,
    start,
    end,
    logger):

    # 1. Major parameters
    ########################################################

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


    # 3. Retrieving documents with the query
    ########################################################

    search_endpoint = api_url + "projects/" + str(PID) + "/inbox/search.json"

    ts_end = pd.to_datetime(end)

    logger.info('DUMP: platform: %s' % platform)

    query_ended = False
    cell_start = start
    dfs = []
    while not query_ended:
        payload = {
          "from": cell_start,
          "to": end,
          "platforms": [platform],
          "focuses": [{"id": query_id, "include": True}],
          "limit": query_size_limit,
          "sortOrder": "asc",
          "sortBy": "date",
          "flag": {'rt': collect_retweets}
          }

        req = requests.post(search_endpoint, headers=headers, json=payload)
        data = req.json()
        if data['total'] == 0:
            query_ended = True
        else:
            df = pd.json_normalize(data['hits'])
            df['query'] = query
            df['platform'] = platform
            # normalize url column name
            if platform == 'facebook':
                df['externalUrl'] = df['url.normalized']
            ts_col = df['date'].apply(pd.to_datetime)
            df = df.rename(columns={'user.id': f'{platform}_id'})
            dfs.append(df)
            logger.info('DUMP: %s to %s' % (df['date'].min(), df['date'].max()))
            if len(data['hits']) < query_size_limit or ts_col.max() > ts_end:
                query_ended = True
            else:
                cell_start = df.loc[ts_col.argmax(), 'date']

    if len(dfs) == 0:
        logger.info("DUMP: nothing found.")
        return

    df = pd.concat(dfs, axis=0, ignore_index=True)

    # emotions
    emotions = list(set(list(chain(*df['emotion']))))
    for emotion in emotions:
        df[emotion] = df['emotion'].apply(
            lambda x: True if emotion in x else False)

    dump_path = get_path(
        platform, query, start, end, 'dumps')
    df.to_csv(dump_path, index=False)

    logger.info(f'DUMP: found {len(df)} entries, data saved at {dump_path}')

    return dump_path