import os
import sqlite3
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO


VALIDFOLDERS = [
    'dumps',
    'fetched'
]

def getCredentials():
    if 'LFCLIENTID' not in os.environ:
        raise ValueError('`LFCLIENTID` environment variable must be set.')
    if 'LFCLIENTSECRET' not in os.environ:
        raise ValueError('`LFCLIENTSECRET` environment variable must be set.')
    return os.environ['LFCLIENTID'], os.environ['LFCLIENTSECRET']

def get_dbpath():
    if 'LFFOLDER' not in os.environ:
        raise ValueError('`LFFOLDER` environment variable must be set.')
    return os.path.join(os.environ['LFFOLDER'], 'fundpet.db')

def get_folder(folder):
    if folder not in VALIDFOLDERS:
        raise ValueError(
            f'Wrong folder kind `{kind}` must be one of {VALIDFOLDERS}.')
    folder_path = os.path.join(os.environ['LFFOLDER'], folder)
    os.makedirs(folder_path, exist_ok=True)
    return folder_path


def get_path(platform, query, start, end, folder):
    filename = '%s_%s_%s_start_%s_end_%s.csv' % (
        folder, platform, query, start, end)
    outpath = os.path.join(get_folder(folder), filename)
    return outpath


def get_fetch_paths(query):
    output_dir = os.path.join(get_folder('fetched'), query)
    os.makedirs(output_dir, exist_ok=True)
    urls_to_fetch_file = os.path.join(output_dir, 'to_fetch_%s.csv' % (query))
    output = os.path.join(output_dir, 'minet_output_%s.csv' % (query))
    return urls_to_fetch_file, output, output_dir


def get_date(start, end):
    """
    If no start or end date, download last week from last monday

    # ORIGINAL FILE COMMENT :
    # These dates should cover the collection launched on Linkfluence dashboard
    # start_timestamp = "2023-05-10T00:00:00-00:00"
    # end_timestamp = "2023-07-27T23:59:59-00:00"

    """
    if not (start and end):
        today = date.today()
        last_monday = today + relativedelta(weekday=MO(-1))
        last_last_monday = today + relativedelta(weekday=MO(-2))
        end_timestamp = last_monday.strftime("%Y-%m-%dT00:00:00-00:00")
        start_timestamp = last_last_monday.strftime("%Y-%m-%dT00:00:00-00:00")
    else:
        end_timestamp = end
        start_timestamp = start

    return start_timestamp, end_timestamp
