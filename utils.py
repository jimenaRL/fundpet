import os
import sqlite3
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO


VALIDFOLDERS = [
    'dumps',
    'fetched',
    'preprocessed',
    'scrapped'
]

if 'LFFOLDER' not in os.environ:
    raise ValueError('`LFFOLDER` environment variable must be set.')


def get_folder(folder):
    if folder not in VALIDFOLDERS:
        raise ValueError(
            f'Wrong folder kind `{kind}` must be one of {VALIDFOLDERS}.')

    return os.path.join(os.environ['LFFOLDER'], folder)


def get_dbpath():
    return os.path.join(os.environ['LFFOLDER'], 'fundpet.db')


def get_path(platform, query, start, end, folder):
    filename = '%s_%s_%s_start_%s_end_%s.csv' % (
        folder, platform, query, start, end)
    outpath = os.path.join(get_folder(folder), filename)
    return outpath


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


def populateSqlite(db_path, table, records, columns, dtype='TEXT'):

    if not os.path.exists(db_path):
        print(f"Creating new sqlite database at {db_path}.")

    # check if the table doesn't exists
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        query = """
            SELECT count(name)
            FROM sqlite_master
            WHERE type='table'
            AND name=? """
        cur.execute(query, (table, ))
        con.commit()
        res = cur.fetchall()

    table_exists = False if res[0][0] == 0 else True

    ncols = len(columns)
    qvals = ''.join('?,' * ncols)[:-1]
    schema = ' '.join([f"{k} TEXT, " for k in columns])
    uniques = ' '.join([f" {k}," for k in columns])[:-1]
    schema += f'UNIQUE({uniques})'

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        query = f'''CREATE TABLE {table}({schema})'''
        if not table_exists:
            cur.execute(query)
            print(f"SQLITE: table {table} created.")
        cur.executemany(
            f"INSERT OR REPLACE INTO {table} VALUES({qvals})", records)
        print(f"SQLITE: table {table} populated.")


def retrieve_table(db_path, table, verbose=False):

    query = f"SELECT * FROM {table}"

    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"Unnable to find database at: '{db_path}'.")

    if verbose:
        print(f"Quering sqlite database at {db_path} with `{query[:100]}`... ")

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute(query)
        columns = list(map(lambda x: x[0], cur.description))
        res = cur.fetchall()

    df = pd.DataFrame(res, columns=columns, dtype=str)

    return df
