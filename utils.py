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


def checkDBexists(db_path, raiseError=True):
    dbExists = os.path.exists(db_path)
    if dbExists:
        return True
    else:
        if raiseError:
            raise FileNotFoundError(
                f"Unnable to find database at: '{db_path}'.")
        else:
            return False

def createTableIfNotExists(db_path, table, columns, dtypes=None):

    checkDBexists(db_path, raiseError=False)

    ncols = len(columns)
    qvals = ''.join('?,' * ncols)[:-1]
    if not dtypes:
        schema = ' '.join([f"{k} TEXT, " for k in columns])
    else:
        schema = ' '.join([f"{k} {d}, " for k, d in zip(columns, dtypes)])
    uniques = ' '.join([f" {k}," for k in columns])[:-1]
    schema += f'UNIQUE({uniques})'

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        query = f'''CREATE TABLE IF NOT EXISTS {table}({schema})'''
        cur.execute(query)

    print(f"Table {table} created if it not already existed.")


def get_folder(folder):
    if folder not in VALIDFOLDERS:
        raise ValueError(
            f'Wrong folder kind `{kind}` must be one of {VALIDFOLDERS}.')
    folder_path = os.path.join(os.environ['LFFOLDER'], folder)
    os.makedirs(folder_path, exist_ok=True)
    return folder_path


def get_dbpath():
    return os.path.join(os.environ['LFFOLDER'], 'fundpet.db')


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


def populateSqlite(db_path, table, records, columns, dtypes=None):

    if not checkDBexists(db_path, raiseError=False):
        print(f"Creating new sqlite database at {db_path}.")

    createTableIfNotExists(db_path, table, columns, dtypes)

    qvals = ''.join('?,' * len(columns))[:-1]
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.executemany(
            f"INSERT OR REPLACE INTO {table} VALUES({qvals})", records)
        print(f"SQLITE: table {table} populated.")


def retrieve_table(db_path, table, verbose=True):

    query = f"SELECT * FROM {table}"

    if verbose:
        print(f"Quering sqlite database at {db_path} with `{query[:100]}`... ")

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute(query)
        columns = list(map(lambda x: x[0], cur.description))
        res = cur.fetchall()

    df = pd.DataFrame(res, columns=columns)

    return df



def retrieve_not_fetched_urls_from_domain(db_path, query, domains, verbose=True):

    checkDBexists(db_path)

    table = query
    doms = ','.join([f"'{d}'" for d in domains])
    columns = ['uid', 'resolvedUrl', '']
    columns_str = ','.join(columns)
    query = f"""
        SELECT uid, resolvedUrl, domain FROM {table}
        where domain IN ({doms})
        AND fetched=='0'"""


    if verbose:
        print(f"Quering sqlite database at {db_path} with `{query[:100]}`... ")

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute(query)
        res = cur.fetchall()

    df = pd.DataFrame(res, columns=columns, dtype=str)

    return df


def update_fetched(db_path, df, query, verbose=True):

    table = query

    fetched_uids = df.uid.tolist()
    qvalues = ', '.join('?' for _ in range(len(df)))
    q1 = f"""
        UPDATE {table}
        SET fetched = '1'
        WHERE uid in ({qvalues})
    """
    if verbose:
        print(
            f"Quering sqlite database at {db_path} with `{q1}`.")

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute(q1, fetched_uids)
        res = cur.fetchall()

    fetched_paths = df.path.tolist()
    q2 = f"""
        UPDATE {table}
        SET htmlPath=?
        WHERE uid=?
    """
    data = [[path, uid] for path, uid in zip(fetched_paths, fetched_uids)]
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.executemany(q2, data)
        res = cur.fetchall()


def update_preprocessed(db_path, query, df, columns):

    table = query

    createTableIfNotExists(db_path, table, columns)

    # get already fetch urls
    query = f"""
        SELECT uid from {table} WHERE fetched='1'
    """
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute(query)
        res = cur.fetchall()
    df_fetched = pd.DataFrame(res, columns=['uid'], dtype=str)

    # remove already fetched urls
    df = df[~df['uid'].isin(df_fetched.uid.tolist())]

    records = df[columns].values.tolist()

    populateSqlite(db_path, table, records, columns)