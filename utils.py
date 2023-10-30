import os
import sqlite3
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


def populateSqlite(db_path, name, df, toKeep, dtype='TEXT'):

    if not os.path.exists(db_path):
        print(f"Creating new sqlite database at {db_path}.")

    records = df[toKeep].to_dict(orient='records')

    # check if the table doesn't exists
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        query = """
            SELECT count(name)
            FROM sqlite_master
            WHERE type='table'
            AND name=? """
        cur.execute(query, (name, ))
        con.commit()
        res = cur.fetchall()
    table_exists = False if res[0][0] == 0 else True

    ncols = len(toKeep)
    qvals = ''.join('?,' * ncols)[:-1]
    schema = ' '.join([f"{k} TEXT," for k in toKeep])[:-1]
    data = df[toKeep].values.tolist()

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        if not table_exists:
            cur.execute(f'''CREATE TABLE {name}({schema})''')
            print(f"SQLITE: table {name} created.")
        cur.executemany(f"INSERT INTO {name} VALUES({qvals})", data)
        print(f"SQLITE: table {name} populated.")
