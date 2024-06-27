import os
import sqlite3
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta, MO
from utils import get_dbpath
from time import gmtime, strftime

DBPATH = get_dbpath()


createMainTableTemplate = """
    CREATE TABLE {query}(
        permalink TEXT NOT NULL,
        uid TEXT PRIMARY KEY UNIQUE,
        text TEXT,
        resolvedUrl TEXT NOT NULL,
        platform TEXT NOT NULL,
        query TEXT NOT NULL,
        domain TEXT NOT NULL,
        date TEXT NOT NULL,
        fetched INTEGER NOT NULL,
        fetchDate TEXT,
        htmlPath TEXT NOT NULL,
        scrapped INTEGER NOT NULL,
    UNIQUE(permalink,  uid));
"""


createDomainCountsTableTemplate = """
    CREATE TABLE {query}_domainCounts(
        domain TEXT PRIMARY KEY UNIQUE,
        domainCount INTEGER NOT NULL,
    UNIQUE(domain));
"""

createUrlCountsTableTemplate = """
    CREATE TABLE {query}_urlCounts(
        resolvedUrl TEXT PRIMARY KEY UNIQUE,
        urlCount INTEGER NOT NULL,
    UNIQUE(resolvedUrl));
"""

createdumpStatsTemplate = """
    CREATE TABLE dumpStats(
        file TEXT PRIMARY KEY UNIQUE,
        lang TEXT NOT NULL,
        platform TEXT NOT NULL,
        nb_entries INTEGER NOT NULL,
        start TEXT NOT NULL,
        end TEXT NOT NULL,
    UNIQUE(file));
"""


def checkDBexists(raiseError=True):
    dbExists = os.path.exists(DBPATH)
    if dbExists:
        return True
    else:
        if raiseError:
            raise FileNotFoundError(
                f"SQLITE: Unnable to find database at: '{DBPATH}'.")
        else:
            return False


def checkTableExists(table, raiseError=False):

    checkDBexists(raiseError=False)

    query = """
        SELECT count(name)
        FROM sqlite_master
        WHERE type='table'
        AND name=?
    """

    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.execute(query, (table, ))
        con.commit()
        res = cur.fetchall()
    tableExists = False if res[0][0] == 0 else True

    if tableExists:
        return True
    else:
        if raiseError:
            raise ValueError(
                f"SQLITE: Unnable to find table: '{table}'.")
        else:
            return False


# def createUniqueTable(table, columns, dtypes=None):

#     checkDBexists(raiseError=False)

#     ncols = len(columns)
#     if not dtypes:
#         schema = ' '.join([f"{k} TEXT, " for k in columns])
#     else:
#         schema = ' '.join([f"{k} {d}, " for k, d in zip(columns, dtypes)])
#     uniques = ' '.join([f" {k}," for k in columns])[:-1]
#     schema += f'UNIQUE({uniques})'

#     query = f'''CREATE TABLE {table}({schema})'''
#     with sqlite3.connect(DBPATH) as con:
#         cur = con.cursor()
#         cur.execute(query)

#     print(f"SQLITE: Table {table} created.")


# def createUniqueTableIfNotExists(table, columns, dtypes=None):

#     checkDBexists(raiseError=False)

#     if not checkTableExists(table, raiseError=False):
#         createUniqueTable(table, columns, dtypes=dtypes)


def populateSqlite(table, records, columns):

    if not checkDBexists(raiseError=False):
        print(f"Creating new sqlite database at {DBPATH}.")

    checkTableExists(table, raiseError=True)

    qvals = ''.join('?,' * len(columns))[:-1]
    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.executemany(
            f"INSERT OR REPLACE INTO {table} VALUES({qvals})", records)
        print(f"SQLITE: table {table} populated with {len(records)} entries.")


def retrieve_table(table, verbose=False):

    query = f"SELECT * FROM {table}"

    if verbose:
        print(f"Quering sqlite database at {DBPATH} with `{query[:100]}`... ")

    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.execute(query)
        columns = list(map(lambda x: x[0], cur.description))
        res = cur.fetchall()

    df = pd.DataFrame(res, columns=columns)

    return df


def createMainTableIfNotExists(query, logger):

    table = query
    if checkTableExists(table, raiseError=False):
        return

    query = createMainTableTemplate.format(query=query)
    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.execute(query)

    logger.info(f"SQLITE: Table {table} created.")


def createUrlCountsTableIfNotExists(query):

    table = f'{query}_urlCounts'
    if checkTableExists(table, raiseError=False):
        return

    query = createUrlCountsTableTemplate.format(query=query)
    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.execute(query)

    print(f"SQLITE: Table {table} created.")


def createDomainCountsTableIfNotExists(query):

    table = f'{query}_domainCounts'
    if checkTableExists(table, raiseError=False):
        return

    query = createDomainCountsTableTemplate.format(query=query)
    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.execute(query)

    print(f"SQLITE: Table {table} created.")

def updatePreprocessed(query, df, logger):

    table = query
    createMainTableIfNotExists(query, logger)
    df = df.assign(fetchDate=0)
    columns = [
        'permalink',
        'uid',
        'text',
        'resolvedUrl',
        'platform',
        'query',
        'domain',
        'date',
        'fetched',
        'fetchDate',
        'htmlPath',
        'scrapped'
    ]
    records = df[columns].values.tolist()
    qvals = ''.join('?,' * len(columns))[:-1]
    columns_ = ','.join(columns)
    insert = f"""
        INSERT OR REPLACE INTO {table} ({columns_})
        VALUES ({qvals})
    """
    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.executemany(insert, records)
        res = cur.fetchall()
    logger.info(f"SQLITE: Table {table} updated.")


def updateUrlCounts(query, df):

    createUrlCountsTableIfNotExists(query)
    table = f'{query}_urlCounts'
    columns = [
        "resolvedUrl",
        "urlCount"
    ]
    records = df[columns].values.tolist()
    qvals = ''.join('?,' * len(columns))[:-1]
    columns_ = ','.join(columns)
    insert = f"""
        INSERT OR REPLACE INTO {table} ({columns_})
        VALUES ({qvals})
    """
    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.executemany(insert, records)
        res = cur.fetchall()
    print(f"SQLITE: Table {table} updated.")


def updateDomainCounts(query, df):
    createDomainCountsTableIfNotExists(query)
    table = f'{query}_domainCounts'
    columns = [
        "domain",
        "domainCount"
    ]
    records = df[columns].values.tolist()
    qvals = ''.join('?,' * len(columns))[:-1]
    columns_ = ','.join(columns)
    insert = f"""
        INSERT OR REPLACE INTO {table} ({columns_})
        VALUES ({qvals})
    """
    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.executemany(insert, records)
        res = cur.fetchall()
    print(f"SQLITE: Table {table} updated.")


def retrieve_not_fetched_urls_from_domain(query, domains=None, verbose=False):

    checkDBexists(DBPATH)

    table = query
    columns = ['uid', 'resolvedUrl', 'domain']
    columns_str = ','.join(columns)
    query = f"SELECT {columns_str} FROM {table} where "
    if domains:
        doms = ','.join([f"'{d}'" for d in domains])
        query += f"domain IN ({doms}) AND "
    query += f"fetched=='0'"""

    if verbose:
        print(f"Quering sqlite database at {DBPATH} with `{query[:100]}`... ")

    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.execute(query)
        res = cur.fetchall()

    df = pd.DataFrame(res, columns=columns, dtype=str)

    return df


def updateFetched(df, query, verbose=False):

    table = query

    fetched_uids = df.uid.tolist()
    qvalues = ', '.join('?' for _ in range(len(df)))
    q1 = f"""
        UPDATE {table}
        SET fetched = '1'
        WHERE uid in ({qvalues})
    """
    with sqlite3.connect(DBPATH) as con:
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
    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.executemany(q2, data)
        res = cur.fetchall()

    fetched_dates = df.datetime_utc.tolist()
    q3 = f"""
        UPDATE {table}
        SET fetchDate=?
        WHERE uid=?
    """
    data = [[date, uid] for date, uid in zip(fetched_dates, fetched_uids)]
    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.executemany(q3, data)
        res = cur.fetchall()


def update_scraped(df, dr, query, verbose=True):

    # update main table
    table = query
    fetched_uids = df.uid.tolist()
    qvalues = ', '.join('?' for _ in range(len(df)))
    q1 = f"""
        UPDATE {table}
        SET scrapped = '1'
        WHERE uid in ({qvalues})
    """
    if verbose:
        print(
            f"Quering sqlite database at {DBPATH} with `{q1}`.")

    with sqlite3.connect(DBPATH) as con:
        cur = con.cursor()
        cur.execute(q1, fetched_uids)
        cur.fetchall()

    # create and populate content table
    dr = pd.concat([df, dr], axis=1)
    table = f"{query}_htmlContent"
    columns = dr.columns.tolist()
    createTableIfNotExists(table, columns)
    records = dr.values.tolist()
    populateSqlite(table, records, columns)
