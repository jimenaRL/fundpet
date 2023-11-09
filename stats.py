import os
import yaml
import minet
import pandas as pd
from tqdm import tqdm
from ural import get_domain_name
from argparse import ArgumentParser
from utils import \
    get_date, \
    get_path, \
    get_dbpath, \
    get_folder, \
    populateSqlite, \
    retrieve_table

ap = ArgumentParser()
ap.add_argument('--config', type=str)
ap.add_argument('--query', type=str)
args = ap.parse_args()
config = args.config
query = args.query

# 0. Set things
########################################################

# load config and set parameters
with open(config, "r") as fh:
    config = yaml.load(fh, Loader=yaml.SafeLoader)

DOMAINNAMETHRESHOLD = config['queries'][query]['domain_freq_threshold']
PLATFORMS = config['platforms']
DBPATH = get_dbpath()


# 1. Retrive data from table
########################################################

table = query
df = retrieve_table(DBPATH, table, verbose=True)
n = len(df)

# 2. Make aggregations
########################################################

# (2a) Agreggate URLs
print("~~~~~~~~~~~~~~~~~~~~~~~")
resolvedUrl_counts = df['resolvedUrl'].value_counts()
resolvedUrl_counts = resolvedUrl_counts \
    .to_frame() \
    .reset_index() \
    .rename(columns={"count": "urlCount"})
u = len(resolvedUrl_counts)
print(
    f"There are {u} unique urls of a total of {n} ({100*u/n:.2f}%)")

df = df.merge(resolvedUrl_counts, on='resolvedUrl')
df = df.groupby('resolvedUrl').first().reset_index() \
    .sort_values(by='urlCount', ascending=False)
print("Most frequents Urls:")
print(resolvedUrl_counts.head(10))

# (2b) Domains stats
print("~~~~~~~~~~~~~~~~~~~~~~~")
domainName_counts = df['domainName'].value_counts()
domainName_counts = domainName_counts \
    .to_frame() \
    .reset_index() \
    .rename(columns={"count": "domainCounts"})
d1 = len(domainName_counts)

print(f"There are {d1} unique domains.")
print("Most frequents domain names counts:")
print(domainName_counts.head(5))

# 2. Make exports
########################################################
table = f'{query}_resolvedUrlCounts'
columns = ["resolvedUrl", "urlCount"]
records = resolvedUrl_counts[columns].values.tolist()
populateSqlite(DBPATH, table, records, columns, dtypes=['TEXT', 'INT'])

table = f'{query}_domainCounts'
columns = ["domainName", "domainCounts"]
records = domainName_counts[columns].values.tolist()
populateSqlite(DBPATH, table, records, columns, dtypes=['TEXT', 'INT'])
