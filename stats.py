import os
import yaml
import minet
import pandas as pd
from tqdm import tqdm
from ural import get_domain_name
from argparse import ArgumentParser
from utils import \
    get_date, \
    get_dbpath
from sqlite import \
    populateSqlite, \
    retrieve_table, \
    updateUrlCounts, \
    updateDomainCounts

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


# 1. Retrive data from table
########################################################
df = retrieve_table(table=query, verbose=True)
n = len(df)

# 2. Make aggregations
########################################################

# (2a) Agreggate URLs
print("~~~~~~~~~~~~~~~~~~~~~~~")
url_counts = df['resolvedUrl'].value_counts()
url_counts = url_counts \
    .to_frame() \
    .reset_index() \
    .rename(columns={"count": "urlCount"})
u = len(url_counts)
print(
    f"There are {u} unique urls of a total of {n} ({100*u/n:.2f}%)")

df = df.merge(url_counts, on='resolvedUrl')
df = df.groupby('resolvedUrl').first().reset_index() \
    .sort_values(by='urlCount', ascending=False)
print("Most frequents Urls:")
print(url_counts.head(10))

# (2b) Domains stats
print("~~~~~~~~~~~~~~~~~~~~~~~")
domain_counts = df['domain'].value_counts()
domain_counts = domain_counts \
    .to_frame() \
    .reset_index() \
    .rename(columns={"count": "domainCount"})
d1 = len(domain_counts)

print(f"There are {d1} unique domains.")
print("Most frequents domain names counts:")
print(domain_counts.head(5))

# 2. Make exports
########################################################
updateUrlCounts(query, url_counts[["resolvedUrl", "urlCount"]])

updateDomainCounts(query, domain_counts[["domain", "domainCount"]])
