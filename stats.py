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
args = ap.parse_args()
config = args.config

# 0. Set things
########################################################

# load config and set parameters
with open(config, "r") as fh:
    config = yaml.load(fh, Loader=yaml.SafeLoader)

DOMAINNAMETHRESHOLD = config['domain_freq_threshold']
PLATFORMS = config['platforms']
QUERIES = config['linkfluence_queries']
DBPATH = get_dbpath()

for platform in PLATFORMS:
    for query in QUERIES:

        # 1. Retrive data from table
        ########################################################

        table = f'{platform}_{query}'
        df = retrieve_table(DBPATH, table, verbose=True)
        n = len(df)

        # 2. Make aggregations
        ########################################################

        # (2a) Agreggate URLs
        # print("~~~~~~~~~~~~~~~~~~~~~~~")
        externalUrl_counts = df['externalUrl'].value_counts()
        externalUrl_counts = externalUrl_counts \
            .to_frame() \
            .reset_index() \
            .rename(
                columns={"index": "externalUrl", "externalUrl": "urlCount"})
        u = len(externalUrl_counts)
        # print(
        #     f"There are {u} unique urls of a total of {n} ({100*u/n:.2f}%)")
        df = df.merge(externalUrl_counts, on='externalUrl')
        df = df.groupby('externalUrl').first().reset_index()
        # print("Most frequents Urls:")
        # print(externalUrl_counts.head(5))

        # (2b) Domains stats
        domainName_counts = df['domainName'].value_counts()
        domainName_counts = domainName_counts \
            .to_frame() \
            .reset_index() \
            .rename(columns={"index": "domain", "domainName": "domainCounts"})
        d1 = len(domainName_counts)

        # print("~~~~~~~~~~~~~~~~~~~~~~~")
        # print(f"There are {d1} unique domains.")
        # print("Most frequents domain names counts:")
        # print(domainName_counts.head(5))

        # urlNoProt = df['externalUrl'].apply(
        #     lambda url: url.replace('https://', '').replace('http://', ''))
        # tmp = df.assign(urlNoProt=urlNoProt)
        # c0 = (tmp.urlNoProt == tmp.domainName).sum()
        # mssg = f"There are at least {c0} unique urls that are equal to their \
        # correspondent domain name."
        # print(mssg)

        # 2. Make exports
        ########################################################
        table = f'{platform}_{query}_externalUrlCounts'
        columns = ["externalUrl", "urlCount"]
        records = externalUrl_counts[columns].values.tolist()
        populateSqlite(DBPATH, table, records, columns)

        table = f'{platform}_{query}_domainCounts'
        columns = ["domain", "domainCounts"]
        records = domainName_counts[columns].values.tolist()
        populateSqlite(DBPATH, table, records, columns)
