import os
import yaml
import minet
import pandas as pd
from tqdm import tqdm
from ural import get_domain_name
from argparse import ArgumentParser
from utils import get_date, get_path, get_dbpath, get_folder, populateSqlite

ap = ArgumentParser()
ap.add_argument('--config', type=str)
ap.add_argument('--start', type=str, default=None)
ap.add_argument('--end', type=str, default=None)
args = ap.parse_args()
config = args.config
start = args.start
end = args.end

# 0. Set things
########################################################

# load config and set parameters
with open(config, "r") as fh:
    config = yaml.load(fh, Loader=yaml.SafeLoader)

DOMAINNAMETHRESHOLD = config['domain_freq_threshold']
DOMAINSTORESOLVE = config['domains_to_preresolve']
IGNOREDOMAINS = config['ignored_domains']

PLATFORMS = config['platforms']
QUERIES = config['linkfluence_queries']

DBPATH = get_dbpath()

# set dates and get paths
start_timestamp, end_timestamp = get_date(start, end)

PREPROCESSEDFOLDER = get_folder('preprocessed')

for platform in PLATFORMS:
    for query in QUERIES:

        # 1. Load dump
        ########################################################

        dump_path = get_path(
            platform, query, start_timestamp, end_timestamp, 'dumps')
        if os.path.exists(dump_path):
            print(f"Analyzing Linkfluence dump from {dump_path}.")
        else:
            print(f"Skipping {query} from platform {platform}. Unnable to find dumps at{dump_path}.")
            continue

        # (0) Load linkfluence dump and standarize url column name
        df = pd.read_csv(dump_path, dtype=str)
        n0 = len(df)
        df = df[~ df['externalUrl'].isna()]
        df = df[~ (df['externalUrl'] == "[]")]
        n1 = len(df)
        print(f"Drop {n0 - n1} empty entry of {n0} in URL column, left {n1}.")

        # 1. Process dump
        ########################################################

        # (1a) Get domain names from URLs
        df = df.assign(domainName=df['externalUrl'].apply(get_domain_name))

        # (1b) Group URLs for specific domains
        def resolveAvaazUrls(df):
            domain = 'avaaz.org'
            df = df.sort_values(by='domainName').reset_index().drop(columns=['index'])
            a0 = df[df.domainName == domain].index[0]
            a1 = df[df.domainName == domain].index[-1]
            urls = df[df.domainName == domain]['externalUrl'].tolist()
            print(f"Resolving {len(urls)} urls for domain '{domain}'.")
            resolved_urls = []
            for url in tqdm(urls):
                resolved_urls.append(minet.web.resolve(url=url)[1][-1].url)
            df['externalUrl'].loc[i0:i1] = resolved_urls
            resDomainNames = [get_domain_name(ru) for ru in resolved_urls]
            df['domainName'].loc[i0:i1] = resDomainNames
            return df

        print('Resolving shortened URLs...')

        def resolveUrls(df, domain):
            if len(df[df.domainName == domain]) > 0:
                df = df.sort_values(by='domainName') \
                    .reset_index() \
                    .drop(columns=['index'])
                i0 = df[df.domainName == domain].index[0]
                i1 = df[df.domainName == domain].index[-1]
                urls = df[df.domainName == domain]['externalUrl'].tolist()
                print(f"Resolving {len(urls)} urls for domain '{domain}'.")
                resolved_urls = []
                for url in tqdm(urls):
                    resolved_urls.append(minet.web.resolve(url=url)[1][-1].url)
                df['externalUrl'].loc[i0:i1] = resolved_urls
                resDomainNames = [get_domain_name(ru) for ru in resolved_urls]
                df['domainName'].loc[i0:i1] = resDomainNames
            return df

        for domain in DOMAINSTORESOLVE:
            df = resolveUrls(df, domain=domain)

        # (1c) Remove URLs from domains to ignore
        for domain in IGNOREDOMAINS:
            n = len(df[df.domainName == domain])
            if n > 0:
                print(f"Removing {n} URLs whose domain name is {domain}")
                df = df[df.domainName != domain]

        # 2. Export processed data to sql table
        ########################################################

        table = f'{platform}_{query}'
        columns = ['permalink', 'domainName', 'externalUrl']
        records = df[columns].values.tolist()
        populateSqlite(DBPATH, table, records, columns)
