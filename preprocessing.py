import os
import re
import yaml
import minet
import tempfile
import pandas as pd
from tqdm import tqdm
from ural import get_domain_name, urls_from_text, is_typo_url
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

# debug
config = 'config.yaml'
start = None
end = None

# 0. Set things
########################################################

# load config and set parameters
with open(config, "r") as fh:
    config = yaml.load(fh, Loader=yaml.SafeLoader)

DOMAINNAMETHRESHOLD = config['domain_freq_threshold']
DOMAINSTORESOLVE = config['domains_to_preresolve']
IGNOREDOMAINS = config['ignored_domains']
IGNOREURLSSAMEASDOM = config['ignored_urls_same_as_domain']

PLATFORMS = config['platforms']
QUERIES = config['linkfluence_queries']

DBPATH = get_dbpath()

# set dates and get paths
start_timestamp, end_timestamp = get_date(start, end)

PREPROCESSEDFOLDER = get_folder('preprocessed')

for query in QUERIES:

    df = []
    columns = ['permalink', 'uid', 'text', 'externalUrl', 'platform', 'query']

    # 1. Load dumps
    ########################################################
    for platform in PLATFORMS:

        # (0) Load linkfluence dump and standarize url column name
        dump_path = get_path(
            platform, query, start_timestamp, end_timestamp, 'dumps')
        if os.path.exists(dump_path):
            print(f"Analyzing Linkfluence dump from {dump_path}.")
            df.append(pd.read_csv(dump_path, dtype=str)[columns])
        else:
            print(f"Skipping {query} from platform {platform}. \
            Unnable to find dumps at{dump_path}.")
            continue
    df = pd.concat(df)

    # 1. Process dump
    ########################################################

    # (1a) Get URLs posts text
    urlsFromText = df['text'] \
        .fillna("") \
        .apply(lambda t: list(urls_from_text(t)))
    df = df.assign(urlsFromText=urlsFromText)
    df = df.explode('urlsFromText') \
        .reset_index()\
        .drop(columns='index') \
        .rename(columns={'urlsFromText': 'urlFromText'})
    df = df[~df.urlFromText.isna()]
    n0 = len(df)
    df = df.assign(isTypoUrl=df['urlFromText'].apply(is_typo_url))
    df = df[~df.isTypoUrl].drop(columns='isTypoUrl')
    print(f"Removing {n0 - len(df)} with typo urls, left {len(df)}.")

    # (1b) Resolve urls
    print('Resolving URLs...')
    with tempfile.NamedTemporaryFile() as tmp1:
        df.to_csv(tmp1.name, index=False)
        with tempfile.NamedTemporaryFile() as tmp2:
            os.system(f"minet resolve urlFromText {tmp1.name} > {tmp2.name}")
            df = pd.read_csv(tmp2.name, dtype=str) \
                .rename(columns={'resolved': 'resolvedUrl'})

    # (1c) Get domain names from URLs
    df = df.assign(
        domainName=df['resolvedUrl'].fillna("").apply(get_domain_name))

    # (1c) Remove URLs from domains to ignore
    for domain in IGNOREDOMAINS:
        n = len(df[df.domainName == domain])
        if n > 0:
            print(f"Removing {n} URLs whose domain name is {domain}")
            df = df[df.domainName != domain]

    print(f"Left {len(df)} entries.")

    # (1d) Drop urls equals to their domain
    def fn(s):
        s = s.replace('https://', '').replace('http://', '')
        s = s.replace('www.', '')
        s = re.sub(r"/$", "", s)
        return s

    urlSameAsDom = (df['resolvedUrl'].fillna("").apply(fn) == df['domainName'])
    df = df.assign(urlSameAsDomain=urlSameAsDom)

    for domain in IGNOREURLSSAMEASDOM:
        n = len(df)
        df = df[~((df.domainName == domain) & df.urlSameAsDomain)]
        print(f"Removing {n - len(df)} with url equal to its domain {domain}.")

    # 2. Export processed data to sql table
    ########################################################

    table = f'{query}'
    columns = [
        'permalink', 'uid', 'text', 'resolvedUrl',
        'platform', 'query', 'domainName',
        ]
    records = df[columns].values.tolist()
    populateSqlite(DBPATH, table, records, columns)
