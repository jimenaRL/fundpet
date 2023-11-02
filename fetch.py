import os
import yaml
import minet
import pandas as pd
from tqdm import tqdm
from ural import get_domain_name
from argparse import ArgumentParser
from utils import \
    get_path, \
    get_dbpath, \
    retrieve_table, \
    get_file_fetch_path, \
    retrieve_urls_from_domain

ap = ArgumentParser()
ap.add_argument('--config', type=str)
args = ap.parse_args()
config = args.config

# 0. Set things
########################################################

# load config and set parameters
with open(config, "r") as fh:
    config = yaml.load(fh, Loader=yaml.SafeLoader)

QUERIES = config['linkfluence_queries']
DOMAINSTOFETCH = config['domains_to_fetch']
DBPATH = get_dbpath()

# 1. Fetch stuff
########################################################
for query in QUERIES:

    domains_to_fetch = DOMAINSTOFETCH[query]
    domain_freq_th = config['domain_freq_threshold'][query]

    # 1a. Retrive data from table
    table = query+"_domainCounts"
    df = retrieve_table(DBPATH, table, verbose=True)
    new_doms_to_fetch = df[df.domainCounts > domain_freq_th]['domain'].tolist()
    #############################
    # TO BETTER WARNING
    if set(domains_to_fetch) ^ set(new_doms_to_fetch):
        print(f"""Domains to fetch are not equal:
        domains from config: {domains_to_fetch}
        domains from stats: {new_doms_to_fetch}""")
    #############################

    # 2. get all url for each one of these domains
    df = retrieve_urls_from_domain(DBPATH, query, domains_to_fetch)
    path_file_urls_to_fetch = get_file_fetch_path(query, 'fetched')
    df.to_csv(path_file_urls_to_fetch, index=False)
    print(f"Got {len(df)} urls to retrieve.")
    # 3. fetch html content for each website
    path_file_urls_to_fetch = get_file_fetch_path(query, 'fetched')
    output = path_file_urls_to_fetch.replace('to_fetch_', 'minet_output_')
    output_dir = os.path.join(os.path.split(output)[0], query)
    command = f"""
        minet fetch resolvedUrl {path_file_urls_to_fetch} \
        --output {output} \
        --output-dir {output_dir} \
        --folder-strategy hostname \
        --threads 100
    """
    os.system(command)
