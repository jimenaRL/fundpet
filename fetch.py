import os
import yaml
import minet
import subprocess
import pandas as pd
from tqdm import tqdm
from ural import get_domain_name
from argparse import ArgumentParser
from utils import \
    get_path, \
    get_dbpath, \
    retrieve_table, \
    get_fetch_paths, \
    retrieve_urls_from_domain,\
    update_fetched

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
NBTHREADS= config['nb_threads_fetch']

# 1. Prepare csv for later fetch with minet
########################################################
for query in QUERIES:

    domains_to_fetch = DOMAINSTOFETCH[query]
    domain_freq_th = config['domain_freq_threshold'][query]

    # 1a. Retrive data from table
    table = query+"_domainCounts"
    df = retrieve_table(DBPATH, table, verbose=True)
    new_doms_to_fetch = df[df.domainCounts > domain_freq_th]['domainName'].tolist()
    #############################
    # TO DO BETTER WARNING
    if set(domains_to_fetch) ^ set(new_doms_to_fetch):
        print(f"""Domains to fetch are not equal:
        domains from config: {domains_to_fetch}
        domains from stats: {new_doms_to_fetch}""")
    #############################

    # 2. get all url for each one of these domains
    filepath, minet_output_file, minet_output_dir = get_fetch_paths(query)

    df = retrieve_urls_from_domain(DBPATH, query, domains_to_fetch)


    #####################################
    ############### TO DEV ##############
    # df = df.iloc[:30]
    #####################################
    #####################################

    df.to_csv(filepath, index=False)
    print(f"Got {len(df)} urls to retrieve. Csv file saved at {filepath}")

    print(f"Fetching website for query {query}")

    import os
    assert os.path.exists(filepath)
    command_pipe = [
        "minet",
        "fetch",
        "resolvedUrl",
        f"-i {filepath}",
        f"--output {minet_output_file}",
        f"--output-dir {minet_output_dir}",
        "--folder-strategy normalized-hostname",
        "--filename-template uid",
        f"--threads {NBTHREADS}"
    ]
    # subprocess.run(command_pipe, shell=True)
    os.system(' '.join(command_pipe))

    df = pd.read_csv(minet_output_file)
    update_fetched(DBPATH, df, query)
