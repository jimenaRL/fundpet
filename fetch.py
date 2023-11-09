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
ap.add_argument('--query', type=str)
args = ap.parse_args()
config = args.config
query = args.query

# 0. Set things
########################################################

# load config and set parameters
with open(config, "r") as fh:
    config = yaml.load(fh, Loader=yaml.SafeLoader)

DBPATH = get_dbpath()
NBTHREADS= config['nb_threads_fetch']
domains_to_fetch = config['queries'][query]['domains_to_fetch']
domain_freq_th = config['queries'][query]['domain_freq_threshold']


# 1.Retrive data from table
########################################################

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

df.to_csv(filepath, index=False)
print(f"Got {len(df)} urls to retrieve. Csv file saved at {filepath}")

print(f"Fetching website for query {query}")

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
