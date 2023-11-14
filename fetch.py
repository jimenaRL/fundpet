import os
import yaml
import minet
import tempfile
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
    retrieve_not_fetched_urls_from_domain, \
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
NBTHREADS = config['nb_threads_fetch']
domains_to_fetch = config['queries'][query]['domains_to_fetch']
domain_freq_th = config['queries'][query]['domain_freq_threshold']


# 1.Retrive data from table
########################################################

table = query+"_domainCounts"
df = retrieve_table(DBPATH, table, verbose=True)
new_doms_to_fetch = df[df.domainCounts > domain_freq_th]['domain'].tolist()

#############################
# TO DO BETTER WARNING
if set(domains_to_fetch) ^ set(new_doms_to_fetch):
    print(f"""Domains to fetch are not equal:
    domains from config: {domains_to_fetch}
    domains from stats: {new_doms_to_fetch}""")
#############################

# 2. get all url for each one of these domains
_, _, minet_output_dir = get_fetch_paths(query)

df = retrieve_not_fetched_urls_from_domain(DBPATH, query, domains_to_fetch)
print(f"Fetching URL's html for {len(df)} websites for query {query}")
with tempfile.NamedTemporaryFile() as tmpFilePath:
    with tempfile.NamedTemporaryFile() as tmpMinetOutput:
        df.to_csv(tmpFilePath, index=False)
        command_pipe = [
            "minet",
            "fetch",
            "resolvedUrl",
            f"-i {tmpFilePath.name}",
            f"--output {tmpMinetOutput.name}",
            f"--output-dir {minet_output_dir}",
            "--folder-strategy normalized-hostname",
            f"--threads {NBTHREADS}"
        ]
        # subprocess.run(command_pipe, shell=True)
        os.system(' '.join(command_pipe))
        tmp = pd.read_csv(tmpMinetOutput.name)
        df_fetched = tmp[tmp.http_status == 200]
        update_fetched(DBPATH, df_fetched, query)
