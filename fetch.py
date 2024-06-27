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
    get_fetch_paths

from sqlite import \
    retrieve_table, \
    retrieve_not_fetched_urls_from_domain


def fetch(query, domains_to_fetch, domain_freq_th, threads, logger):

    # 1.Retrive data from table
    ########################################################

    table = query+"_domainCounts"
    df = retrieve_table(table, verbose=False)

    new_doms_to_fetch = df[df.domainCount > domain_freq_th]['domain'].tolist()

    if set(domains_to_fetch) ^ set(new_doms_to_fetch):
        mssg = f"FETCH: There are new domains with more than {domain_freq_th} "
        mssg += f"entries : {set(new_doms_to_fetch) - set(domains_to_fetch)}"
        logger.info(mssg)

    # 2. get all url for each one of these domains
    _, _, minet_output_dir = get_fetch_paths(query)

    df = retrieve_not_fetched_urls_from_domain(query, domains_to_fetch)
    mssg = f"FETCH: fetching URL's html for {len(df)} websites "
    mssg += f"for query {query} and domains: {', '.join(domains_to_fetch)}."
    logger.info(mssg)

    with tempfile.NamedTemporaryFile() as tmpFilePath:
        with tempfile.NamedTemporaryFile() as tmpMinetOutput:
            df[['uid', 'resolvedUrl']].drop_duplicates() \
                .to_csv(tmpFilePath, index=False)
            command_pipe = [
                "minet",
                "fetch",
                "resolvedUrl",
                f"-i {tmpFilePath.name}",
                f"--output {tmpMinetOutput.name}",
                f"--output-dir {minet_output_dir}",
                "--folder-strategy normalized-hostname",
                f"--threads {threads}"
            ]
            os.system(' '.join(command_pipe))
            tmp = pd.read_csv(
                tmpMinetOutput.name,
                usecols=['uid', 'resolvedUrl', 'http_status', 'datetime_utc', 'path'],
                dtype=str)

            df_fetched = tmp[tmp.http_status == '200']

            n = len(df_fetched)
            logger.info(
                f"FETCH: fetching succeded (http_status = 200) for {n} entries.")

            return df_fetched
