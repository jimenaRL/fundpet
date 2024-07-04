import os
import sys
import yaml
import logging
from time import gmtime, strftime

from argparse import ArgumentParser

from get_dump import getDump
from stats import stats
from fetch import fetch
from preprocessing import preprocessDumps
from utils import \
    get_date, \
    get_dbpath, \
    get_basepath, \
    getCredentials
from sqlite import \
    updatePreprocessed, \
    updateUrlCounts, \
    updateDomainCounts, \
    updateFetched


# parse arguments
ap = ArgumentParser()
ap.add_argument('--config', type=str, default="config.yaml")
ap.add_argument('--query', type=str)
ap.add_argument('--start', type=str, default=None)
ap.add_argument('--end', type=str, default=None)
args = ap.parse_args()
config = args.config
query = args.query
start = args.start
end = args.end

# 0. set things

client_id, client_secret = getCredentials()
start_timestamp, end_timestamp = get_date(start, end)
BASEPATH = get_basepath()
DBPATH = get_dbpath()

with open(config, "r") as fh:
    config = yaml.load(fh, Loader=yaml.SafeLoader)

query_id = config['queries'][query]['id']
PLATFORMS = config['platforms']
FREQFETCHDOMAINS = config['queries'][query]['warning_domain_freq_threshold']
IGNOREDOMAINS = config['ignored_domains']
IGNOREURLSSAMEASDOM = config['ignored_urls_same_as_domain']
IGNOREURLS = config['ignored_urls']
NBTHREADS = config['nb_threads_fetch']
FETCHDOMAINS = config['queries'][query]['domains_to_fetch']

now = strftime("%Y-%m-%d_%H:%M:%S", gmtime())
logfile = os.path.join(BASEPATH, f'logs/{query}_{now}.log')
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(logfile, 'w+', 'utf-8'),
        logging.StreamHandler(sys.stdout)],
)


# 1. dump download
for platform in PLATFORMS:
    dump_path = getDump(
        config=config,
        query=query,
        query_id=query_id,
        client_id=client_id,
        client_secret=client_secret,
        platform=platform,
        start=start,
        end=end,
        logger=logger)

    command = "xan select permalink,uid,text,externalUrl,platform,query,date "
    command += f"{dump_path} | xan slice -l 1 | xan flatten"
    os.system(command)

# 2. preprocess dump and update query table
df = preprocessDumps(
    query=query,
    start=start,
    end=end,
    platforms=PLATFORMS,
    domain_freq_threshold=FREQFETCHDOMAINS,
    ignored_domains=IGNOREDOMAINS,
    ignored_urls=IGNOREURLS,
    ignored_urls_same_as_domain=IGNOREURLSSAMEASDOM,
    logger=logger)

updatePreprocessed(query, df, logger)

command = f'sqlite3 -batch -csv {DBPATH} "SELECT * from {query}"'
command += " | "
command += "xan slice -l 1 | xan flatten"
os.system(command)

# 3. stats
url_counts, domain_counts = stats(
    query=query,
    logger=logger)

updateUrlCounts(query, url_counts)
updateDomainCounts(query, domain_counts)

command = f'sqlite3 -batch -csv {DBPATH} "SELECT * from {query}_urlCounts"'
command += " | "
command += "xan slice -l 10 | xan view"
os.system(command)

command = f'sqlite3 -batch -csv {DBPATH} "SELECT * from {query}_domainCounts"'
command += " | "
command += "xan slice -l 10 | xan view"
os.system(command)

# 4. fetch html
df_fetched = fetch(
    query=query,
    domains_to_fetch=FETCHDOMAINS,
    domain_freq_th=FREQFETCHDOMAINS,
    threads=NBTHREADS,
    logger=logger)

updateFetched(df_fetched, query)

command = f'sqlite3 -batch -csv {DBPATH} "SELECT resolvedUrl,htmlPath,date,fetched,fetchDate,scrapped from {query}"'
command += " | "
command += "xan view"
os.system(command)


# # # ## 5. scrap content from fetched websites
# # printf 'FUNDPET 4: scrapping\n'
# # python scrapping.py --query=$query --config=config.yaml