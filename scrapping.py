import os
import yaml
import json
import pandas as pd
from tqdm import tqdm
from argparse import ArgumentParser
from scraper import ChangeOrg
from utils import get_dbpath, \
    retrieve_table, \
    get_fetch_paths, \
    update_scraped

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
_, _, outdir = get_fetch_paths(query)

# 1.Retrive data from table
########################################################
table = query
df = retrieve_table(DBPATH, table, verbose=True)

df = df[df.fetched == '1']
df = df[df.scrapped == '0']

df = df.assign(htmlPath=df.htmlPath.apply(lambda p: os.path.join(outdir, p)))

########## HOT FIX ##########
df = df.assign(pathExists=df.htmlPath.apply(os.path.exists))
df = df[df.pathExists]
df = df.groupby('htmlPath').first().reset_index()
#############################

records = []
for htmlPath in tqdm(df['htmlPath'].tolist()[:30]):
    scraper = ChangeOrg(htmlPath)
    scraper.scrappe()
    res = scraper.response()
    records.append(json.dumps(res))
    del scraper

records = [json.loads(r) for r in records]
dr = pd.DataFrame(records)

columns = ['permalink', 'uid',  'resolvedUrl', 'htmlPath']
update_scraped(DBPATH, df[columns], dr, query)
