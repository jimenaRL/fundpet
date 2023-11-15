import os
import re
import yaml
import minet
import tempfile
import subprocess
import pandas as pd
from tqdm import tqdm
from ural import get_domain_name, urls_from_text, is_typo_url
from argparse import ArgumentParser
from utils import get_date, get_path, get_dbpath, get_folder, populateSqlite, \
    update_preprocessed

ap = ArgumentParser()
ap.add_argument('--config', type=str)
ap.add_argument('--query', type=str)
ap.add_argument('--start', type=str, default=None)
ap.add_argument('--end', type=str, default=None)
args = ap.parse_args()
config = args.config
query = args.query
start = args.start
end = args.end

# 0. Set things
########################################################

# load config and set parameters
with open(config, "r") as fh:
    config = yaml.load(fh, Loader=yaml.SafeLoader)

DOMAINNAMETHRESHOLD = config['queries'][query]['domain_freq_threshold']
IGNOREDOMAINS = config['ignored_domains']
IGNOREURLSSAMEASDOM = config['ignored_urls_same_as_domain']

PLATFORMS = config['platforms']

DBPATH = get_dbpath()
START, END = get_date(start, end)

PREPROCESSECOLUMNS = [
        'permalink', 'uid', 'text', 'externalUrl', 'platform', 'query', 'date']

print(f'\n~~~~~~~~~~~~~~ {query} ~~~~~~~~~~~~~~')

df = []

# 1. Load dumps
########################################################
for platform in PLATFORMS:

    print('\n\t**Platform: %s' % platform)
    # load linkfluence dump and standarize url column name
    dump_path = get_path(
        platform, query, START, END, 'dumps')
    if os.path.exists(dump_path):
        print(f"\tAnalyzing Linkfluence dump from {dump_path}.")
        df.append(pd.read_csv(dump_path, dtype=str)[PREPROCESSECOLUMNS])
    else:
        print(f"\tSkipping {query} from platform {platform}. \
        Unnable to find dumps at{dump_path}.")
        continue
df = pd.concat(df)

# 1. Process dump
########################################################

print('\nExtracting URLs from posts text')

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
print('Resolving URLs')
with tempfile.NamedTemporaryFile() as tmp1:
    df.to_csv(tmp1.name, index=False)
    with tempfile.NamedTemporaryFile() as tmp2:
        command_pipe = [
            "minet",
            "resolve",
            "urlFromText",
            f"-i {tmp1.name}",
            ">",
            f"{tmp2.name}"
        ]
        # subprocess.run(command_pipe, shell=True)
        os.system(' '.join(command_pipe))
        df = pd.read_csv(tmp2.name, dtype=str) \
            .rename(columns={'resolved_url': 'resolvedUrl'})

# (1c) Get domain names from URLs
print('Getting domain names')
df = df.assign(
    domain=df['resolvedUrl'].fillna("").apply(get_domain_name))

# (1c) Remove URLs from domains to ignore
for domain in IGNOREDOMAINS:
    n = len(df[df.domain == domain])
    if n > 0:
        print(f"Removing {n} URLs whose domain name is {domain}")
        df = df[df.domain != domain]

print(f"Left {len(df)} entries.")


# (1d) Drop urls equals to their domain
def fn(s):
    s = s.replace('https://', '').replace('http://', '')
    s = s.replace('www.', '')
    s = re.sub(r"/$", "", s)
    return s


urlSameAsDom = (df['resolvedUrl'].fillna("").apply(fn) == df['domain'])
df = df.assign(urlSameAsDomain=urlSameAsDom)

for domain in IGNOREURLSSAMEASDOM:
    n = len(df)
    df = df[~((df.domain == domain) & df.urlSameAsDomain)]
    d = n - len(df)
    if d > 0:
        print(f"Drop {d} entries with url equal to its domain {domain}.")

# 2. Export processed data to sql table
########################################################

# Before exporting, deal with repeated uid as consequance of the exploding
# operation in (1a) when urls are extrated from text

uid_suffix = df.groupby('uid', as_index=False)['uid'].cumcount().astype(str)
df = df.assign(uid=df.uid+'_'+uid_suffix)
df = df.assign(htmlPath='')
df = df.assign(scrapped=False)
df = df.assign(fetched=False)
columns = [
    'permalink', 'uid', 'text', 'resolvedUrl',
    'platform', 'query', 'domain', 'date', 'fetched', 'htmlPath', 'scrapped'
    ]

update_preprocessed(DBPATH, query, df, columns)
