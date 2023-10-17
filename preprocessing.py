import os
import yaml
import minet
import pandas as pd
from tqdm import tqdm
from ural import get_domain_name
from argparse import ArgumentParser

ap = ArgumentParser()
ap.add_argument('--platform', type=str)
ap.add_argument('--config', type=str)
args = ap.parse_args()
platform = args.platform
config = args.config

with open(config, "r") as fh:
    config = yaml.load(fh, Loader=yaml.SafeLoader)

externalUrl = 'externalUrl' if platform=='twitter' else 'url.normalized'

FILENAME = f"full_{platform}__start_{config['start']}_end_{config['end']}.csv"
DUMPPATH = os.path.join(config['dump_folder'], FILENAME)
print(f"Analyzing full {platform} dump from Linkfluence from {DUMPPATH}.")
OUTFOLDER = os.path.join(config['export_folder'])
OUTPATH = os.path.join(OUTFOLDER, f"toFetchUrls_full_{platform}.csv")

DOMAINNAMETHRESHOLD = config['domain_freq_threshold']
DOMAINSTORESOLVE = config['domains_to_preresolve']
IGNOREDOMAINS = config['ignored_domains']

# (0) Load linkfluence dump and standarize url column name
df = pd.read_csv(DUMPPATH, dtype=str) \
    .rename(columns={externalUrl: 'externalUrl'})
n0 = len(df)
df = df[~ df['externalUrl'].isna()]
df = df[~ (df['externalUrl'] == "[]")]
n1 = len(df)
print(f"Drop {n0 - n1} empty entry of {n0} in URL column, left {n1}.")


# (1) Drop repeated URLs
externalUrl_counts = df['externalUrl'].value_counts()
externalUrl_counts = externalUrl_counts \
    .to_frame() \
    .reset_index() \
    .rename(columns={"index": "externalUrl", "externalUrl": "urlCount"})
e1 = len(externalUrl_counts)
print(f"""There are {e1} unique urls of a total of {n1} ({100*e1/n1:.2f}%),
keeping only one entry for each unique url.""")
df = df.merge(externalUrl_counts, on='externalUrl')
df = df.groupby('externalUrl').first().reset_index()
# print("Most frequents Urls:")
# print(externalUrl_counts.head(20))


# (1) Get domain names from URLs and compute stats
df = df.assign(domainName=df['externalUrl'].apply(get_domain_name))


# (2) Try to resolve shortened URLs and update domain names
print('Resolving shortened URLs.')

def resolveUrls(df, domain):
    df = df.sort_values(by='domainName').reset_index().drop(columns=['index'])
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

# (3) Remove URLs from domains to ignore
for domain in IGNOREDOMAINS:
    n = len(df[df.domainName == domain])
    print(f"Removing {n} URLs whose domain name is {domain}")
    df = df[df.domainName != domain]


# (4) Domains stats
domainName_counts = df['domainName'].value_counts()
domainName_counts = domainName_counts \
    .to_frame() \
    .reset_index() \
    .rename(columns={"index": "domain", "domainName": "domainCounts"})
d1 = len(domainName_counts)
print(f"There are {d1} unique domains.")

urlNoProt = df['externalUrl'].apply(
    lambda url: url.replace('https://', '').replace('http://', ''))
tmp = df.assign(urlNoProt=urlNoProt)
c0 = (tmp.urlNoProt == tmp.domainName).sum()
mssg = f"There are at least {c0} unique urls that are equal to their  \
correspondent domain name."
print(mssg)

# print("Most frequents domain names counts:")
# print(domainName_counts.head(20))

# (4) Export full preprocessed file
toKeep = ['uid', 'permalink', 'domainName', 'externalUrl', 'urlCount']
df = df[toKeep].sort_values(by=['urlCount', 'domainName'], ascending=False)
df.to_csv(OUTPATH, index=False)
print(f"File save to {OUTPATH}")

domainName_outpath = os.path.join(
    OUTFOLDER,
    f"postprecessed_domain_counts_{platform}.csv")
domainName_counts.to_csv(domainName_outpath, index=False)
print(f"Domain counts saved at {domainName_outpath}.")

externalUrl_outpath = os.path.join(
    OUTFOLDER,
    f"original_urls_counts_{platform}.csv")
externalUrl_counts = externalUrl_counts.merge(
    df[['externalUrl', 'urlCount', 'domainName']],
    on=['externalUrl', 'urlCount'])
externalUrl_counts[['externalUrl', 'domainName','urlCount']] \
    .to_csv(externalUrl_outpath, index=False)
print(f"Domain counts saved at {externalUrl_outpath}.")


# (5) Export preprocessed files by domain for most frquent domains
print(f"Exporting data for domains with at least {DOMAINNAMETHRESHOLD} URLs.")
is_most_freq_domain = domainName_counts.domainCounts > DOMAINNAMETHRESHOLD
for t in domainName_counts[is_most_freq_domain].itertuples():
    domain_folder_path = os.path.join(OUTFOLDER, f"{t.domain}")
    os.makedirs(domain_folder_path, exist_ok=True)
    outpath = os.path.join(
        domain_folder_path,
        f"toFetchUrl_{platform}__{t.domain}.csv")
    df[df.domainName == t.domain].to_csv(outpath, index=False)
    print(f"Save {t.domainCounts} URLs to fecth at {outpath}.")
