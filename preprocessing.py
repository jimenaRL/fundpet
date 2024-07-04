import os
import re
import yaml
import minet
import tempfile
import subprocess
from tqdm import tqdm

import pandas as pd
from ural import get_domain_name, urls_from_text, is_typo_url

from utils import get_date, get_path, get_dbpath, get_folder


def fn(s):
    s = s.replace('https://', '').replace('http://', '')
    s = s.replace('www.', '')
    s = re.sub(r"/$", "", s)
    return s

def preprocessDumps(
    query,
    start,
    end,
    platforms,
    domain_freq_threshold,
    ignored_domains,
    ignored_urls,
    ignored_urls_same_as_domain,
    logger):

    DBPATH = get_dbpath()
    START, END = get_date(start, end)

    PREPROCESSECOLUMNS = [
            'permalink', 'uid', 'id', 'text', 'externalUrl', 'platform', 'query', 'date']

    df = []

    # 1. Load dumps
    ########################################################
    for platform in platforms:
        # load linkfluence dump and standarize url column name
        dump_path = get_path(
            platform, query, START, END, 'dumps')
        if os.path.exists(dump_path):
            logger.info(f"PREPROCESS: analyzing {platform} dump from {dump_path}.")
            df.append(pd.read_csv(dump_path, dtype=str)[PREPROCESSECOLUMNS])
        else:
            logger.info(f"PREPROCESS: skipping {query} from platform {platform}. \
            Unnable to find dumps at {dump_path}.")
            continue
    df = pd.concat(df)

    # 1. Process dump
    ########################################################

    logger.info('PREPROCESS: extracting URLs from posts text')

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
    logger.info(
        f"PREPROCESS: removing {n0 - len(df)} with typo urls, left {len(df)}.")

    # (1b) Resolve urls
    logger.info('PREPROCESS: resolving URLs')
    with tempfile.NamedTemporaryFile() as tmp1:
        df.drop_duplicates(subset=['urlFromText'], keep='first').to_csv(tmp1.name, index=False)
        with tempfile.NamedTemporaryFile() as tmp2:
            command_pipe = [
                "minet",
                "resolve",
                "urlFromText",
                "-i",
                f"{tmp1.name}",
                "-o",
                f"{tmp2.name}"
            ]
            os.system(' '.join(command_pipe))
            tmp = pd.read_csv(
                tmp2.name,
                usecols=['urlFromText', 'resolved_url'],
                dtype=str) \
                .rename(columns={'resolved_url': 'resolvedUrl'})

    df = df.merge(tmp, on='urlFromText')

    n = len(df)
    df = df[~df.resolvedUrl.isnull()]
    logger.info(
        f"PREPROCESS: drop {n - len(df)} entries with null `resolvedUrl`, left {len(df)}.")

    # (1c) Get domain names from URLs
    logger.info('PREPROCESS: getting domain names...')
    df = df.assign(
        domain=df['resolvedUrl'].fillna("").apply(get_domain_name))

    # (1c) Remove URLs from domains to ignore
    for domain in ignored_domains:
        n = len(df[df.domain == domain])
        if n > 0:
            logger.info(f"Removing {n} URLs whose domain name is {domain}")
            df = df[df.domain != domain]

    logger.info(f"PREPROCESS: left {len(df)} entries.")

    # (1d) Drop urls equals to their domain
    urlSameAsDom = (df['resolvedUrl'].fillna("").apply(fn) == df['domain'])
    df = df.assign(urlSameAsDomain=urlSameAsDom)

    for domain in ignored_urls_same_as_domain:
        n = len(df)
        df = df[~((df.domain == domain) & df.urlSameAsDomain)]
        d = n - len(df)
        if d > 0:
            logger.info(
                f"PREPROCESS: drop {d} entries with url equal to its domain {domain}.")


    # (1d) Drop unwanted
    for url in ignored_urls:
        n = len(df[df.resolvedUrl == url])
        if n > 0:
            logger.info(f"Removing {n} entries whose resolvedUrl is {url}")
            df = df[df.resolvedUrl != url]


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

    return df[columns]