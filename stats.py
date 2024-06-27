import os
from glob import glob

from sqlite import \
    populateSqlite, \
    retrieve_table

def stats(query, logger):

    # 1. Retrive data from table
    ########################################################
    df = retrieve_table(table=query, verbose=True)
    n = len(df)

    # 2. Make aggregations
    ########################################################

    # (2a) Agreggate URLs
    url_counts = df['resolvedUrl'].value_counts()
    url_counts = url_counts \
        .to_frame() \
        .reset_index() \
        .rename(columns={"count": "urlCount"})
    u = len(url_counts)
    logger.info(
        f"STATS: there are {u} unique urls of a total of {n} ({100*u/n:.2f}%)")

    df = df.merge(url_counts, on='resolvedUrl')
    df = df.groupby('resolvedUrl').first().reset_index() \
        .sort_values(by='urlCount', ascending=False)

    # (2b) Domains stats
    domain_counts = df['domain'].value_counts()
    domain_counts = domain_counts \
        .to_frame() \
        .reset_index() \
        .rename(columns={"count": "domainCount"})
    d1 = len(domain_counts)

    logger.info(f"STATS: there are {d1} unique domains.")

    url_counts = url_counts[["resolvedUrl", "urlCount"]]
    domain_counts = domain_counts[["domain", "domainCount"]]

    return url_counts, domain_counts
