import os
import ural
import pandas as pd
import numpy as np
from ural import get_domain_name

import matplotlib.pyplot as plt
import seaborn as sns
sns.set(rc={'figure.figsize': (20, 12)})


DATAPATH = os.path.join(os.environ["DATA"], "linkfluence")

start_timestamp = "2023-02-01T00:00:00-00:00"
end_timestamp = "2023-04-20T23:59:59-00:00"
suffix = f"twitter_start_{start_timestamp}_end_{end_timestamp}"

CAMPAIGNS = {
    # 'GermanFundraising':   385879,
    # 'GermanPetitioning':   385878,
    # 'FrenchFundraising':   385881,
    # 'FrenchPetitioning':   385880,
    # 'DutchFundraising':    385883,
    # 'DutchPetitioning':    385882,  # fails with ural pb
    # 'PolishFundraising':   385885,
    # 'PolishPetitioning':   385884,
    # 'RomanianFundraising': 385887,
    # 'RomanianPetitioning': 385886,
    # 'ItalianFundraising':  385889,
    # 'ItalianPetitioning':  385888,
    # 'SpanishFundraising':  385891,  # fails with ural pb
    'SpanishPetitioning':  385890,
}

THRESHOLD = 30

camp = 'FrenchFundraising'

for camp in CAMPAIGNS:

    path = os.path.join(DATAPATH, f"{camp}_{CAMPAIGNS[camp]}_{suffix}.csv.zip")
    figpath = os.path.join(f"{camp}.png")

    if os.path.exists(figpath):
        continue

    print(f"\n~~~~~~~~~~~~~~~~~~~~~~~~~~ {camp} ~~~~~~~~~~~~~~~~~~~~~~~~~~\n")

    df = pd.read_csv(path, dtype=str)

    l0 = len(df)
    df = df[~ df.externalUrl.isna()]
    l1 = len(df)
    print(f"Drop {l1 - l0} entries with no `externalUrl`, left {l1}.")

    df = df.assign(domainName=df.externalUrl.apply(get_domain_name))

    domainName_counts = df['domainName'].value_counts()
    principal_domains = domainName_counts[:THRESHOLD]
    principal_domains = principal_domains \
        .to_frame() \
        .reset_index() \
        .rename(columns={"index": "domain", "domainName": "count"})

    print(principal_domains)

    g = sns.barplot(
        data=principal_domains,
        y="domain",
        x="count",
        orient='h'
    )

    title = f"{camp}\n{THRESHOLD} first domains with more counts\n{len(df)} total"
    g.set(title=title)

    g.get_figure().savefig(figpath)

    print(f"Figure saved at {figpath}.")
