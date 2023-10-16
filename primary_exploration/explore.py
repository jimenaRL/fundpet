import os
import ural
import pandas as pd
import numpy as np
from ural import get_domain_name

DATAPATH = os.path.join(os.environ["DATA"], "linkfluence")

start_timestamp = "2023-02-01T00:00:00-00:00"
end_timestamp = "2023-04-20T23:59:59-00:00"
suffix = f"twitter_start_{start_timestamp}_end_{end_timestamp}"

CAMPAIGNS = {
    'GermanFundraising':   385879,
    'GermanPetitioning':   385878,
    'FrenchFundraising':   385881,
    'FrenchPetitioning':   385880,
    'DutchFundraising':    385883,
    'DutchPetitioning':    385882,
    'PolishFundraising':   385885,
    'PolishPetitioning':   385884,
    'RomanianFundraising': 385887,
    'RomanianPetitioning': 385886,
    'ItalianFundraising':  385889,
    'ItalianPetitioning':  385888,
    'SpanishFundraising':  385891,
    'SpanishPetitioning':  385890,
}

THRESHOLD = 30

stats = []
for camp, _id in CAMPAIGNS.items():

    path = os.path.join(DATAPATH, f"{camp}_{CAMPAIGNS[camp]}_{suffix}.csv.zip")
    figpath = os.path.join("domains", f"{camp}.png")
    csvpath = os.path.join("domains", f"{camp}.csv")

    df = pd.read_csv(path, dtype=str)
    e = len(df)
    dg = df[df.externalUrl.isna()]
    df = df[~ df.externalUrl.isna()]
    df = df[~ (df.externalUrl == "[]")]
    u = len(df)

    df = df.assign(domainName=df.externalUrl.apply(get_domain_name))

    domainName_counts = df['domainName'].value_counts()

    principal_domains = domainName_counts[:THRESHOLD]
    principal_domains = principal_domains \
        .to_frame() \
        .reset_index() \
        .rename(columns={"index": "domain", "domainName": "count"})
    d = len(domainName_counts)
    principal_domains.to_csv(csvpath, index=False)

    stats.append([camp, _id, e, u, d])

stats = pd.DataFrame(
    stats,
    columns=["campaing", "id", "entries", "externalUrl", "domains"])
# stats.to_csv("stats.csv", index=False)
print(stats)