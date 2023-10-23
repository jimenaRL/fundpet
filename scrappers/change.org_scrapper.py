import os
import json
from tqdm import tqdm
from glob import glob
from bs4 import BeautifulSoup
import pandas as pd
import trafilatura

DOMAIN = 'change.org'
BASEFOLDER = "/home/jimena/work/data/fundpet/scrapping"
FOLDER = os.path.join(BASEFOLDER, DOMAIN)
paths = glob(os.path.join(FOLDER,'fetched', '*.html'))
uids = [os.path.split(p)[1].replace('.html', '') for p in paths]

outpath = os.path.join(FOLDER, f'extrated_content_{DOMAIN}.csv')

res = {
     uid : {
        'h1': None,
        'h2': None,
        'title': None,
        'p': [],
        'comments': None
    } for uid in uids
}

for uid, path in tqdm(zip(uids, paths)):

    # add info with BeautifulSoup
    with open(path) as f:
        soup = BeautifulSoup(f, 'html.parser')

    h1 = soup.h1
    if h1 is not None:
        res[uid]['h1'] = h1.text

    h2 = soup.h2
    if h2 is not None:
        res[uid]['h2'] = h2.text

    res[uid]['title'] = soup.title.text

    pdivs = soup.find('div', class_="corgi-1wj70kl")
    if pdivs is not None:
        if len(pdivs) > 0:
            for c in pdivs.childGenerator():
                if c.name == 'p':
                    res[uid]['p'].append(c.text)

    # add info with Trafilatura
    with open(path) as f:
        filecontent = f.read()

    res[uid].update(json.loads(
        trafilatura.extract(
            filecontent,
            favor_recall=True,
            include_comments=True,
            include_formatting=True,
            output_format='json')))


df = pd.DataFrame.from_records(data=res)
df.to_csv(outpath, index=False)
print(f"Content extracted from {len(df)} sited saved to {outpath}.")
print(df)