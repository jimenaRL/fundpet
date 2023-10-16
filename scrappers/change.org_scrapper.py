import os
from tqdm import tqdm
from glob import glob
from bs4 import BeautifulSoup

FOLDER = "/home/jimena/work/dev/fundpet/externalUrl"
paths = glob(os.path.join(FOLDER, '*.html'))
uids = [os.path.split(p)[1].replace('.html', '') for p in paths]
# Open File
res = {
     uid : {
        'h1': None,
        'h2': None,
        'title': None,
        'p': []
    } for uid in uids
}

for uid in tqdm(uids):
    path = os.path.join(FOLDER, f'{uid}.html')
    with open(path) as f:
        soup = BeautifulSoup(f, 'html.parser')

        h1 = soup.h1
        if h1 is not None:
            res[uid]['h1'] = h1.text

        h2 = soup.h2
        if h2 is not None:
            res[uid]['h2'] = h2.text

        res[uid]['title'] = soup.title

        pdivs = soup.find_all('div', class_="corgi-1wj70kl")
        if len(pdivs) > 0:
            if len(pdivs) > 1:
                raise ValueError('RARO')
            else:
                for c in ddiv.childGenerator():
                    if c.name == 'p':
                        res[uid]['p'].append(c.text)
                    else:
                        raise ValueError('RARITO')

