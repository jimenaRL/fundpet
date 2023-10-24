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
            res[uid]['p'] = []
            for c in pdivs.childGenerator():
                if c.name == 'p':
                    res[uid]['p'].append(c.text)

     #find the author of the petition, the balises could be different from a page to another
    if soup.find("a", attrs={"data-testid":"user-profile-page-link"}):
        for auteur in soup.find_all("a", attrs={"data-testid":"user-profile-page-link"}):
            res[uid]["auteur"] = auteur.get_text()
            res[uid]["balise"] = "data-testid"
    elif soup.find("a", class_="corgi-1ct102y"):
        for auteur in soup.find_all("a", class_="corgi-1ct102y"):
            res[uid]["auteur"] = auteur.get_text()
            res[uid]["balise"] = "class"
    #find the organised to which the petition is addressed
    for addressed in soup.find_all("div", class_="corgi-z5fbhm"):
        adresse = addressed.find("div", class_="corgi-jfeg2y").get_text()
        if "Adressée à" in adresse or "Petition to" in adresse:
            res[uid]["adressed_to"] = addressed.find("div", class_="corgi-1lk6gf3").get_text()
        else:
            res[uid]["adressed_to"] = ""

    sign = soup.find("span", class_="corgi-fnmi1p")
    if sign:
        res[uid]["stat"]= (sign.get_text().replace("\u202f",""))# volume of signature that have been collected the day of the scrap
    goal = soup.find("span", class_="corgi-1xmxvqx")
    if goal:
        res[uid]["goal"] = int(goal.get_text().replace("\u202f",""))# the goal
    date = soup.find("div", class_="corgi-1lk6gf3")
    if date:
        res[uid]["date"] = date.get_text() # the date of petition
    text = soup.find("div", attrs={"data-qa":"description-content"})
    if text:
        res[uid]["text"] = text.get_text() #the text


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


df = pd.DataFrame.from_records(data=res).T
df.to_csv(outpath, index=False)
print(f"Content extracted from {len(df)} sited saved to {outpath}.")
print(df)