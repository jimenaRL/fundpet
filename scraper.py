import os
import json
import pandas as pd
from bs4 import BeautifulSoup
import trafilatura


class Scraper():

    res = {}
    htmlPath = None

    def htmlPathExists(self, htmlPath, raiseError=True):
        exists = os.path.exists(htmlPath)
        if exists:
            return True
        else:
            if raiseError:
                raise FileNotFoundError(
                    f"Unnable to find html content at: '{exists}'.")
            else:
                return False

    def __init__(self, htmlPath: str):

        self.htmlPathExists(htmlPath)
        self.path = htmlPath
        with open(self.path) as f:
            self.soup = BeautifulSoup(f, 'html.parser')

    def scrape(self):
        return

    def response(self):
        return self.res


class ChangeOrg(Scraper):

    def scrappe(self):

        h1 = self.soup.h1
        if h1 is not None:
            self.res['h1'] = h1.text

        h2 = self.soup.h2
        if h2 is not None:
            self.res['h2'] = h2.text

        self.res['title'] = self.soup.title.text

        # pdivs = self.soup.find('div', class_="corgi-1wj70kl")
        # if pdivs is not None:
        #     if len(pdivs) > 0:
        #         self.res['p'] = []
        #         for c in pdivs.childGenerator():
        #             if c.name == 'p':
        #                 self.res['p'].append(c.text)

        # find the author of the petition,
        # the balises could be different from a page to another
        attrs = {"data-testid": "user-profile-page-link"}
        if self.soup.find("a", attrs=attrs):
            for author in self.soup.find_all("a", attrs=attrs):
                self.res["author"] = author.get_text()
        elif self.soup.find("a", class_="corgi-1ct102y"):
            for author in self.soup.find_all("a", class_="corgi-1ct102y"):
                self.res["author"] = author.get_text()
        # find the organised to which the petition is addressed
        for addressed in self.soup.find_all("div", class_="corgi-z5fbhm"):
            atext = addressed.find("div", class_="corgi-jfeg2y").get_text()
            if "Adressée à" in atext or "Petition to" in atext or "Petición para" in atext:
                self.res["addressed_to"] = addressed.find(
                    "div", class_="corgi-1lk6gf3").get_text()
            else:
                self.res["addressed_to"] = ""
        sign = self.soup.find("span", class_="corgi-fnmi1p")
        if sign:
            # volume of signature that have been collected the day of the scrap
            self.res["signatures"] = (sign.get_text().replace("\u202f", ""))
        goal = self.soup.find("span", class_="corgi-1xmxvqx")
        if goal:
            # the goal
            self.res["goal"] = int(
                goal.get_text()
                .replace("\u202f", "").replace(',', '').replace('.', ''))
        date = self.soup.find("div", class_="corgi-1lk6gf3")
        if date:
            # the date of petition
            self.res["launched"] = date.get_text()
        text = self.soup.find("div", attrs={"data-qa": "description-content"})
        if text:
            # the text
            self.res["text"] = text.get_text()

        # add extra info with Trafilatura
        # with open(self.path) as f:
        #     traf = json.loads(
        #         trafilatura.extract(
        #             f.read(),
        #             favor_recall=True,
        #             include_comments=True,
        #             include_formatting=True,
        #             output_format='json'))
        # traf.update({"source_hostname": traf['source-hostname']})
        # del traf['source-hostname']
        # self.res.update({f"traf_{k}": v for k, v in traf.items()})
