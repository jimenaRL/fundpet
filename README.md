# `fundpet`


┌───┬───────────────────────────┬───────────────────────────┬───────────────────────────┬───────────────────────────┬──────────┬────────────────┬──────────┬──────────────────────────┬─────────┬───────────┬──────────┬──────────┐
│ - │ permalink                 │ uid                       │ text                      │ resolvedUrl               │ platform │ query          │ domain   │ date                     │ fetched │ fetchDate │ htmlPath │ scrapped │
├───┼───────────────────────────┼───────────────────────────┼───────────────────────────┼───────────────────────────┼──────────┼────────────────┼──────────┼──────────────────────────┼─────────┼───────────┼──────────┼──────────┤
│ 0 │ https://x.com/FOTS352809… │ r3_prod_5760-16580716141… │ RT @mllesyara: Story vac… │ https://support.verse.me… │ twitter  │ SoMe4DemFrench │ verse.me │ 2023-05-15T11:27:32.000Z │ 0       │ 0         │ <empty>  │ 0        │
│ 1 │ https://x.com/mllesyara/… │ r3_prod_5760-16584674874… │ Être a mes pieds se méri… │ https://support.verse.me… │ twitter  │ SoMe4DemFrench │ verse.me │ 2023-05-16T13:40:36.000Z │ 0       │ 0         │ <empty>  │ 0        │
│ 2 │ https://x.com/GogGogy/st… │ r3_prod_5760-16584685643… │ RT @mllesyara: Être a me… │ https://support.verse.me… │ twitter  │ SoMe4DemFrench │ verse.me │ 2023-05-16T13:44:53.000Z │ 0       │ 0         │ <empty>  │ 0        │
│ 3 │ https://x.com/VLemois/st… │ r3_prod_5760-16584789348… │ RT @mllesyara: Être a me… │ https://support.verse.me… │ twitter  │ SoMe4DemFrench │ verse.me │ 2023-05-16T14:26:05.000Z │ 0       │ 0         │ <empty>  │ 0        │
│ 4 │ https://x.com/mllesyara/… │ r3_prod_5760-16587862274… │ 40€ a rembourser sur pay… │ https://support.verse.me… │ twitter  │ SoMe4DemFrench │ verse.me │ 2023-05-17T10:47:10.000Z │ 0       │ 0         │ <empty>  │ 0        │
└───┴───────────────────────────┴───────────────────────────┴───────────────────────────┴───────────────────────────┴──────────┴────────────────┴──────────┴──────────────────────────┴─────────┴───────────┴──────────┴──────────┘



Every week job for dutch, french,  german, italian, polish, romanian, and spanish.

`SoMe4DemFrench_urlCounts` table

| - | domain          | domainCount |
|---| --------------- | ----------- |
| 1 | helloasso.com   | 25643       |
| 2 | mesopinions.com | 20779       |
| 3 | change.org      | 1054        |
| 4 | teaming.net     | 124         |
| 5 | wishtender.com  | 85          |

`SoMe4DemFrench_urlCounts` table

| - | resolvedUrl                                                                            | urlCount |
|---| -------------------------------------------------------------------------------------- | -------- |
| 1 | https://www.mesopinions.com/fr                                                         | 28239    |
| 2 | https://www.helloasso.com/page-introuvable                                             | 1885     |
| 3 | https://www.l214.com/enquetes/2023/abattoir-de-bazas/                                  | 1655     |
| 4 | https://www.change.org/p/maintenir-l-allocation-sp%C                                   | 1347     |
| 5 | https://www.helloasso.com/associations/association-urgences-fourrieres/formulaires/2   | 782      |

`SoMe4DemFrench` table

| - | permalink                 | uid                       | text                      | resolvedUrl               | platform | query          | domain   | date                     | fetched | fetchDate | htmlPath | scrapped |
| - | ------------------------- | ------------------------- | ------------------------- | ------------------------- | -------- | -------------- | -------- | ------------------------ | ------- | --------- | -------- | --------- |
| 0 | https://x.com/FOTS352809… | r3_prod_5760-16580716141… | RT @mllesyara: Story vac… | https://support.verse.me… | twitter  | SoMe4DemFrench | verse.me | 2023-05-15T11:27:32.000Z | 0       | 0         | <empty>  | 0         |
| 1 | https://x.com/mllesyara/… | r3_prod_5760-16584674874… | Être a mes pieds se méri… | https://support.verse.me… | twitter  | SoMe4DemFrench | verse.me | 2023-05-16T13:40:36.000Z | 0       | 0         | <empty>  | 0         |
| 2 | https://x.com/GogGogy/st… | r3_prod_5760-16584685643… | RT @mllesyara: Être a me… | https://support.verse.me… | twitter  | SoMe4DemFrench | verse.me | 2023-05-16T13:44:53.000Z | 0       | 0         | <empty>  | 0         |
| 3 | https://x.com/VLemois/st… | r3_prod_5760-16584789348… | RT @mllesyara: Être a me… | https://support.verse.me… | twitter  | SoMe4DemFrench | verse.me | 2023-05-16T14:26:05.000Z | 0       | 0         | <empty>  | 0         |
| 4 | https://x.com/mllesyara/… | r3_prod_5760-16587862274… | 40€ a rembourser sur pay… | https://support.verse.me… | twitter  | SoMe4DemFrench | verse.me | 2023-05-17T10:47:10.000Z | 0       | 0         | <empty>  | 0         |



## 1. DOWNLOAD LF DUMP

## 2. PREPROCESS DATA
 
```
0 Load dump
1 Extract URLs from posts text
2 Explode list of found urls
3 Remove bad formatted urls (empty and typos)
4 Resolve urls
5 Get domain names
6 Remove entries from unwanted domains (e.g.: instagram or paypal, see config.yaml file)
7 Drop entries exactly equal to their domain (e.g.: change.org or mesopinions.com/fr, see config.yaml files)
8 Append suffix to LF uids
9 Update SQLITE db with new valid entries
```

## 3. COMPUTE STATS TABLES ON SQLITE DB

```
0 Update url counts
1 Update domain counts
```

## 4. FETCH WEBSITES


```
0 Fetch html from urls in `SoMe4DemFrench` table which domain are in config.yml
1 Update SoMe4DemFrench table with fetch information
```
## 5. SCRAP CONTENT (to do)
