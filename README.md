# `fundpet`

Every week job for dutch, french,  german, italian, polish, romanian, and spanish.

| - | domain          | domainCount |
|---| --------------- | ----------- |
| 1 | helloasso.com   | 25643       |
| 2 | mesopinions.com | 20779       |
| 3 | change.org      | 1054        |
| 4 | teaming.net     | 124         |
| 5 | wishtender.com  | 85          |


| - | resolvedUrl                                                                            | urlCount |
|---| -------------------------------------------------------------------------------------- | -------- |
| 1 | https://www.mesopinions.com/fr                                                         | 28239    |
| 2 | https://www.helloasso.com/page-introuvable                                             | 1885     |
| 3 | https://www.l214.com/enquetes/2023/abattoir-de-bazas/                                  | 1655     |
| 4 | https://www.change.org/p/maintenir-l-allocation-sp%C                                   | 1347     |
| 5 | https://www.helloasso.com/associations/association-urgences-fourrieres/formulaires/2   | 782      |


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

## 5. SCRAP CONTENT (to do)
