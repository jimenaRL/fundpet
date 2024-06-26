# `fundpet`


## 1. DOWNLOAD LF DUMP

## 2. PREPROCESS DATA


 
```
0 Load dump
1 Extract URLs from posts text
2 Explode list of found urls
3 Remove bad formatted urls (empty, typos)
4 Resolve urls
5 Get domain names
6 Remove entries from unwanted domains (e.g.: instagram, paypal, see config.yaml file.)
7 Drop entries exactly equal to their domain (e.g.: change.org, mesopinions.com/fr, see config.yaml files)
8 Append suffix to LF uids
9 Update SQLITE db with new valid entries
```
## 3. FETCH WEBSITES
