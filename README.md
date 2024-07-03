# `fundpet`

Every week job for dutch, french,  german, italian, polish, romanian, and spanish.

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

## 3. COMPUTE STATS

## 4. FETCH WEBSITES

## 5. SCRAP CONTENT (to do)
