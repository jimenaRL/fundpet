# `fundpet`

## PREPROCESSING STEPS


 
```
0 Load dump
1 Extract URLs from posts text
2 Explode list of found urls
3 Remove bad formatted urls (empty, typos)
4 Resolve urls
5 Get domain names
6 Remove entries from unwanted domains (instagram, paypal, etc.)
7 Drop entries exactly equal to their domain (change.org, mesopinions.com/fr)
8 Append suffix to LF uids
9 Update SQLITE db with new valid entries
```
