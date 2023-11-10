#!/bin/bash

client_id=$1
client_secret=$2
start=$3
end=$4
query=$5

## 0. set environement
pyenv local fundpet

# 1. dump download
printf 'FUNDPET 0: getting dumps'
python get_dump.py --query=$query --config=config.yaml --client_id=$client_id --client_secret=$client_secret --start=$start --end=$end

## 2. preprocess dump
printf 'FUNDPET 1: preprocessing dumps'
python preprocessing.py --query=$query --config=config.yaml --start=$start --end=$end

## 3. compute stats
printf 'FUNDPET 2: computing stats'
python stats.py --query=$query --config=config.yaml

# # 4. prepare files and fetch websites
printf 'FUNDPET 3: fetching websites'
python fetch.py --query=$query --config=config.yaml

# ## 5. scrap content from fetched websites
# # python scrappers/${domain}_scrapper.py
