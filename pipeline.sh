#!/bin/bash

client_id=$1
client_secret=$2
start=$3
end=$4

## 0. set environement
pyenv local fundpet

# 1. dump download
printf '\nFUNDPET 0: getting dumps\n'
python get_dump.py --config=config.yaml --client_id=$client_id --client_secret=$client_secret --start=$start --end=$end

## 2. preprocess dump
printf '\nFUNDPET 1: preprocessing dumps\n'
python preprocessing.py --config=config.yaml

## 3. compute stats
printf '\nFUNDPET 2: computing stats\n'
python stats.py --config=config.yaml

# 4. prepare files and fetch websites
printf '\nFUNDPET 3: fetching websites\n'
python fetch.py --config=config.yaml

## 5. scrap content from fetched websites
# python scrappers/${domain}_scrapper.py
