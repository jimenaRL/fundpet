#!/bin/bash

client_id=$1
client_secret=$2

# 0. set environement
source conf.sh
pyenv local linkfluence

# 1. dump download
python get_dump.py --config=config.yaml --client_id=$client_id --client_secret=$client_secret

# 2. preprocess dump
python preprocessing.py --config=config.yaml

# # fetch websites
# minet fetch externalUrl /home/jimena/work/data/fundpet/scrapping/${domain}/toFetchUrl_${platform}__${domain}.csv \
# --output /home/jimena/work/data/fundpet/scrapping/${domain}/fetchedUrl_${platform}__${domain}.csv \
# --output-dir /home/jimena/work/data/fundpet/scrapping/${domain}/fechted \
# --threads 100

# # # extarct content from fetched sites
# # python scrappers/${domain}_scrapper.py
