#!/bin/bash

client_id=$1
client_secret=$2

# 0. set environement
pyenv local linkfluence

# # 1. dump download
# python get_dump.py --config=config.yaml --client_id=$client_id --client_secret=$client_secret

# # 2. preprocess dump
# python preprocessing.py --config=config.yaml

# 3. compute stats
# python stats.py --config=config.yaml

# 4. fetch websites
python fetch.py --config=config.yaml

# 5. scrap content from fetched websites
# python scrappers/${domain}_scrapper.py
