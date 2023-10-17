#!/bin/bash

platform=$1
domain=$2

# data preprocessing
pyenv local linkfluence
echo Preprocessing data for platform $platform
python preprocessing.py --platform=$platform  --config=preprocessing_config.yaml

# fetch websites
echo Fetching website of domain $domain
minet fetch externalUrl /home/jimena/work/data/fundpet/scrapping/${domain}/toFetchUrl_${platform}__${domain}.csv \
--output /home/jimena/work/data/fundpet/scrapping/${domain}/fetchedUrl_${platform}__${domain}.csv \
--output-dir /home/jimena/work/data/fundpet/scrapping/${domain}/fechted \
--threads 100
