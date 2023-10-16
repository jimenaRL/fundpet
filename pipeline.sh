#!/bin/bash

pyenv local linkfluence
python scrapping/preprocessing.py --platform=twitter  --config=scrapping/preprocessing_config.yaml
python scrapping/preprocessing.py --platform=facebook --config=scrapping/preprocessing_config.yaml

