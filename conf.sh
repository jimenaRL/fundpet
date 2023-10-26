#!/bin/bash

if [[ -z "${DATA}" ]]; then
  echo "Enviroment variable DATA is undefined."
  exit 1
else
  export LFFOLDER=$DATA/fundpet/linkfluence_petitioning_fundraising
fi


# ORIGINAL FILE COMMENT :
# These dates should cover the collection launched on Linkfluence dashboard
# start_timestamp = "2023-05-10T00:00:00-00:00"
# end_timestamp = "2023-07-27T23:59:59-00:00"
