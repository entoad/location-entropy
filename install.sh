#!/usr/bin/env bash -e

ln -sF total-air-passenger-arrivals-by-country.csv /tmp/data.csv

if [[ -z "$(which pyspark)" ]]; then
  echo "PySpark is not installed so exiting"
  exit 1
fi

if [[ -z "$(which jupyter)" ]]; then
  echo "Jupyter is not installed so exiting"
  exit 1
fi
