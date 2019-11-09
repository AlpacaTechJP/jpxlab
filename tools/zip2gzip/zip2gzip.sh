#!/bin/bash

if (( $# != 1 )); then
  echo "usage: zip2gzip.sh <zipfilename>"
  exit 1
fi

unzip -p -q $1 | gzip > ${1/%.zip/.gz}
