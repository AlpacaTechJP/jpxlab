#!/bin/bash

# edit these
FTP_USER=
FTP_PASS=

[ -z "$FTP_USER" ] && { echo "Edit fetch.sh to set FTP_USER"; exit 1; }
[ -z "$FTP_PASS" ] && { echo "Edit fetch.sh to set FTP_PASS"; exit 1; }

[ $# -eq 0 ] && { echo "Usage: $0 <date YYYYMMDD>"; exit 1; }

DLDIR=$(cd "$(dirname "$0")./../downloads"; pwd)
DATE=$1

docker run \
  -v $DLDIR:/opt/ \
  aria2 aria2c -c \
    --ftp-user=$FTP_USER \
    --ftp-passwd=$FTP_PASS \
    -d /opt/ \
    ftp://ftp.tmi.tse.or.jp/archives/$DATE/StandardEquities_$DATE.zip
