#!/bin/bash

# A simple ftp downloader for FLEX Historical
#
# Usage: ./fetch.sh <YYYYMMDD>
#
# - Wildcard is allowed to fetch multiple files
# - Downloaded files go `/jpxlab/downloads`
#

# edit these
FTP_USER=$FTP_USER
FTP_PASS=$FTP_PASS

[ -z "$FTP_USER" ] && { echo "Edit fetch.sh to set FTP_USER"; exit 1; }
[ -z "$FTP_PASS" ] && { echo "Edit fetch.sh to set FTP_PASS"; exit 1; }

[ $# -eq 0 ] && { echo "Usage: $0 <date YYYYMMDD>"; exit 1; }

DLDIR=$(cd "$(dirname "$0")./../downloads"; pwd)
DATE=$1

docker run -it \
  -v $DLDIR:/opt/ \
  lftp lftp -u $FTP_USER,$FTP_PASS \
  -e "lcd /opt; mget /archives/$DATE/StandardEquities_*.zip" \
  ftp.tmi.tse.or.jp
