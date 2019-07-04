#!/bin/sh

set -e
set -u
set -x

# don't forget to add / to access root folder
python downloader.py uploads /data --overwrite

python importer.py -d /data datapunt
