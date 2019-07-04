#!/bin/sh

set -e
set -u
set -x

# export DATABASE_NAME=timetell
# export OBJECTSTORE_CONTAINER=uploads
# export SQL_QUERY=datapunt

# export DATABASE_NAME=timetelldienstverlening
# export OBJECTSTORE_CONTAINER=uploadIVCLDI
# export SQL_QUERY=dienstverlening

# Don't forget to add / to access root folder for local path
python downloader.py $OBJECTSTORE_CONTAINER /data --overwrite
python importer.py -d /data $SQL_QUERY
