#!/bin/sh

set -e
set -u
set -x

export DATABASE_NAME=timetell
export DATABASE_USER=timetell
export DATABASE_PASSWORD=timetell
export DATABASE_HOST=localhost
export DATABASE_PORT=5444

export OBJECTSTORE_CONTAINER=uploads
export SQL_QUERY=datapunt
