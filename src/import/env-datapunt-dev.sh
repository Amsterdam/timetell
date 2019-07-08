#!/bin/sh

set -e
set -u
set -x

export DATABASE_NAME=timetell
export OBJECTSTORE_CONTAINER=uploads
export SQL_QUERY=datapunt
