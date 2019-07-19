#!/bin/sh

set -e
set -u
set -x

export DATABASE_NAME=timetelldienstverlening
export DATABASE_USER=timetell
export DATABASE_PASSWORD=timetell
export DATABASE_HOST=localhost
export DATABASE_PORT=5444

export OBJECTSTORE_CONTAINER=uploadIVCLDI
export SQL_QUERY=dienstverlening
