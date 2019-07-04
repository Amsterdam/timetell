#!/bin/sh

set -e
set -u
set -x

export DATABASE_NAME=timetelldienstverlening
export OBJECTSTORE_CONTAINER=uploadIVCLDI
export SQL_QUERY=dienstverlening
