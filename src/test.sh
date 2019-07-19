#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

# echo "Waiting for db"
./docker-wait.sh
./docker-migrate.sh

echo "Running style checks"
flake8 app/ois-projects-employees

echo "Running unit tests"
./manage.py test