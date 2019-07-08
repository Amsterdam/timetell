# Timetell data importer
[![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/) [![MPL 2.0](https://img.shields.io/badge/license-MPLv2.0-blue.svg)](https://www.mozilla.org/en-US/MPL/2.0/)

This project downloads Timetell csv-data from an ftp objectstore and imports it into a Postgres database for use in our Project Management Dashboards using Tableau Server.
The CSV data is exported to an objectstore by Timetell.

## Documentation
See the Timetell [documentation](doc/) for more information regarding the data model.



## How to run

### Clone the repository
```
git clone https://github.com/Amsterdam/timetell.git
cd timetell
```

### Set objectstore environment variables
```
export OBJECTSTORE_USERNAME=timetell
export OBJECTSTORE_PASSWORD=#################
export OBJECTSTORE_PROJECT_ID=#################
```
### Set container sql query and database name
For each organisation specific arguments need to be set to load them correctly and separately into each dedicated database. For example:
```
export DATABASE_NAME=timetell
export OBJECTSTORE_CONTAINER=uploads
export SQL_QUERY=datapunt
```

These can be set by running one of these bash commands:

#### Onderzoek, informatie, Statistiek
`sh ./src/import/env-datapunt-dev.sh`

#### Informatievoorziening Cluster Dienstverlening
`sh ./src/import/env-dienstverlening-dev.sh`

## Running using [Docker](https://www.docker.com)
```
docker-compose build
docker-compose up
```

## Local Development

1. Start the database using Docker from the root folder using the specified DATABASE_NAME argument.
`docker-compose up -d database`
2. Create a virtual environment, for example in the root folder.
```
virtualenv --python=$(which python3) venv
source venv/bin/activate
```
3. Install the necessary Python packages.
```
cd src/import
pip install -r requirements.txt
```
4. Download the csv files to a local folder. (it uses the default overwrite option) 

`python download.py -s $OBJECTSTORE_CONTAINER -t data`

5. Import the data and create views using earlier set SQL_QUERY and the DATABASE_NAME arguments

`python import.py -s data $SQL_QUERY`

# How to access database locally after import

The database can be accessed locally with pgadmin for example using:
- host: localhost
- port: 5444
- database: timetell (or timetelldienstverlening)
- username: timetell
- password: insecure
