# Timetell data importer
[![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/) [![MPL 2.0](https://img.shields.io/badge/license-MPLv2.0-blue.svg)](https://www.mozilla.org/en-US/MPL/2.0/)

This project downloads Timetell csv-data from an ftp objectstore and imports it into a Postgres database for use in our Project Management Dashboards using Tableau Server.
The CSV data is exported to an objectstore by Timetell.

## Documentation
See the Timetell [documentation](doc/) for more information regarding the data model.



## How to use

Before being able to run the commands described below, you need to have the following environment credentials added to be able to access the objectstore. This is also needed for configuring these variables in production with our Ansible/Jenkins pipeline.

### Environment variables:
```
export OBJECTSTORE_USERNAME=timetell
export OBJECTSTORE_PASSWORD=#################
export OBJECTSTORE_PROJECT_ID=#################
```

Clone the repository:
```
git clone https://github.com/Amsterdam/timetell.git
cd timetell
```

## Running using [Docker](https://www.docker.com)
```
docker-compose build
docker-compose up
```

## Local Development

1. Start the database using Docker from the root folder
`docker-compose up -d database`
2. Create a virtual environment, for example in the root folder:
```
virtualenv --python=$(which python3) venv
source venv/bin/activate
```
3. Install the necessary python packages
```
cd src/import
pip install -r requirements.txt
```
4. Download the csv files to a local folder 
`python download.py upload data`
5. Import the data and create views for a specific customer: datapunt or dienstverlening.
`python importer.py data datapunt`
