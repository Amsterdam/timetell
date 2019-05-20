Timetell data importer
===

This project downloads Timetell csv-data from an ftp objectstore and imports it into a Postgres database.
The CSV data is exported by Timetell and placed on an ftp endpoint that we can reach.

See doc/ for more documentation regarding Timetell and its datamodel.


Install
=======
* Clone the repository
* Create a virtual env
* Install the requirements: `make install`

Getting started
==============
Before being able to run the commands described below, you need to have the following credentials to be able
to access the FTP objectstore.
* obj_store_password
* obj_store_project_id

Note that the obj_store_project_id is often communicated as part of the user, in the form of `project_id:username`.
 

Development
===========
* Create a local database, or run the database inside the docker container:
    > `docker-compose up database`
* Download the CSV files from the FTP endpoint, into a specified local_csv_download_dir/:
    > `ttldl -v -o --user <timetell:obj_store_password> <obj_store_project_id> uploads <local_csv_download_dir>`
* import datapunt views:
    > `ttli -v -d <local_csv_download_dir> postgresql://<db_user>:<db_password>@localhost/<db_name> datapunt`
* import dienstverlening views:
    > `ttli -v -d <local_csv_download_dir> postgresql://<db_user>:<db_password>@localhost/<db_name> dienstverlening`


Testing
=======
`make test` but there are no tests.
