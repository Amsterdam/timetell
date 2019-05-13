Timetell data importer
===

This project downloads Timetell csv-data from an ftp objectstore and imports it into a Postgres database.

See doc/ for more documentation.


Install
=======
* Clone the repository
* Create a virtual env
* Install the requirements: `make install`


Development
===========
* Create a local database, or run the database inside the docker container
* import data:
    > `ttldl -v -o --user <timetell:obj_store_password> <obj_store_project_id> uploads <local_csv_download_dir>`
* import:
    > `ttli -v -d <local_csv_download_dir> postgresql://<db_user>:<db_password>@localhost/<db_name> datapunt`
* import:
    > `ttli -v -d <local_csv_download_dir> postgresql://<db_user>:<db_password>@localhost/<db_name> dienstverlening`


Testing
=======
`make test` but there are no tests.
