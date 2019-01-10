Timetell data importer
===

This project imports Timetell data it receives through POST requests into a Postgress database.



Install
=======
* Clone the repository
* Create a virtual env
* Install the requirements: `make install`


Development
===========
* Create a local database
* import data:
    > `ttldl -v -o --user <timetell:obj_store_password> <obj_store_project> uploads <destination_folder>`
* import:
    > `ttli -v -d <csv_download_dir> postgresql://<db_user>:<db_password>@localhost/<db_name> datapunt`
* import:
    > `ttli -v -d <csv_download_dir> postgresql://<db_user>:<db_password>@localhost/<db_name> dienstverlening`


Testing
=======
`make test` but there are no tests.
