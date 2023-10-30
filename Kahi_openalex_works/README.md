<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi works plugin 
Kahi will use this plugin to insert or update the works information from openalex database.

# Description
Plugin that read the information from a mongodb database with openalex information to update or insert the information of the research products in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_openalex_works
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
For the data dependencies the user must have a copy of the openalex dump with the collection of works of interest (take a subset since this database is huge) which can be downloaded at [OpenAlex data dump website](https://docs.openalex.org/download-all-data/openalex-snapshot "OpenAlex data dump website") and import it on a mongodb database.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  openalex_works:
    num_jobs: 5
    verbose: 5
    database_url: localhost:27017
    database_name: openalexco
    collection_name: works
```

* WARNING *. This process could take several hours

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
