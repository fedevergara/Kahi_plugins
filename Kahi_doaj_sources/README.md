<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi scimago sources plugin 
Kahi will use this plugin to insert or update the journal information from DOAJ

# Description
Plugin that reads the information from a mongodb collection with doaj json information to update or insert the information of the journals in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_doaj_sources
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have a copy of the DOAJ dump which can be downloaded at [DOAJ data dump website](https://doaj.org/docs/public-data-dump/ "DOAJ data dump website") and import it on a mongodb database.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  doaj_sources:
    database_url: localhost:27017
    database_name: doaj
    collection_name: stage
    verbose: 5
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/




