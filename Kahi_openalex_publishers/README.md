<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi OpenAlex publishers plugin 
Kahi will use this plugin to insert or update publishers information from openalex

# Description
Plugin that reads the information from a mongodb collection with openalex information to update or insert the information of the publishers in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_openalex_publishers
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have a copy of the openalex dumpwith the collection of venues which can be downloaded at [OpenAlex data dump website](https://docs.openalex.org/download-all-data/openalex-snapshot "OpenAlex data dump website") and import it on a mongodb database.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  openalex_publishers:
    database_url: localhost:27017
    database_name: openalex
    publishers_collection: publishers
    num_jobs: 20
    verbose: 2
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/

