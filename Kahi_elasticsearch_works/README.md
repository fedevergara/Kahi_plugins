<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi ElasticSearch works plugin 
Kahi will use this plugin to insert works information from kahi into elasticsearch database.

# Description
Plugin that read the information from a mongodb database with information in colav database to insert the information of the research products in elastic search. This serves as a solution to the uniocity problem when there's no DOI available.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_elasticsearch_works
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
For the data dependencies the user must have the output of any [Kahi's work plugin](https://github.com/colav/Kahi-plugins) with the resulting database with the academic works.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  elasticsearch_works/delete:
    es_index: kahi_es
    es_url: http://localhost:9200
    es_user: elastic
    es_password: colav
    task: delete
    debug: false
    verbose: 5
  elasticsearch_works/bulk_insert:
    es_index: kahi_es
    es_url: http://localhost:9200
    es_user: elastic
    es_password: colav
    task: bulk_insert
    bulk_size: 100
    verbose: 5
```
The task options are:
* delete: deletes the configured index when it exists; otherwise it completes successfully without creating it
* bulk_insert: inserts the registers from kahi's resulting database in chunks of bulk_size

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
