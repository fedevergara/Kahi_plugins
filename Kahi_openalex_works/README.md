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
C++ library libhunspell-dev must be installed on your system. On ubuntu you can do it by typing
```shell
$ sudo apt install libhunspell-dev
```

# Similarity support
To process works without doi, similarity is mandaroty, then a elastic search server must be running. The plugin will use the server to find the most similar works in the database. 
To deploy it please read https://github.com/colav/Chia/tree/main/elasticsaerch and follow the instructions.

Docker and docker-compose are required to deploy the server.

if you only wants to process works with doi, you can skip this step and remove the es_index, es_url, es_user and es_password from the yaml file.

**But it is mandatory to put `openalex_works/doi` in the yaml file.**

The plugin creates the MongoDB indexes required by its work and author
lookups, including `works.external_ids.id_1` and `person.full_name_1`.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  openalex_works/doi:
    database_url: localhost:27017
    database_name: openalex
    collection_name: works
    num_jobs: 20
    es_index: kahi_es
    es_url: http://localhost:9200
    es_user: elastic
    es_password: colav
    verbose: 5
  openalex_works:
    database_url: localhost:27017
    database_name: openalex
    collection_name: works
    num_jobs: 20
    es_index: kahi_es
    es_url: http://localhost:9200
    es_user: elastic
    es_password: colav
    verbose: 5
```

* WARNING *. This process could take several hours

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
