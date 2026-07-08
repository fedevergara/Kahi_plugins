<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias opendata plugin 
Kahi will use this plugin to insert or update the works information from the minciencias opendata database.

# Description
Plugin that reads the information from minciencias opendata database to insert or update the information of the of academic products in colav's database.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_minciencias_opendata_works
```
# Similarity support
The plugin resolves DOI and Scienti identifiers exactly. Works without identifiers use Elasticsearch similarity before an optional insertion. To deploy the similarity service, read https://github.com/colav/Chia/tree/main/elasticsaerch and follow the instructions.

Docker and docker-compose are required to deploy the server.


# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  minciencias_opendata_works:
    es_index: kahi_es
    es_url: http://localhost:9200
    es_user: elastic
    es_password: colav
    database_url: localhost:27017
    database_name: yuku
    collection_name: gruplac_production_data
    person_related_works: true
    person_collection: person
    insert_all: true
    thresholds: [65, 90, 95]
    num_jobs: 6
    verbose: 1
```
* WARNING *. This process can take more than an hour.

Note: 
-In case you want to insert all documents that fail to be associated through the similarity processes as new documents, you need to change the value of the insert_all flag to True in the workflow
-The thresholds parameter only accepts a list of three corresponding values for: A threshold for author names, a low threshold for works and a high threshold for works.
-`person_related_works: true` promotes the enriched references stored by `Kahi_minciencias_opendata_person` into `works`. Resolution is attempted by DOI, Scienti identifier, deterministic title fingerprint, and Elasticsearch similarity, in that order.
-Works without a bibliographic identifier receive a `minciencias_title_fingerprint` ingestion key so repeated runs remain idempotent.

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
