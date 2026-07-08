<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi CIARP works plugin 
Kahi will use this plugin to insert or update the works information from ciarp office file from public universities.

# Description
Plugin that reads papers published and reported to the CIARP office from MongoDB to update or insert the information of the research products in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install Kahi_ciarp_works
```
# Similarity support
To process works without doi, similarity is mandaroty. Then a elastic search server must be running. The plugin will use the server to find the most similar works in the database. To deply it please read https://github.com/colav/Chia/tree/main/elasticsaerch and follow the instructions.

Docker and docker-compose are required to deploy the server.

if you only wants to process works with doi, you can skip this step and remove the es_index, es_url, es_user and es_password from the yaml file.

**But it is mandatory to put `ciarp_works/doi` in the yaml file.**


# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  ciarp_works/doi:
    database_url: localhost:27017
    database_name: institutional_data
    ciarp_collection_name: ciarp
    num_jobs: 1
    verbose: 4
  ciarp_works:
    database_url: localhost:27017
    database_name: institutional_data
    ciarp_collection_name: ciarp
    num_jobs: 1
    verbose: 4
```

* WARNING *. This process could take several hours

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/
