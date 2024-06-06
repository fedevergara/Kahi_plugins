<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi SDS Mapping
Kahi will use this plugin for mapping sll the collections into the data structure for the Secretaría de Salud.

# Description
This plugin has the purpose of mapping the kahi data into the data structure for the Secretaría de Salud.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_sds_mapping
```

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  sds_mapping:
    database_url: localhost:27017
    database_name: sds
    num_jobs: 20
    verbose: 5
```

* WARNING *. This process could take several hours


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



