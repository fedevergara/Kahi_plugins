<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi staff affiliations plugin 
Kahi will use this plugin to insert or update the affiliations information from institution's staff file

# Description
Plugin that reads staff information from MongoDB to update or insert the faculties and departments in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_staff_affiliations
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have the institutional staff data loaded in MongoDB.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as:
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  staff_affiliations:
    database_url: localhost:27017
    database_name: institutional_data
    staff_collection_name: staff
    num_jobs: 1
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/

