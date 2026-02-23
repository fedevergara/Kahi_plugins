<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi publindex sources plugin
Kahi will use this plugin to insert or update journal source records from a Publindex MongoDB collection.

# Description
Plugin that reads records from a MongoDB collection (for example `publindex_data`) and upserts them in CoLav's `sources` collection format.

The implementation separates:
- insertion of new source records
- update of existing source records

# Installation
You can install from source:
```shell
pip3 install .
```

Or from package:
```shell
pip3 install kahi_publindex_sources
```

## Dependencies
Software dependencies are installed with the package.
You need:
- a target Kahi MongoDB database
- a source MongoDB database/collection with Publindex records

# Usage
To use this plugin you must have Kahi installed and define a workflow like:

```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  publindex_sources:
    database_url: localhost:27017
    database_name: publindex
    collection_name: publindex_data
    verbose: 5
```

# License
BSD-3-Clause License

# Links
http://colav.udea.edu.co/
