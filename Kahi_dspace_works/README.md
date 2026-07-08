<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi dspace_works plugin 
This is the plugin for the kahi workflow system that allows to process the works from the Dspace repository.


# Description
The dataset have to be downloaded from the Dspace repository using https://github.com/colav/oxomoc and stored in mongodb, 
then the works are processed to extract the metadata.

# Installation

## Dependencies
- Kahi_impactu_utils
- MongoDB
- oxomoc dataset already downloaded

## Package
The package is available in the PyPi repository, so you can install it using pip:
`pip install kahi_dspace_works`


# Usage
Parameters for kahi_run in the workflow should be similar to.

```
  dspace_works:
    database_url: localhost:27017
    database_name: oxomoc_colombia
    batch_size: 500
    num_jobs: 12
    # Optional: collections are discovered automatically from *_records.
    repository_affiliations:
      dspace_udea_records: https://ror.org/03bp5hc83
      dspace_univalle_records: https://ror.org/02xw8cw23
```

`repositories` remains supported for backwards compatibility. Use `collections`
or `exclude_collections` to restrict automatic discovery.


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/


