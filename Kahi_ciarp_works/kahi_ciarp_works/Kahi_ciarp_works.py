from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from pandas import read_excel, isna
from kahi_ciarp_works.process_one import process_one
from kahi_impactu_utils.Utils import doi_processor
from mohan.Similarity import Similarity


class Kahi_ciarp_works(KahiBase):

    config = {}

    def __init__(self, config):
        """
        Constructor for the Kahi_ciarp_works class.

        Several indices are created in the MongoDB collection to speed up the queries.

        Parameters
        ----------
        config : dict
            The configuration dictionary. It should contain the following keys:
            - ciarp_works(/doi or empty): a dictionary with the following keys:
                - task: the task to be performed. It can be "doi" or "all"
                - num_jobs: the number of jobs to be used in parallel processing
                - verbose: the verbosity level
                - databases: a list of dictionaries with the following keys:
                    - database_url: the URL for the MongoDB database
                    - database_name: the name of the database
                    - collection_name: the name of the collection
                    - es_index: the name of the Elasticsearch index
                    - es_url: the URL for the Elasticsearch server
                    - es_user: the username for the Elasticsearch server
                    - es_password: the password for the Elasticsearch server
        """
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("year_published")
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])

        if "es_index" in config["ciarp_works"].keys() and "es_url" in config["ciarp_works"].keys() and "es_user" in config["ciarp_works"].keys() and "es_password" in config["ciarp_works"].keys():  # noqa: E501
            es_index = config["ciarp_works"]["es_index"]
            es_url = config["ciarp_works"]["es_url"]
            if config["ciarp_works"]["es_user"] and config["ciarp_works"]["es_password"]:
                es_auth = (config["ciarp_works"]["es_user"],
                           config["ciarp_works"]["es_password"])
            else:
                es_auth = None
            self.es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth)
            print("INFO: ES handler created successfully")
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.ranking = read_excel(config["ciarp_works"]["file_path"], dtype={
                                  "cedula": str, "DOI": str})
        # adding index column for external_ids
        index = []
        for i, rec in enumerate(self.ranking["cedula"]):
            # row index - cedula - timestamp
            index.append(f"{str(i)}-{rec}-{str(time())}")
        self.ranking['index'] = index
        self.ranking = self.ranking.to_dict(orient="records")

        self.task = config["ciarp_works"]["task"]
        self.n_jobs = config["ciarp_works"]["num_jobs"] if "num_jobs" in config["ciarp_works"].keys(
        ) else 1
        self.verbose = config["ciarp_works"]["verbose"] if "verbose" in config["ciarp_works"].keys(
        ) else 0

        self.udea_reg = self.db["affiliations"].find_one(
            {"names.name": "University of Antioquia"})
        if not self.udea_reg:
            self.udea_reg = self.db["affiliations"].find_one(
                {"names.name": "Universidad de Antioquia"})
        if not self.udea_reg:
            print(
                "University of Antioquia not found in database. Creating it with minimal information...")
            udea_reg = self.empty_affiliation()
            udea_reg["updated"].append(
                {"time": int(time()), "source": "manual"})
            udea_reg["names"] = [
                {"name": 'Universidad de Antioquia',
                    "lang": 'es', "source": "ciarp"}
            ]
            udea_reg["abbreviations"] = ['UdeA']
            udea_reg["year_established"] = 1803
            udea_reg["addresses"] = [
                {
                    "lat": 6.267417,
                    "lng": -75.568389,
                    "postcode": '',
                    "state": "Antioquia",
                    "city": 'Medellín',
                    "country": 'Colombia',
                    "country_code": 'CO'
                }
            ]
            udea_reg["external_ids"] = [
                {"source": 'isni', "id": '0000 0000 8882 5269'},
                {"source": 'fundref', "id": '501100005278'},
                {"source": 'orgref', "id": '2696975'},
                {"source": 'wikidata', "id": 'Q1258413'},
                {"source": 'ror', "id": 'https://ror.org/03bp5hc83'},
                {"source": 'minciencias', "id": '007300000887'},
                {"source": 'nit', "id": '890980040-8'}
            ]
            self.db["affiliations"].insert_one(udea_reg)
            self.udea_reg = self.db["affiliations"].find_one(
                {"names.name": "Universidad de Antioquia"})

    def process_ciarp(self):
        """
        Method to process the CIARP database.
        Checks if the task is "doi" or not and processes the documents accordingly.

        Parameters:
        -----------
        db : Database
            The MongoDB database to be used. (colav database genrated by the kahi)
        collection : Collection
            The MongoDB collection to be used. (works collection genrated by the kahi)
        config : dict
            A dictionary with the configuration for the scienti database. It should have the following keys:
            - database_url: the URL for the MongoDB database
            - database_name: the name of the database
            - collection_name: the name of the collection
            - es_index: the name of the Elasticsearch index
            - es_url: the URL for the Elasticsearch server
            - es_user: the username for the Elasticsearch server
            - es_password: the password for the Elasticsearch server
        """

        # selects papers with doi according to task variable
        if self.task == "doi":
            papers = []
            for par in self.ranking:
                if not isna(par["DOI"]):
                    if doi_processor(par["DOI"]):
                        papers.append(par)
            self.ranking = papers
        else:
            # TODO: implement similarity task and a default task that runs all
            papers = []
            for par in self.ranking:
                if isna(par["DOI"]):
                    papers.append(par)
                elif not doi_processor(par["DOI"]):
                    papers.append(par)
            self.ranking = papers

        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["works"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(process_one)(
                    paper,
                    db,
                    collection,
                    self.udea_reg,
                    self.empty_work(),
                    True if self.task != "doi" else False,
                    self.es_handler,
                    verbose=self.verbose
                ) for paper in self.ranking
            )

    def run(self):
        """
        Method to run the process_ciarp method.
        Entrypoint for the Kahi_ciarp_works class to be excute by kahi.
        """
        self.process_ciarp()
        return 0
