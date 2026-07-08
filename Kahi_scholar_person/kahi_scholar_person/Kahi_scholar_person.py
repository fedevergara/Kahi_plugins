from kahi_scholar_person.parser import parse_scholar
from kahi_scholar_person.process_one import external_scholar_id, merge_author_entries, process_one
from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from joblib import Parallel, delayed


class Kahi_scholar_person(KahiBase):

    config = {}

    def __init__(self, config):
        """
        Constructor for the Kahi_scholar_person class.

        Several indices are created in the database to speed up the process.
        We also handle the error to check db and collection existence.

        Parameters:
        -----------
        config : dict
            The configuration dictionary. It should contain the following keys:
            - scholar_person: A dictionary with the following keys:
                - database_url: The url of the scholar works database.
                - database_name: The name of the scholar works database.
                - collection_name: The name of the collection in the scholar works database.
                - num_jobs: The number of jobs to be used in parallel processing.
                - verbose: The verbosity level.
        """
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.scholar_client = MongoClient(
            config["scholar_person"]["database_url"])
        if config["scholar_person"]["database_name"] not in self.scholar_client.list_database_names():
            raise ValueError(
                f"Database {config['scholar_person']['database_name']} found not in "
                f"{config['scholar_person']['database_url']}")
        self.scholar_db = self.scholar_client[config["scholar_person"]
                                              ["database_name"]]
        if config["scholar_person"]["collection_name"] not in self.scholar_db.list_collection_names():
            raise ValueError(
                f"Collection {config['scholar_person']['database_name']}."
                f"{config['scholar_person']['collection_name']} found not in "
                f"{config['scholar_person']['database_url']}")
        self.scholar_collection = self.scholar_db[config["scholar_person"]
                                                  ["collection_name"]]

        self.n_jobs = config["scholar_person"]["num_jobs"] if "num_jobs" in config["scholar_person"].keys(
        ) else 1
        self.verbose = config["scholar_person"]["verbose"] if "verbose" in config["scholar_person"].keys(
        ) else 0

    def process_scholar(self):
        """
        Method to process the scholar works and add the authors to the main database.

        We use the process_one function to process each paper in parallel.
        """
        paper_cursor = self.scholar_collection.find(
            {},
            {"author": 1, "profiles": 1, "cid": 1, "doi": 1},
        )

        authors_by_id = {}
        empty_person = self.empty_person()
        for paper in paper_cursor:
            for author in parse_scholar(
                    paper, empty_person, verbose=self.verbose):
                scholar_id = external_scholar_id(author)
                if not scholar_id:
                    continue
                if scholar_id in authors_by_id:
                    merge_author_entries(authors_by_id[scholar_id], author)
                else:
                    authors_by_id[scholar_id] = author

        if self.verbose:
            print(
                f"INFO: Processing {len(authors_by_id)} unique Scholar profiles")

        client = MongoClient(self.mongodb_url)
        db = client[self.config["database_name"]]
        collection = db["person"]
        Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading")(
            delayed(process_one)(
                author,
                collection,
                verbose=self.verbose
            ) for author in authors_by_id.values()
        )
        client.close()

    def run(self):
        """
        Method start the execution of the workflow in parallel.
        """
        self.process_scholar()
        return 0
