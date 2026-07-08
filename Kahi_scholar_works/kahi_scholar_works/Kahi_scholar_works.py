from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from joblib import Parallel, delayed

from mohan.Similarity import Similarity
from kahi_scholar_works.process_one import process_one
from threading import BoundedSemaphore


SCHOLAR_SOURCE_PROJECTION = {
    "doi": 1,
    "title": 1,
    "year": 1,
    "cid": 1,
    "abstract": 1,
    "volume": 1,
    "issue": 1,
    "pages": 1,
    "bibtex": 1,
    "cites": 1,
    "cites_link": 1,
    "pdf": 1,
    "journal": 1,
    "author": 1,
    "profiles": 1,
}


def configure_work_indexes(collection, task):
    collection.create_index("external_ids.id")
    collection.create_index("year_published")
    collection.create_index("authors.affiliations.id")
    collection.create_index("authors.id")

    text_index = "titles.title_text"
    if task == "doi":
        if text_index in collection.index_information():
            collection.drop_index(text_index)
    else:
        collection.create_index([("titles.title", TEXT)])


class Kahi_scholar_works(KahiBase):
    config = {}

    def __init__(self, config):
        """
        Constructor for the Kahi_scholar_works class.

        Several indices are created in the database to speed up the process.
        We also handle the error to check db and collection existence.

        Parameters:
        -----------
        config : dict
            The configuration dictionary. It should contain the following keys:
            - scholar_works: A dictionary with the following keys:
                - database_url: The url of the scholar works database.
                - database_name: The name of the scholar works database.
                - collection_name: The name of the collection in the scholar works database.
                - task: The task to be performed. It can be "doi" or empty for similarity.
                - num_jobs: The number of jobs to be used in parallel processing.
                - verbose: The verbosity level.
                - es_index: The index to be used in elasticsearch.
                - es_url: The url of the elasticsearch server.
                - es_user: The user for the elasticsearch server.
                - es_password: The password for the elasticsearch server.
        """
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]
        self.task = config["scholar_works"]["task"]

        configure_work_indexes(self.collection, self.task)

        self.scholar_client = MongoClient(
            config["scholar_works"]["database_url"])
        if (
            config["scholar_works"]["database_name"]
            not in self.scholar_client.list_database_names()
        ):
            raise ValueError(
                f"Database {config['scholar_works']['database_name']} found not in "
                f"{config['scholar_works']['database_url']}")
        self.scholar_db = self.scholar_client[config["scholar_works"]
                                              ["database_name"]]
        if (
            config["scholar_works"]["collection_name"]
            not in self.scholar_db.list_collection_names()
        ):
            raise ValueError(
                f"Collection {config['scholar_works']['database_name']}."
                f"{config['scholar_works']['collection_name']} found not in "
                f"{config['scholar_works']['database_url']}")
        self.scholar_collection = self.scholar_db[
            config["scholar_works"]["collection_name"]
        ]

        if (
            "es_index" in config["scholar_works"].keys()
            and "es_url" in config["scholar_works"].keys()
            and "es_user" in config["scholar_works"].keys()
            and "es_password" in config["scholar_works"].keys()
        ):
            es_index = config["scholar_works"]["es_index"]
            es_url = config["scholar_works"]["es_url"]
            if (
                config["scholar_works"]["es_user"]
                and config["scholar_works"]["es_password"]
            ):
                es_auth = (
                    config["scholar_works"]["es_user"],
                    config["scholar_works"]["es_password"],
                )
            else:
                es_auth = None
            self.es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth)
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.n_jobs = (
            config["scholar_works"]["num_jobs"]
            if "num_jobs" in config["scholar_works"].keys()
            else 1
        )
        self.verbose = (
            config["scholar_works"]["verbose"]
            if "verbose" in config["scholar_works"].keys()
            else 0
        )
        self.es_semaphore = None
        if self.es_handler is not None and self.task != "doi":
            es_max_concurrency = config["scholar_works"].get(
                "es_max_concurrency", 8)
            if es_max_concurrency < 1:
                raise ValueError(
                    "scholar_works.es_max_concurrency must be greater than zero")
            self.es_semaphore = BoundedSemaphore(es_max_concurrency)

    def process_scholar(self):
        """
        Method to process the scholar works, and add them to the main database.

        We use the process_one function to process each paper in parallel.

        There are to possible tasks:
        - "doi": process papers with doi
        - empty to process papers without doi using similarity with Mohan package (https://github.com/colav/Mohan)
        The task is defined in the configuration file. (Read more about tasks in Mohan package)
        """
        # selects papers with doi according to task variable
        if self.task == "doi":
            paper_cursor = self.scholar_collection.find(
                {"$and": [{"doi": {"$ne": ""}}, {"doi": {"$ne": None}}]},
                SCHOLAR_SOURCE_PROJECTION,
            ).batch_size(1000)
        else:
            paper_cursor = self.scholar_collection.find(
                {"$or": [{"doi": {"$eq": ""}}, {"doi": {"$eq": None}}]},
                SCHOLAR_SOURCE_PROJECTION,
            ).batch_size(1000)

        client = MongoClient(self.mongodb_url)
        db = client[self.config["database_name"]]
        collection = db["works"]
        try:
            results = Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading",
                pre_dispatch="2*n_jobs",
                return_as="generator_unordered",
            )(
                delayed(process_one)(
                    paper,
                    db,
                    collection,
                    self.empty_work(),
                    False if self.task == "doi" else True,
                    es_handler=self.es_handler,
                    verbose=self.verbose,
                    es_semaphore=getattr(self, "es_semaphore", None),
                )
                for paper in paper_cursor
            )
            for _ in results:
                pass
        finally:
            paper_cursor.close()
            client.close()

    def run(self):
        """
        Method start the execution of the workflow in parallel.
        """
        self.process_scholar()
        return 0
