from kahi.KahiBase import KahiBase
from pymongo import ASCENDING, MongoClient, TEXT
from joblib import Parallel, delayed
from kahi_openalex_works.process_one import process_one
from mohan.Similarity import Similarity
from threading import BoundedSemaphore


class Kahi_openalex_works(KahiBase):

    config = {}

    def __init__(self, config):
        """
        Constructor for the Kahi_openalex_works class.

        Several indices are created in the database to speed up the process.
        We also handle the error to check db and collection existence.

        Parameters:
        -----------
        config : dict
            The configuration dictionary. It should contain the following keys:
            - Kahi_openalex_works: A dictionary with the following keys:
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

        self.collection.create_index("year_published")
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])
        # DOI and other identifier lookups only constrain external_ids.id, so
        # the compound OpenAlex identity index below cannot serve them.
        self.collection.create_index("external_ids.id")
        # Used as a fallback when an OpenAlex author identifier is not present.
        self.db["person"].create_index("full_name")
        # A partial multikey index filters whole documents, not individual
        # external_ids elements. The former unique index therefore enforced
        # uniqueness on MAG, DOI and other identifiers as well as OpenAlex.
        legacy_index = "external_ids_source_id_1_unique_openalex_partial"
        if legacy_index in self.collection.index_information():
            self.collection.drop_index(legacy_index)
        self.collection.create_index(
            [("external_ids.source", 1), ("external_ids.id", 1)],
            name="external_ids_source_id_1",
        )

        self.openalex_client = MongoClient(
            config["openalex_works"]["database_url"])
        if config["openalex_works"]["database_name"] not in list(self.openalex_client.list_database_names()):
            raise RuntimeError(
                f'''Database {config["openalex_works"]["database_name"]} was not found''')
        self.openalex_db = self.openalex_client[config["openalex_works"]
                                                ["database_name"]]
        if config["openalex_works"]["collection_name"] not in self.openalex_db.list_collection_names():
            raise RuntimeError(
                f'''Collection {config["openalex_works"]["collection_name"]} was not found on database {config["openalex_works"]["database_name"]}''')
        self.openalex_collection = self.openalex_db[config["openalex_works"]
                                                    ["collection_name"]]
        if "es_index" in config["openalex_works"].keys() and "es_url" in config["openalex_works"].keys() and "es_user" in config["openalex_works"].keys() and "es_password" in config["openalex_works"].keys():
            es_index = config["openalex_works"]["es_index"]
            es_url = config["openalex_works"]["es_url"]
            if config["openalex_works"]["es_user"] and config["openalex_works"]["es_password"]:
                es_auth = (config["openalex_works"]["es_user"],
                           config["openalex_works"]["es_password"])
            else:
                es_auth = None
            self.es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth, es_req_timeout=300)
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.task = config["openalex_works"]["task"]
        self.n_jobs = config["openalex_works"]["num_jobs"] if "num_jobs" in config["openalex_works"].keys(
        ) else 1
        self.verbose = config["openalex_works"]["verbose"] if "verbose" in config["openalex_works"].keys(
        ) else 0

        self.backend = "threading" if "backend" not in config[
            "openalex_works"].keys() else config["openalex_works"]["backend"]

        self.es_semaphore = None
        if self.backend == "threading" and self.es_handler is not None:
            es_max_concurrency = config["openalex_works"].get(
                "es_max_concurrency", 10)
            if es_max_concurrency < 1:
                raise ValueError(
                    "openalex_works.es_max_concurrency must be greater than zero")
            self.es_semaphore = BoundedSemaphore(es_max_concurrency)

    def process_openalex(self):
        # selects papers with doi according to task variable
        if self.task == "doi":
            query = {
                "doi": {"$ne": None},
                "title": {"$type": "string", "$regex": r"\S"},
                "type": {"$ne": "grant"},
            }
            count = self.openalex_collection.count_documents(query)
            print(f"INFO: proccesing {count} works with DOI")
        else:
            query = {
                "doi": {"$eq": None},
                "title": {"$type": "string", "$regex": r"\S"},
                "type": {"$ne": "grant"},
            }
            count = self.openalex_collection.count_documents(query)
            print(f"INFO: proccesing {count} works without DOI")

        page_size = self.config["openalex_works"].get(
            "page_size", max(self.n_jobs * 2, 100))
        if page_size < 1:
            raise ValueError("openalex_works.page_size must be greater than zero")

        def paginated_papers():
            last_id = None
            loaded = 0
            while True:
                page_query = dict(query)
                if last_id is not None:
                    page_query["_id"] = {"$gt": last_id}
                page = list(
                    self.openalex_collection.find(page_query)
                    .sort("_id", ASCENDING)
                    .hint("_id_")
                    .limit(page_size)
                )
                if not page:
                    return

                last_id = page[-1]["_id"]
                loaded += len(page)
                if self.verbose > 0 and (
                        loaded % (page_size * 10) == 0 or loaded == count):
                    print(f"INFO: loaded {loaded}/{count} works")
                yield from page

        with Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend=self.backend,
                return_as="generator_unordered",
                batch_size=10) as parallel:
            results = parallel(
                delayed(process_one)(
                    paper,
                    self.config,
                    self.empty_work(),
                    self.client if self.backend == "threading" else None,
                    self.es_handler if self.backend == "threading" else None,
                    self.backend,
                    verbose=self.verbose,
                    es_semaphore=self.es_semaphore,
                )
                for paper in paginated_papers()
            )
            # Consume results incrementally. process_one writes directly to
            # MongoDB and returns None, so retaining a result list is wasteful.
            for _ in results:
                pass

    def run(self):
        self.process_openalex()
        return 0
