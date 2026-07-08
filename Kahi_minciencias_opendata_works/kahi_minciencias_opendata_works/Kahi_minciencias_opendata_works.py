from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from pymongo.errors import ConnectionFailure
from joblib import Parallel, delayed
from kahi_minciencias_opendata_works.process_one import process_one
from kahi_minciencias_opendata_works.related_works import process_person_related_works
from mohan.Similarity import Similarity
from threading import BoundedSemaphore


class Kahi_minciencias_opendata_works(KahiBase):

    config = {}

    def __init__(self, config):
        """
        Constructor for the Kahi_minciencias_opendata_works class.

        Several indices are created in the MongoDB collection to speed up the queries.
        We also handle the error to check db and collection existence.

        Parameters
        ----------
        config : dict
            The configuration dictionary. It should contain the following keys:
            - minciencias_opendata_works: a dictionary with the following keys:
                - task: the task to be performed. It can be "doi" or "all"
                - num_jobs: the number of jobs to be used in parallel processing
                - verbose: the verbosity level
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

        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])
        self.collection.create_index("external_ids.id")

        if "es_index" in config["minciencias_opendata_works"].keys() and "es_url" in config["minciencias_opendata_works"].keys() and "es_user" in config["minciencias_opendata_works"].keys() and "es_password" in config["minciencias_opendata_works"].keys():  # noqa: E501
            es_index = config["minciencias_opendata_works"]["es_index"]
            es_url = config["minciencias_opendata_works"]["es_url"]
            if config["minciencias_opendata_works"]["es_user"] and config["minciencias_opendata_works"]["es_password"]:
                es_auth = (config["minciencias_opendata_works"]["es_user"],
                           config["minciencias_opendata_works"]["es_password"])
            else:
                es_auth = None
            self.es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth)
            print("INFO: ES handler created successfully")
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.task = config["minciencias_opendata_works"]["task"] if "task" in config["minciencias_opendata_works"].keys(
        ) else None
        self.insert_all = config["minciencias_opendata_works"]["insert_all"] if "insert_all" in config["minciencias_opendata_works"].keys(
        ) else False
        self.thresholds = config["minciencias_opendata_works"]["thresholds"] if "thresholds" in config["minciencias_opendata_works"].keys(
        ) else None
        self.include_person_related_works = config["minciencias_opendata_works"].get(
            "person_related_works", False
        )
        self.person_collection_name = config["minciencias_opendata_works"].get(
            "person_collection", "person"
        )
        self.n_jobs = config["minciencias_opendata_works"]["num_jobs"] if "num_jobs" in config["minciencias_opendata_works"].keys(
        ) else 1
        self.verbose = config["minciencias_opendata_works"]["verbose"] if "verbose" in config["minciencias_opendata_works"].keys(
        ) else 0
        self.es_semaphore = None
        if self.es_handler is not None:
            es_max_concurrency = config["minciencias_opendata_works"].get(
                "es_max_concurrency", 8
            )
            if es_max_concurrency < 1:
                raise ValueError(
                    "minciencias_opendata_works.es_max_concurrency must be greater than zero"
                )
            self.es_semaphore = BoundedSemaphore(es_max_concurrency)

        # checking if the databases and collections are available
        self.check_databases_and_collections()

    def check_databases_and_collections(self):
        """
        Method to check if the databases and collections are available.
        """
        try:
            with MongoClient(self.config["minciencias_opendata_works"]["database_url"]) as client:
                db_name = self.config["minciencias_opendata_works"]["database_name"]
                collection_name = self.config["minciencias_opendata_works"]["collection_name"]

                # Check if database exists
                if db_name not in client.list_database_names():
                    raise ValueError(f"Database {db_name} not found")

                db = client[db_name]

                # Check if collection exists
                if collection_name not in db.list_collection_names():
                    raise ValueError(
                        f"Collection {collection_name} in database {db_name} not found")

        except ConnectionFailure:
            raise ConnectionFailure("Failed to connect to MongoDB server.")

    def process_opendata(self):
        """
        Method to process the minciencias_opendata database.
        Checks if the task is "doi" or "all" and processes the records accordingly.
        """
        client = MongoClient(
            self.config["minciencias_opendata_works"]["database_url"])
        db = client[self.config["minciencias_opendata_works"]["database_name"]]
        opendata = db[self.config["minciencias_opendata_works"]
                      ["collection_name"]]
        print("INFO: Creating indices")
        opendata.create_index("id_producto_pd")
        opendata.create_index("nme_tipologia_pd")
        exclude = ["Evento científico", "Eventos artísticos, de arquitectura o de diseño con componentes de apropiación", "Eventos artísticos",
                   "Patente de invención", "Patente modelo de utilidad",
                   "Proyecto ID+I con Formación", "Proyecto de Investigacion y Desarrollo", "Proyecto de Investigación y Creación",
                   "Proyecto de extensión", "Proyecto de extensión y responsabilidad social en CTI"]

        pipeline = [
            {'$match': {"nme_producto_pd": {"$type": "string", "$ne": ""}}},
            {'$match': {'nme_tipologia_pd': {'$nin': exclude}}},
            {'$sort': {'ano_convo': -1}},
            {'$group': {
                '_id': '$id_producto_pd',
                'originalDoc': {'$first': '$$ROOT'},
                'rankingHistory': {'$push': {
                    'ano_convo': '$ano_convo',
                    'id_tipo_pd_med': '$id_tipo_pd_med',
                    'nme_tipo_medicion_pd': '$nme_tipo_medicion_pd',
                    'nme_categoria_pd': '$nme_categoria_pd'
                }}
            }},
            {'$set': {'originalDoc._ranking_history': '$rankingHistory'}},
            {'$replaceRoot': {'newRoot': '$originalDoc'}}
        ]
        paper_cursor = opendata.aggregate(
            pipeline, allowDiskUse=True, batchSize=1000
        )
        print(f"INFO: Processing productions other than categories {exclude}")
        try:
            results = Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading",
                pre_dispatch="2*n_jobs",
                return_as="generator_unordered",
            )(
                delayed(process_one)(
                    work,
                    self.db,
                    self.collection,
                    self.empty_work(),
                    self.es_handler,
                    insert_all=self.insert_all,
                    thresholds=self.thresholds,
                    verbose=self.verbose,
                    es_semaphore=self.es_semaphore,
                ) for work in paper_cursor
            )
            for _ in results:
                pass
        finally:
            paper_cursor.close()
        if self.include_person_related_works:
            person_collection = self.db[self.person_collection_name]
            person_collection.create_index("related_works.source")
            person_collection.create_index("related_works.id")
            counters = process_person_related_works(
                person_collection=person_collection,
                works_collection=self.collection,
                empty_work_factory=self.empty_work,
                es_handler=self.es_handler,
                insert_all=self.insert_all,
                thresholds=self.thresholds,
                es_semaphore=self.es_semaphore,
            )
            if self.verbose > 0:
                print(f"INFO: person.related_works results: {counters}")
        client.close()

    def run(self):
        self.process_opendata()
        return 0
