from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from joblib import Parallel, delayed
from kahi_scienti_works.process_one import process_one
from mohan.Similarity import Similarity
from kahi_impactu_utils.Utils import doi_processor
import re
from threading import BoundedSemaphore


class Kahi_scienti_works(KahiBase):

    config = {}

    def __init__(self, config):
        """
        Constructor for the Kahi_scienti_works class.

        Several indices are created in the MongoDB collection to speed up the queries.
        We also handle the error to check db and collection existence.

        Parameters
        ----------
        config : dict
            The configuration dictionary. It should contain the following keys:
            - scienti_works: a dictionary with the following keys:
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

        self.collection.create_index("year_published")
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])
        self.collection.create_index("external_ids.id")
        self.db["person"].create_index("external_ids.id")
        self.db["person"].create_index("external_ids.id.COD_RH")
        self.db["person"].create_index("full_name")
        self.db["person"].create_index("affiliations.id")
        self.db["affiliations"].create_index("external_ids.id")
        self.db["affiliations"].create_index("names.name")
        self.db["affiliations"].create_index("relations.id")
        self.db["sources"].create_index("external_ids.id")
        if "es_index" in config["scienti_works"].keys() and "es_url" in config["scienti_works"].keys() and "es_user" in config["scienti_works"].keys() and "es_password" in config["scienti_works"].keys():  # noqa: E501
            es_index = config["scienti_works"]["es_index"]
            es_url = config["scienti_works"]["es_url"]
            if config["scienti_works"]["es_user"] and config["scienti_works"]["es_password"]:
                es_auth = (config["scienti_works"]["es_user"],
                           config["scienti_works"]["es_password"])
            else:
                es_auth = None
            self.es_handler = Similarity(
                es_index, es_uri=es_url, es_auth=es_auth)
            print("INFO: ES handler created successfully")
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.task = config["scienti_works"]["task"]

        self.n_jobs = config["scienti_works"]["num_jobs"] if "num_jobs" in config["scienti_works"].keys(
        ) else 1
        self.verbose = config["scienti_works"]["verbose"] if "verbose" in config["scienti_works"].keys(
        ) else 0
        self.es_semaphore = None
        if self.es_handler is not None and self.task != "doi":
            es_max_concurrency = config["scienti_works"].get(
                "es_max_concurrency", 8)
            if es_max_concurrency < 1:
                raise ValueError(
                    "scienti_works.es_max_concurrency must be greater than zero")
            self.es_semaphore = BoundedSemaphore(es_max_concurrency)

        # checking if the databases and collections are available
        self.check_databases_and_collections()

    def check_databases_and_collections(self):
        """
        Method to check if the databases and collections are available.
        """
        for db_info in self.config["scienti_works"]["databases"]:
            client = MongoClient(db_info["database_url"])
            if db_info['database_name'] not in client.list_database_names():
                raise Exception("Database {} not found".format(
                    db_info['database_name']))
            if db_info['collection_name'] not in client[db_info['database_name']].list_collection_names():
                raise Exception("Collection {}.{} not found".format(db_info['database_name'],
                                                                    db_info['collection_name']))
            client.close()

    def process_doi_group(self, group, db, collection, collection_scienti,
                          empty_work, es_handler, similarity, verbose=0,
                          es_semaphore=None):
        """
        This method processes a group of documents with the same DOI.
        This allows to process the documents in parallel without having to worry about the DOI being processed more than once.

        Parameters
        ----------
        group : dict
            A dictionary with the group of documents to be processed. It should have the following keys:
            - _id: the DOI
            - ids: a list with the IDs of the documents
        db : Database
            The MongoDB database to be used. (colav database genrated by the kahi)
        collection : Collection
            The MongoDB collection to be used. (works collection genrated by the kahi)
        collection_scienti : Collection
            The MongoDB collection with the scienti data.
        empty_work : dict
            A template for the work entry. Structure is defined in the schema.
        es_handler : Similarity
            The Elasticsearch handler to be used for similarity checks. Take a look in Mohan package.
        similarity : bool
            A flag to indicate if similarity checks should be performed if doi is not available.
        verbose : int
            The verbosity level. Default is 0.
        """
        for i in group["ids"]:
            reg = collection_scienti.find_one({"_id": i})
            process_one(reg, db, collection, empty_work,
                        es_handler, similarity, verbose,
                        es_semaphore=es_semaphore)

    def process_scienti(self, db, collection, config):
        """
        Method to process the scienti database.
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
        client = MongoClient(config["database_url"])
        scienti = client[config["database_name"]][config["collection_name"]]
        types_level0 = ['111', '112', '113', '114',  # articulos
                        '121', '122',  # Trabajos en eventos
                        '131', '132', '133', '134', '135', '136', '137', '138', '139', '140',  # libros
                        '141', '142', '143', '144', '145',  # Otro artículo publicado
                        '1A1', '1A2', '1A9',  # 1A: Traducciones
                        '1B1', '1B2', '1B3', '1B9',  # 1B: Partituras musicales
                        '1D',  # 1D: Documento de trabajo (Working Paper)
                        '1K',  # 1K: Nota científica
                        '1Z2', '1Z3', '1Z4', '1Z9',  # 1Z: Otra producción bibliográfica
                        '61', '62', '63', '64', '65', '66'  # Trabajos dirigidos/Tutorías
                        ]

        if self.task == "doi":
            pipeline = [
                {"$match": {"product_type.COD_TIPO_PRODUCTO": {"$in": types_level0}}},
                {"$match": {"TXT_DOI": {"$ne": None}}},
                {"$match": {"TXT_NME_PROD_FILTRO": {"$ne": None}}},
                {"$match": {"TXT_NME_PROD": {"$ne": " "}}},
                {"$project": {"doi": {"$trim": {"input": "$TXT_DOI"}}}},
                {"$project": {"doi": {"$toLower": "$doi"}}},
                {"$group": {"_id": "$doi", "ids": {"$push": "$_id"}}}
            ]
            paper_groups = list(scienti.aggregate(
                pipeline, allowDiskUse=True))
            results = Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading",
                return_as="generator_unordered")(
                delayed(self.process_doi_group)(
                    doi_group,
                    db,
                    collection,
                    scienti,
                    self.empty_work(),
                    self.es_handler,
                    similarity=False,
                    verbose=self.verbose,
                    es_semaphore=self.es_semaphore,
                ) for doi_group in paper_groups
            )
            for _ in results:
                pass
        else:
            # correr doi processor para TXT_DOI y TXT_WEBSITE*
            # saco los dois malos y luego hago un find $in sobre esos COD_RH /COD_PRODUCTO y paso el cursor a parallel

            pipeline = [
                {"$match": {"product_type.COD_TIPO_PRODUCTO": {"$in": types_level0}}},
                {"$match": {"TXT_DOI": {"$ne": None}}},
                {"$match": {"TXT_NME_PROD_FILTRO": {"$ne": None}}},
                {"$match": {"TXT_NME_PROD": {"$ne": " "}}},
                {"$project": {"doi": {"$trim": {"input": "$TXT_DOI"}},
                              "web_doi": {"$trim": {"input": "$TXT_WEB_PRODUCTO"}}}},
                {"$project": {"doi": {"$toLower": "$doi"},
                              "web_doi": {"$toLower": "$web_doi"}}},
                {"$group": {"_id": {"doi": "$doi", "web_doi": "$web_doi"},
                            "ids": {"$push": "$_id"}}}
            ]
            paper_group_doi_cursor = scienti.aggregate(
                pipeline)  # update for doi and not doi

            works_nodoi = []
            count = 0
            for scienti_reg in paper_group_doi_cursor:
                count += 1
                doi = None
                if scienti_reg["_id"]["doi"]:
                    doi = doi_processor(scienti_reg["_id"]["doi"])
                if not doi:
                    if "web_doi" in scienti_reg["_id"].keys() and scienti_reg["_id"]["web_doi"] and "10." in scienti_reg["_id"]["web_doi"]:
                        doi = doi_processor(scienti_reg["_id"]["web_doi"])
                        if doi:
                            extracted_doi = re.compile(
                                r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', re.IGNORECASE).match(doi)
                            if extracted_doi:
                                doi = extracted_doi.group(0)
                                for keyword in ['abstract', 'homepage', 'tpmd200765', 'event_abstract']:
                                    doi = doi.split(
                                        f'/{keyword}')[0] if keyword in doi else doi
                if not doi:
                    works_nodoi.extend(scienti_reg["ids"])
            print(f"INFO: processing {len(works_nodoi)} records with bad dois")
            paper_cursor = scienti.find(
                {"_id": {"$in": works_nodoi}, "TXT_NME_PROD_FILTRO": {"$ne": None}, "TXT_NME_PROD": {"$ne": ' '}, "product_type.COD_TIPO_PRODUCTO": {"$in": types_level0}})
            results = Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading",
                return_as="generator_unordered")(
                delayed(process_one)(
                    work,
                    db,
                    collection,
                    self.empty_work(),
                    self.es_handler,
                    similarity=True,
                    verbose=self.verbose,
                    es_semaphore=self.es_semaphore,
                ) for work in paper_cursor
            )
            for _ in results:
                pass

            paper_cursor = scienti.find(
                {"$or": [{"doi": {"$eq": ""}}, {"doi": {"$eq": None}}], "TXT_NME_PROD_FILTRO": {"$ne": None}, "TXT_NME_PROD": {"$ne": ' '}, "product_type.COD_TIPO_PRODUCTO": {"$in": types_level0}})
            paper_cursor_count = scienti.count_documents(
                {"$or": [{"doi": {"$eq": ""}}, {"doi": {"$eq": None}}], "TXT_NME_PROD_FILTRO": {"$ne": None}, "TXT_NME_PROD": {"$ne": ' '}, "product_type.COD_TIPO_PRODUCTO": {"$in": types_level0}})
            print(f"INFO: processing {paper_cursor_count} records without doi")

            results = Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading",
                return_as="generator_unordered")(
                delayed(process_one)(
                    work,
                    db,
                    collection,
                    self.empty_work(),
                    self.es_handler,
                    similarity=True,
                    verbose=self.verbose,
                    es_semaphore=self.es_semaphore,
                ) for work in paper_cursor
            )
            for _ in results:
                pass
        client.close()

    def run(self):
        for config in self.config["scienti_works"]["databases"]:
            if self.verbose > 0:
                print("Processing {}.{} database".format(
                    config["database_name"], config["collection_name"]))
            if self.verbose > 4:
                print("Updating already inserted entries")
            self.process_scienti(self.db, self.collection, config)
        return 0
