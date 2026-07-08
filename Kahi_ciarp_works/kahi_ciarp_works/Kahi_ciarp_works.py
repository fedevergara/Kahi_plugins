from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from pandas import DataFrame, read_excel
from kahi_ciarp_works.process_one import process_one
from kahi_impactu_utils.Utils import doi_processor
from kahi_impactu_utils.Mapping import ciarp_mapping
from mohan.Similarity import Similarity
from pathlib import Path
from threading import BoundedSemaphore


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

        if all(
            key in config["ciarp_works"] for key in [
                "es_index",
                "es_url",
                "es_user",
                "es_password"]):
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

        self.ciarp_databases = config["ciarp_works"].get("databases", [])

        self.required_columns = [
            "código_unidad_académica",
            "código_subunidad_académica",
            "tipo_documento",
            "identificación",
            "año",
            "título",
            "idioma",
            "revista",
            "editorial",
            "doi",
            "issn",
            "isbn",
            "volumen",
            "issue",
            "primera_página",
            "pais_producto",
            "última_página",
            "entidad_premiadora",
            "ranking"]

        self.task = config["ciarp_works"]["task"]
        self.n_jobs = config["ciarp_works"].get("num_jobs", 1)
        self.verbose = config["ciarp_works"].get("verbose", 0)
        self.es_semaphore = None
        if self.es_handler is not None and self.task != "doi":
            es_max_concurrency = config["ciarp_works"].get(
                "es_max_concurrency", 8)
            if es_max_concurrency < 1:
                raise ValueError(
                    "ciarp_works.es_max_concurrency must be greater than zero")
            self.es_semaphore = BoundedSemaphore(es_max_concurrency)

    def _source_collection(self, database):
        source_url = database.get("database_url", self.mongodb_url)
        source_db_name = database.get("database_name", "institutional_data")
        source_collection_name = database.get(
            "ciarp_collection_name", database.get("collection_name", "ciarp"))
        client = MongoClient(source_url)
        return client, client[source_db_name][source_collection_name], source_db_name, source_collection_name

    def _institution_ids(self):
        if self.ciarp_databases:
            return [
                database["institution_id"].replace("https://ror.org/", "")
                for database in self.ciarp_databases
            ]

        client, collection, _, _ = self._source_collection(
            self.config["ciarp_works"])
        try:
            return sorted(collection.distinct("institution_id"))
        finally:
            client.close()

    def _config_for_institution(self, institution_id):
        for database in self.ciarp_databases:
            if database["institution_id"].replace(
                    "https://ror.org/", "") == institution_id:
                return database
        return self.config["ciarp_works"]

    def _load_ciarp_data(self, database, institution_id):
        dtype_mapping = {col: str for col in self.required_columns}
        if "file_path" in database:
            data = read_excel(
                database["file_path"],
                dtype=dtype_mapping).fillna("")
            for col in self.required_columns:
                if col not in data.columns:
                    print(
                        f"Column {col} found not in file {
                            database['file_path']}, and it is required.")
                    return None
            source = str(Path(database["file_path"]).resolve())
            data["source_record_id"] = [
                f"{source}: {row_index}" for row_index in data.index]
            return data

        client, collection, source_db_name, source_collection_name = self._source_collection(
            database)
        try:
            records = list(collection.find({"institution_id": institution_id}))
        finally:
            client.close()

        if not records:
            raise ValueError(
                f"No CIARP records found for institution {institution_id} in "
                f"{source_db_name}.{source_collection_name}")

        for record in records:
            record["source_record_id"] = str(record.pop("_id"))
        data = DataFrame(records).fillna("")
        for col in self.required_columns:
            if col not in data.columns:
                data[col] = ""
            data[col] = data[col].astype(str)
        return data

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
        for source_institution_id in self._institution_ids():
            database = self._config_for_institution(source_institution_id)
            institution_id = f"https: //ror.org/{source_institution_id}"
            self.aff_reg = self.db["affiliations"].find_one(
                {"external_ids.id": institution_id})
            if not self.aff_reg:
                self.aff_reg = self.db["affiliations"].find_one(
                    {"_id": source_institution_id})
            if not self.aff_reg:
                print(
                    f"WARNING: Affiliation {institution_id} not found")
                continue

            self.ciarp = self._load_ciarp_data(database, source_institution_id)
            if self.ciarp is None:
                return

            # Some CIARP sources use placeholders such as "0" in the DOI
            # column. Skip institutions without any valid DOI before asking
            # for their category mapping; there is nothing to process for the
            # /doi task in that case.
            if self.task == "doi":
                valid_doi = self.ciarp["doi"].map(
                    lambda doi: bool(doi_processor(doi)) if doi else False
                )
                self.ciarp = self.ciarp[valid_doi].copy()
                if self.ciarp.empty:
                    if self.verbose > 0:
                        print(
                            f"Skipping {institution_id}: no valid DOI records found")
                    continue

            # Get allowed categories for the current entity
            allowed_categories = ciarp_mapping(institution_id, "works")

            # Filter DataFrame by `ranking` field
            self.filtered_ciarp = self.ciarp[self.ciarp["ranking"].isin(
                allowed_categories)].copy()
            self.filtered_ciarp = self.filtered_ciarp[
                self.filtered_ciarp["título"].str.strip().ne("")].copy()
            if self.verbose > 0:
                print(
                    "Filtering by {} categories of works".format(
                        len(allowed_categories)))

            # Keep the historical timestamp identifier. source_record_id is
            # separately used to make retries idempotent.
            index = []
            for i, rec in enumerate(self.filtered_ciarp["identificación"]):
                # row index - cedula - timestamp
                index.append(f"{str(i)}-{rec}-{int(time())}")
            self.filtered_ciarp['index'] = index
            self.filtered_ciarp = self.filtered_ciarp.to_dict(orient="records")

            # DOI validity was checked before resolving the institution
            # mapping. The default task keeps only records without a valid DOI.
            if self.task != "doi":
                # TODO: implement similarity task and a default task that runs
                # all
                papers = []
                for par in self.filtered_ciarp:
                    if par["doi"] == "":
                        papers.append(par)
                    elif not doi_processor(par["doi"]):
                        papers.append(par)
                self.filtered_ciarp = papers

            with MongoClient(self.mongodb_url) as client:
                db = client[self.config["database_name"]]
                collection = db["works"]

                results = Parallel(
                    n_jobs=self.n_jobs,
                    verbose=self.verbose,
                    backend="threading",
                    return_as="generator_unordered")(
                    delayed(process_one)(
                        paper,
                        db,
                        collection,
                        self.aff_reg,
                        self.empty_work(),
                        True if self.task != "doi" else False,
                        self.es_handler,
                        verbose=self.verbose,
                        es_semaphore=self.es_semaphore,
                    ) for paper in self.filtered_ciarp
                )
                for _ in results:
                    pass

    def run(self):
        """
        Method to run the process_ciarp method.
        Entrypoint for the Kahi_ciarp_works class to be excute by kahi.
        """
        self.process_ciarp()
        return 0
