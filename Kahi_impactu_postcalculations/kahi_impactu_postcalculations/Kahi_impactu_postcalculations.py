from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase
from pymongo import MongoClient
import subprocess
from spacy import cli
from kahi_impactu_postcalculations.utils import network_creation, top_words, start_mongo_client, count_works_one


class Kahi_impactu_postcalculations(KahiBase):
    """
    Plugin for performing post calculations for Impactu.

    This class extends KahiBase and implements functions for creating co-authorship networks and
    extracting top words
    """

    config = {}
    words_inserted_ids = []

    def __init__(self, config):
        """
        Initialize the Kahi_impactu_postcalculations plugin.
        """
        self.config = config

        self.mongodb_url = config["database_url"]
        self.database_name = config["database_name"]

        self.impactu_database_url = config["impactu_postcalculations"]["database_url"]
        self.impactu_database_name = config["impactu_postcalculations"]["database_name"]
        self.verbose = self.config["impactu_postcalculations"]["verbose"]
        self.n_jobs = self.config["impactu_postcalculations"]["n_jobs"]
        self.author_count = self.config["impactu_postcalculations"][
            "author_count"] if "author_count" in self.config["impactu_postcalculations"] else 6
        self._check_and_install_spacy_models()

    def _check_and_install_spacy_models(self):
        """
        Check if the spaCy models are installed and install them if needed.
        """
        if not self.are_spacy_models_installed():
            self.install_spacy_models()

    def are_spacy_models_installed(self):
        """
        Check if the required spaCy models are installed.

        Returns:
            bool: True if models are installed, False otherwise.
        """
        return "en_core_web_sm" in cli.info()["pipelines"].keys() and "es_core_news_sm" in cli.info()["pipelines"].keys()

    def install_spacy_models(self):
        """
        Install the required spaCy models.
        """
        subprocess.run(["python3", "-m", "spacy",
                       "download", "en_core_web_sm"])
        subprocess.run(["python3", "-m", "spacy",
                       "download", "es_core_news_sm"])

    def run(self):
        """
        Execute the plugin to create co-authorship networks and extract top words.
        """
        start_mongo_client(self.mongodb_url, self.database_name,
                           self.impactu_database_url, self.impactu_database_name)

        client = MongoClient(self.mongodb_url)
        db = client[self.database_name]

        if self.verbose > 1:
            print("INFO: Creating indexes")
        db["works"].create_index("authors.id")

        # Getting the list of institutions ids with works
        if self.verbose > 1:
            print("INFO: Getting authors and affiliations ids")
        institutions_ids = []

        for aff in db["affiliations"].find({"types.type": {"$nin": ["faculty", "department", "group"]}}, {"_id": 1}):
            count = db["works"].count_documents(
                {"authors.affiliations.id": aff["_id"]})
            if count != 0:
                institutions_ids.append(aff["_id"])

        # Getting the list of authors ids
        authors_ids = [x["_id"] for x in db["person"].find({}, {"_id": 1})]

        # Creating the networks of coauthorship for each affiliation

        if self.verbose > 1:
            print("INFO: Creating affiliations networks")

        if institutions_ids:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend="multiprocessing")(
                    delayed(network_creation)(
                        oaid,
                        "affiliations",
                        self.author_count
                    ) for oaid in institutions_ids)

        # Getting the list of authors ids with works
        if self.verbose > 1:
            print("INFO: Checking authors with works")

        authors_ids = Parallel(n_jobs=self.n_jobs, backend="multiprocessing", verbose=10)(
            delayed(count_works_one)(author) for author in authors_ids)
        authors_ids = [x for x in authors_ids if x is not None]

        # Creating the networks of coauthorship for each author
        if authors_ids:
            Parallel(
                n_jobs=self.n_jobs,
                verbose=10,
                backend="multiprocessing")(
                    delayed(network_creation)(
                        oaid,
                        "person",
                        self.author_count
                    ) for oaid in authors_ids)

        # Getting the top words for institution - step 1
        if self.verbose > 1:
            print("INFO: Creating top words for institutions - step 1")
        Parallel(
            n_jobs=self.n_jobs,
            verbose=10,
            backend="multiprocessing")(
                delayed(top_words)(
                    '1',
                    aff
                ) for aff in db["affiliations"].find())

        # Getting the top words for institution - step 2
        if self.verbose > 1:
            print("INFO: Creating top words for institutions - step 2")
        Parallel(
            n_jobs=self.n_jobs,
            verbose=10,
            backend="multiprocessing")(
                delayed(top_words)(
                    '2',
                    aff
                ) for aff in db["affiliations"].find({"types.type": {"$in": ["faculty", "department", "group"]}}))

        # Getting the top words for authors
        if self.verbose > 1:
            print("INFO: Creating top words for authors")
        Parallel(
            n_jobs=self.n_jobs,
            verbose=10,
            backend="multiprocessing")(
                delayed(top_words)(
                    '3',
                    author,
                    self.words_inserted_ids
                ) for author in db["person"].find({"_id": {"$nin": self.words_inserted_ids}}, no_cursor_timeout=True))

        client.close()
