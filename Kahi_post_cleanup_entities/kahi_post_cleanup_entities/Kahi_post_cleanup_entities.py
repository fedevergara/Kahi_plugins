from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from joblib import Parallel, delayed


class Kahi_post_cleanup_entities(KahiBase):

    config = {}

    def __init__(self, config):
        """
        Constructor for the class, database connection is established here
        and collections are initialized.

        Parameters:
        ----------
        config : dict
            config:
            database_url: localhost:27017
            database_name: kahi
            log_database: kahi
            log_collection: log
            workflow:
            ##Works plugins here
            post_cleanup_entities: # run this after all works plugins are done
                num_jobs: 20
                verbose: 4
        """
        self.config = config
        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.works = self.db["works"]
        self.person = self.db["person"]
        self.affiliations = self.db["affiliations"]

        self.n_jobs = config["post_cleanup_entities"]["num_jobs"] if "num_jobs" in config["post_cleanup_entities"].keys(
        ) else 1
        self.verbose = config["post_cleanup_entities"]["verbose"] if "verbose" in config["post_cleanup_entities"].keys(
        ) else 0
        self.dry_run = config["post_cleanup_entities"].get("dry_run", True)
        self.entity_collections = [
            self.db[name] for name in ("works", "events", "projects", "patents")
            if name in self.db.list_collection_names()
        ]
        # Every branch used by cleanup_author/cleanup_affiliation must be
        # indexed. If one branch of the $or lacks an index, MongoDB falls back
        # to a full collection scan for every affiliation.
        for collection in self.entity_collections:
            collection.create_index("authors.id")
            collection.create_index("authors.affiliations.id")
            collection.create_index("groups.id")

    def cleanup_author(self, author):
        """
        remove author if no works are associated with the author

        Parameters:
        ----------
        author : dict
            author object from the person collection with _id field
        """
        referenced = any(
            collection.count_documents({"authors.id": author["_id"]}, limit=1)
            for collection in self.entity_collections
        )
        if not referenced:
            if not self.dry_run:
                self.person.delete_one({"_id": author["_id"]})
            return 1
        return 0

    def cleanup_affiliation(self, affiliation):
        """
        remove affiliation if no authors are associated with the affiliation

        Parameters:
        ----------
        affiliation : dict
            affiliation object from the affiliations collection with _id field
        """
        affiliation_id = affiliation["_id"]
        referenced = self.person.count_documents(
            {"affiliations.id": affiliation_id}, limit=1
        ) > 0
        if not referenced:
            referenced = self.affiliations.count_documents(
                {"relations.id": affiliation_id}, limit=1
            ) > 0
        if not referenced:
            referenced = any(
                collection.count_documents(
                    {"$or": [
                        {"authors.affiliations.id": affiliation_id},
                        {"groups.id": affiliation_id},
                    ]}, limit=1,
                )
                for collection in self.entity_collections
            )
        if not referenced:
            if not self.dry_run:
                self.affiliations.delete_one({"_id": affiliation_id})
            return 1
        return 0

    def run(self):
        """
        Run the post cleanup process for authors and affiliations
        """
        authors = self.person.find({}, {"_id": 1})
        out = Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading", pre_dispatch="2*n_jobs",
            return_as="generator_unordered")(
            delayed(self.cleanup_author)(
                author
            ) for author in authors
        )
        print("INFO: {} {} authors".format(
            "Would remove" if self.dry_run else "Removed", sum(out)))

        affiliations = self.affiliations.find({}, {"_id": 1})
        out = Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading", pre_dispatch="2*n_jobs",
            return_as="generator_unordered")(
            delayed(self.cleanup_affiliation)(
                affiliation
            ) for affiliation in affiliations
        )
        print("INFO: {} {} affiliations".format(
            "Would remove" if self.dry_run else "Removed", sum(out)))
        return 0
