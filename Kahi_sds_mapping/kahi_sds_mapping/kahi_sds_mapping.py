from pymongo import MongoClient
from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase


class Kahi_sds_mapping(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]
        self.client = MongoClient(config["database_url"])
        self.db = self.client[config["database_name"]]

        self.sds_mapping_db = config["sds_mapping"]["database_name"] if "database_name" in config["sds_mapping"].keys(
        ) else "sds"

        self.n_jobs = config["sds_mapping"]["num_jobs"] if "num_jobs" in config["sds_mapping"].keys(
        ) else 1

        self.verbose = config["sds_mapping"][
            "verbose"] if "verbose" in config["sds_mapping"].keys() else 0
        print("Kahi_sds_mapping initialized")


    def process_works(self, reg):
        keys = [
            '_id', 'updated', 'titles', 'subtitle', 'abstract', 'keywords', 'types', 'ranking', 'external_ids','external_urls',
            'date_published', 'year_published', 'bibliographic_info', 'authors', 'author_count', 'subjects', 'policies', 'citations_count'
        ]

        entry = {}
        for key in keys:
            if key not in reg.keys():
                reg[key] = None
            entry[key] = reg[key]

        self.sds_mapping_db.works.insert_one(entry)
        return 0


    def process_sources(self, reg):
        keys = [
            '_id', 'updated', 'names', 'aliases', 'abbreviations', 'keywords', 'types', 'languages', 'publisher', 'open_access_start_year',
            'status', 'relations', 'addresses', 'external_urls', 'external_ids', 'subjects', 'ranking', 'review_process', 'plagiarism_detection',
            'publication_time_weeks', 'apc', 'copyright', 'licenses'
        ]

        entry = {}
        for key in keys:
            if key not in reg.keys():
                reg[key] = None
            if key == "aliases": # None of the records have aliases
                entry[key] = []
                continue
            elif key == "status": # None of the records have status
                entry[key] = ""
                continue
            # status
            entry[key] = reg[key]

        self.sds_mapping_db.sources.insert_one(entry)
        return 0


    def process_affiliations(self, reg):
        keys = [
            '_id', 'updated', 'names', 'aliases', 'abbreviations', 'types', 'year_established', 'status', 'relations', 'addresses', 'external_urls',
            'external_ids', 'subjects', 'ranking', 'citations_count', 'products_count', 'citations_by_year', 'products_by_year',
            'coauthors_network', 'subjects_by_year', 'policies'
        ]

        entry = {}

        for key in keys:
            if key not in reg.keys():
                reg[key] = None
            if key == "status": # Some records have "active" status
                entry[key] = ""
                continue
            # 'citations_count', 'products_count', 'citations_by_year', 'products_by_year', 'coauthors_network', 'subjects_by_year', 'policies'
            entry[key] = reg[key]

        self.sds_mapping_db.affiliations.insert_one(entry)
        return 0


    def process_person(self, reg):
        keys = [
            '_id', 'updated', 'full_name', 'first_names', 'last_names', 'initials', 'aliases', 'affiliations', 'keywords', 'external_ids',
            'sex', 'birthplace', 'birthdate', 'degrees', 'ranking', 'subjects', 'citations_by_year', 'products_by_year', 'coauthors_network',
            'subjects_by_year', 'products_count', 'citations_count', 'policies'
        ]

        entry = {}

        for key in keys:
            if key not in reg.keys():
                reg[key] = None
            # 'citations_by_year', 'products_by_year', 'coauthors_network', 'subjects_by_year', 'products_count', 'citations_count', 'policies'
            entry[key] = reg[key]

        self.sds_mapping_db.person.insert_one(entry)
        return 0


    def process_subjects(self, reg):
        keys = [
            '_id', 'updated', 'names', 'descriptions', 'external_ids', 'external_urls', 'level', 'related_subjects', 'relations', 'citations_count',
            'products_count', 'counts_by_year', 'affiliations', 'authors'
        ]

        entry = {}

        for key in keys:
            if key not in reg.keys():
                reg[key] = None
            # related_subjects, citations_count, 'products_count', 'counts_by_year', 'affiliations', 'authors'
            entry[key] = reg[key]

        self.sds_mapping_db.subjects.insert_one(entry)
        return 0


    def process_policies(self, reg):
        # policies
        return 0


    def process_collection(self, reg, collection, verbose=0):

        if collection == "works":
            self.process_works(reg, verbose=0)
        if collection == "sources":
            self.process_sources(reg, verbose=0)
        if collection == "affiliations":
            self.process_affiliations(reg, verbose=0)
        if collection == "person":
            self.process_person(reg, verbose=0)
        if collection == "subjects":
            self.process_subjects(reg, verbose=0)

        self.process_policies(reg, verbose=0)
        return 0


    def process_data(self):
        collections = ['works', 'sources', 'affiliations', 'person', 'subjects', 'policies']
        for collection in collections:
            with MongoClient(self.mongodb_url) as client:

                Parallel(
                    n_jobs=self.n_jobs,
                    verbose=2,
                    backend="threading")(
                    delayed(self.process_collection)(
                        reg,
                        collection,
                        self.verbose
                    ) for reg in self.db[collection].find()
                )
                client.close()

    def run(self):
        self.process_data()
        return 0
