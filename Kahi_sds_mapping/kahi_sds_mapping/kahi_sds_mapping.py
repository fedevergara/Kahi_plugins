from pymongo import MongoClient, UpdateOne
from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase


class Kahi_sds_mapping(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]
        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]

        self.sds_mapping_db = config["sds_mapping"]["database_name"] if "database_name" in config["sds_mapping"].keys(
        ) else "sds"

        self.n_jobs = config["sds_mapping"]["num_jobs"] if "num_jobs" in config["sds_mapping"].keys(
        ) else 1

        self.verbose = config["sds_mapping"][
            "verbose"] if "verbose" in config["sds_mapping"].keys() else 0
        print("Kahi_sds_mapping initialized")


    def process_works(self, reg, collection):
        keys = [
            '_id', 'updated', 'titles', 'subtitle', 'abstract', 'keywords', 'types', 'ranking', 'external_ids','external_urls',
            'date_published', 'year_published', 'bibliographic_info', 'authors', 'author_count', 'subjects', 'policies', 'citations_count'
        ]

        entry = {}
        for key in keys:
            if key not in reg.keys():
                reg[key] = None
            entry[key] = reg[key]

        self.sds_mapping_db[collection].insert_one(entry)
        return 0


    def process_sources(self, reg, collection):
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
            entry[key] = reg[key]

        self.sds_mapping_db[collection].insert_one(entry)
        return 0


    def process_affiliations(self, reg, collection):
        keys = [
            '_id', 'updated', 'names', 'aliases', 'abbreviations', 'types', 'year_established', 'status', 'relations', 'addresses', 'external_urls',
            'external_ids', 'subjects', 'ranking', 'citations_count', 'products_count', 'citations_by_year', 'products_by_year',
            'coauthors_network', 'subjects_by_year', 'policies'
        ]

        ror_stage = self.client["ror_2024"]["stage"]
        entry = {}

        for key in keys:
            if key not in reg.keys():
                reg[key] = None
            # status
            if key == "status":
                ror_id = [aff["id"] for aff in reg["external_ids"] if aff["source"] == "ror"]
                if ror_id:
                    aff_db = ror_stage.find_one({"id": ror_id[0]}, {"status": 1})
                    if aff_db:
                        entry[key] = aff_db["status"]
                        continue
                reg[key] = ""
            # 'citations_count', 'products_count', 'citations_by_year', 'products_by_year', 'coauthors_network', 'subjects_by_year', 'policies'
            entry[key] = reg[key]

        self.sds_mapping_db[collection].insert_one(entry)
        return 0


    def process_person(self, reg, collection):
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

        self.sds_mapping_db[collection].insert_one(entry)
        return 0


    def process_subjects(self, reg, collection):
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

        self.sds_mapping_db[collection].insert_one(entry)
        return 0


    def process_policies(self, reg):
        # policies
        return 0


    def products_count(self):
        collections = [
            {"name": "affiliations", "query_field": "authors.affiliations.id"},
            {"name": "person", "query_field": "authors.id"},
            {"name": "policies", "query_field": "policies.id"},
            {"name": "subjects", "query_field": "subjects.id"}
        ]

        bulk_operations = []

        for collection_info in collections:
            collection_name = collection_info["name"]
            query_field = collection_info["query_field"]

            pipeline = [
                {"$match": {query_field: {"$exists": True}}},
                {"$group": {"_id": "$" + query_field, "count": {"$sum": 1}}}
            ]

            cursor = self.sds_mapping_db["works"].aggregate(pipeline)

            bulk_operations = []

            for result in cursor:
                bulk_operations.append(
                    UpdateOne(
                        {"_id": result["_id"]},
                        {"$set": {"products_count": result["count"], "citations_count": 0}},
                        upsert=True
                    )
                )

            if bulk_operations:
                self.sds_mapping_db[collection_name].bulk_write(bulk_operations)


    def process_products_and_citations(self, collection, collection_query, works_query, update_field):
        for doc in collection.find(collection_query):
            year_product_count = []
            year_citations_count = []
            year_count_map = {}

            for paper in self.sds_mapping_db["works"].find({works_query: doc["_id"]}, no_cursor_timeout=True):
                if paper.get("year_published"):
                    year = int(paper["year_published"])
                    if year not in year_count_map:
                        year_count_map[year] = {"products": 0, "citations": 0}
                    year_count_map[year]["products"] += 1

            for year, counts in year_count_map.items():
                year_product_count.append({"value": counts["products"], "year": year})
                year_citations_count.append({"value": counts["citations"], "year": year})

            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"products_by_year": year_product_count, "citations_by_year": year_citations_count}}
            )


    def products_and_citations(self):
        collections = [
            {"name": "affiliations", "collection_query": {}, "works_query": "authors.affiliations.id"},
            {"name": "person", "collection_query": {"products_by_year": {"$exists": 0}}, "works_query": "authors.id"}
        ]
        for collection_info in collections:
            collection_name = collection_info["name"]
            collection_query = collection_info["collection_query"]
            works_query = collection_info["works_query"]

            self.process_products_and_citations(self.sds_mapping_db[collection_name], collection_query, works_query, collection_name)


    def process_collection(self, reg, collection, verbose=0):

        if collection == "works":
            self.process_works(reg, collection, erbose=0)
        if collection == "sources":
            self.process_sources(reg, collection, verbose=0)
        if collection == "affiliations":
            self.process_affiliations(reg, collection, verbose=0)
        if collection == "person":
            self.process_person(reg, collection, verbose=0)
        if collection == "subjects":
            self.process_subjects(reg, collection, verbose=0)

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

        # Products_count
        self.products_count()

        # Products and citations
        self.products_and_citations()

    def run(self):
        self.process_data()
        return 0
