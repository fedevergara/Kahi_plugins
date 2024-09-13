from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed


def process_one(oa_aff, publishers_collection, affiliations_collection, subjects_collection, empty_affiliations, max_tries=10):

    db_reg = None
    for source, idx in oa_aff["ids"].items():
        db_reg = publishers_collection.find_one({"external_ids.id": idx})
        if db_reg:
            break
    if db_reg:
        for upd in db_reg["updated"]:
            if upd["source"] == "openalex":
                return  # Should it be update-able?
        # Update publisher with the data of other source
        return

    else:
        entry = empty_affiliations.copy()
        entry["updated"].append({"time": int(time()), "source": "openalex"})
        # names
        db_aff = None
        if "roles" in oa_aff.keys() and oa_aff["roles"]:
            entry["relations"] = [{"source": "openalex", "role": role["role"], "id": role["id"]}
                                  for role in oa_aff["roles"] if "role" in role and "id" in role]
            institution_id = next((role["id"] for role in entry["relations"] if role["role"] == "institution"), None)
            if institution_id:
                db_aff = affiliations_collection.find_one(
                    {"external_ids.id": institution_id})
        if db_aff:
            entry["names"].append(db_aff["names"])
        else:
            entry["names"].append(
                {"source": "openalex", "lang": "en", "name": oa_aff["display_name"]})
        # linage
        entry["lineage"] = [{"source": "openalex", "parent": False, "id": ling} for ling in oa_aff["lineage"]]
        # parent_publisher
        if "parent_publisher" in oa_aff.keys() and oa_aff["parent_publisher"]:
            parent_publisher_id = oa_aff["parent_publisher"]["id"]
            # Search for parent_publisher in lineage and set parent to True
            for lineage in entry["lineage"]:
                if lineage["id"] == parent_publisher_id:
                    lineage["parent"] = True
                    break  # Only one parent_publisher
        # herarchy_level
        entry["hierarchy_level"] = [{"source": "openalex", "level": oa_aff["hierarchy_level"]}]
        # external_ids
        for source, idx in oa_aff["ids"].items():
            if isinstance(idx, str):
                if "http" in idx and "openalex" not in idx:
                    continue
            entry["external_ids"].append({"source": source, "id": idx})
        # external_urls
        for source, url in oa_aff["ids"].items():
            entry["external_urls"].append({"source": source, "url": url})
        if oa_aff["homepage_url"]:
            entry["external_urls"].append(
                {"source": "site", "url": oa_aff["homepage_url"]})
        if oa_aff["image_url"]:
            entry["external_urls"].append(
                {"source": "logo", "url": oa_aff["image_url"]})
        # subjects
        if "x_concepts" in oa_aff.keys() and oa_aff["x_concepts"]:
            entry["subjects"].append(
                {"source": "openalex", "subjects": []})
            for sub in oa_aff["x_concepts"]:
                entry["subjects"][0]["subjects"].append({"id": sub["id"], "name": sub["display_name"], "level": sub["level"]})
            for sub in entry["subjects"][0]["subjects"]:
                subject_db = subjects_collection.find_one({"external_ids.id": sub["id"]})
                if subject_db:
                    sub["id"] = subject_db["_id"]
        # types
        if db_aff:
            entry["types"].append(db_aff["types"])
        # abbreviations
        if db_aff:
            entry["abbreviations"].append(db_aff["abbreviations"])
        # addresses
        if db_aff:
            entry["addresses"].append(db_aff["addresses"])
        # insert
        publishers_collection.insert_one(entry)


class Kahi_openalex_publishers(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["publishers"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("names.name")
        self.collection.create_index("types.type")
        self.collection.create_index([("names.name", TEXT)])

        self.openalex_client = MongoClient(
            config["openalex_publishers"]["database_url"])
        if config["openalex_publishers"]["database_name"] not in self.openalex_client.list_database_names():
            raise Exception("Database {} not found in {}".format(
                config["openalex_publishers"]['database_name'], config["openalex_publishers"]["database_url"]))

        self.openalex_db = self.openalex_client[config["openalex_publishers"]
                                                ["database_name"]]
        if config["openalex_publishers"]["publishers_collection"] not in self.openalex_db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                config["openalex_publishers"]['publishers_collection'], config["openalex_publishers"]["database_url"]))

        self.publishers_collection = self.openalex_db[config["openalex_publishers"]["publishers_collection"]]

        self.n_jobs = config["openalex_publishers"]["num_jobs"] if "num_jobs" in config["openalex_publishers"].keys(
        ) else 1
        self.verbose = config["openalex_publishers"]["verbose"] if "verbose" in config["openalex_publishers"].keys(
        ) else 0

        self.client.close()

    def process_openalex(self):
        publishers_cursor = self.publishers_collection.find(
            no_cursor_timeout=True)

        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            publishers_collection = db["publishers"]
            affiliations_collection = db["affiliations"]
            subjects_collection = db["subjects"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(process_one)(
                    aff,
                    publishers_collection,
                    affiliations_collection,
                    subjects_collection,
                    self.empty_affiliation()
                ) for aff in publishers_cursor
            )
            client.close()

    def run(self):
        self.process_openalex()
        return 0
