from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed


def process_one(oa_aff, collection, empty_affiliations, max_tries=10):

    db_reg = None
    for source, idx in oa_aff["ids"].items():
        db_reg = collection.find_one({"external_ids.id": idx})
        if db_reg:
            break
    if db_reg:
        for upd in db_reg["updated"]:
            if upd["source"] == "openalex":
                return  # Should it be update-able?
        db_reg["updated"].append({"time": int(time()), "source": "openalex"})
        id_sources = [ext["id"] for ext in db_reg["external_ids"]]
        for source, idx in oa_aff["ids"].items():
            if isinstance(idx, str):
                if "http" in idx and "openalex" not in idx:
                    continue
            if idx not in id_sources:
                db_reg["external_ids"].append({"source": source, "id": idx})
        url_sources = [ext["url"] for ext in db_reg["external_urls"]]
        for source, url in oa_aff["ids"].items():
            if url not in url_sources:
                db_reg["external_urls"].append({"source": source, "url": url})

        # addresses
        if len(db_reg["addresses"]) == 0:
            db_reg["addresses"] = [
                {
                    "lat": oa_aff["geo"]["latitude"],
                    "lng": oa_aff["geo"]["longitude"],
                    "state": oa_aff["geo"]["region"],
                    "city": oa_aff["geo"]["city"],
                    "city_id": oa_aff["geo"]["geo_names_city_id"],
                    "country": oa_aff["geo"]["country"],
                    "country_code": oa_aff["geo"]["country_code"]
                }
            ]

        # names
        langs = [name["lang"] for name in db_reg["names"]]
        for lang, name in oa_aff["international"]["display_name"].items():
            if lang not in langs:
                langs.append(lang)
                db_reg["names"].append(
                    {"source": "openalex", "lang": lang, "name": name})
        # types
        types_source = [typ["source"] for typ in db_reg["types"]]
        if "openalex" not in types_source:
            db_reg["types"].append(
                {"source": "openalex", "type": oa_aff["type"]})
        # abbreviations
        for abv in oa_aff["display_name_alternatives"]:
            if abv not in db_reg["abbreviations"]:
                db_reg["abbreviations"].append(abv)
        collection.update_one(
            {"_id": db_reg["_id"]},
            {"$set": {
                "updated": db_reg["updated"],
                "external_ids": db_reg["external_ids"],
                "external_urls": db_reg["external_urls"],
                "addresses": db_reg["addresses"],
                "names": db_reg["names"],
                "types": db_reg["types"],
                "abbreviations": db_reg["abbreviations"]
            }}
        )
    else:
        entry = empty_affiliations.copy()
        entry["updated"].append({"time": int(time()), "source": "openalex"})
        # names
        entry["names"].append(
            {"source": "openalex", "lang": "en", "name": oa_aff["display_name"]})
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
        # types
        entry["types"].append({"source": "openalex", "type": oa_aff["type"]})
        # abbreviations
        for abv in oa_aff["display_name_alternatives"]:
            entry["abbreviations"].append(abv)
        # addresses
        entry["addresses"] = [
            {
                "lat": oa_aff["geo"]["latitude"],
                "lng": oa_aff["geo"]["longitude"],
                "state": oa_aff["geo"]["region"],
                "city": oa_aff["geo"]["city"],
                "city_id": oa_aff["geo"]["geonames_city_id"],
                "country": oa_aff["geo"]["country"],
                "country_code": oa_aff["geo"]["country_code"]
            }
        ]
        collection.insert_one(entry)


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
        if config["openalex_publishers"]["collection_name"] not in self.openalex_db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                config["openalex_publishers"]['collection_name'], config["openalex_publishers"]["database_url"]))

        self.openalex_collection = self.openalex_db[config["openalex_publishers"]
                                                    ["collection_name"]]

        self.n_jobs = config["openalex_publishers"]["num_jobs"] if "num_jobs" in config["openalex_publishers"].keys(
        ) else 1
        self.verbose = config["openalex_publishers"]["verbose"] if "verbose" in config["openalex_publishers"].keys(
        ) else 0

        self.client.close()

    def process_openalex(self):
        publishers_cursor = self.openalex_collection.find(
            no_cursor_timeout=True)

        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["publishers"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(process_one)(
                    aff,
                    collection,
                    self.empty_affiliation()
                ) for aff in publishers_cursor
            )
            client.close()

    def run(self):
        self.process_openalex()
        return 0
