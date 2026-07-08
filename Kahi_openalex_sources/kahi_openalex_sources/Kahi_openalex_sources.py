from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from time import time
from joblib import Parallel, delayed


def get_global_counts(source):
    return {
        "global_products_count": source.get("works_count") or 0,
        "global_citations_count": source.get("cited_by_count") or 0,
    }


def get_open_access_start_year(source):
    if "open_access_start_year" not in source:
        return None
    return source["open_access_start_year"]


def iter_issns(value):
    if not value:
        return []
    if isinstance(value, list):
        raw_values = value
    else:
        raw_values = [value]
    issns = []
    for raw in raw_values:
        if raw is None:
            continue
        issn = str(raw).strip()
        if issn and issn not in issns:
            issns.append(issn)
    return issns


def has_external_id(entry, source, identifier):
    identifier = str(identifier)
    for ext in entry["external_ids"]:
        if ext.get("source") == source and str(ext.get("id")) == identifier:
            return True
    return False


def append_external_id(entry, source, identifier):
    if identifier is None:
        return
    identifier = str(identifier).strip()
    if not identifier or has_external_id(entry, source, identifier):
        return
    entry["external_ids"].append({"source": source, "id": identifier})


def process_one(source, client, db_name, empty_source):
    db = client[db_name]
    collection = db["sources"]
    global_counts = get_global_counts(source)
    open_access_start_year = get_open_access_start_year(source)

    source_db = collection.find_one(
        {"external_ids": {"$elemMatch": {"source": "openalex", "id": source["id"]}}}
    )
    if not source_db:
        issns = iter_issns(source.get("issn"))
        if issns:
            source_db = collection.find_one({"external_ids.id": {"$in": issns}})
    if not source_db:
        if "issn_l" in source.keys():
            if source["issn_l"]:
                source_db = collection.find_one(
                    {"external_ids.id": source["issn_l"]})
    if source_db:
        updated_found = False
        for upd in source_db["updated"]:
            if upd["source"] == "openalex":
                upd["time"] = int(time())
                updated_found = True
        if not updated_found:
            source_db["updated"].append(
                {"source": "openalex", "time": int(time())})

        append_external_id(source_db, "openalex", source["id"])
        for issn in iter_issns(source.get("issn")):
            append_external_id(source_db, "issn", issn)
        append_external_id(source_db, "issn_l", source.get("issn_l"))

        if "type" in source.keys():
            if source["type"]:
                type_found = False
                for typ in source_db["types"]:
                    if typ["source"] == "openalex":
                        type_found = True
                        break
                if not type_found:
                    source_db["types"].append(
                        {"source": "openalex", "type": source["type"]})
        name_found = False
        for name in source_db["names"]:
            if name["name"] == source["display_name"]:
                name_found = True
                break
        if not name_found:
            source_db["names"].append(
                {"lang": "en", "name": source["display_name"], "source": "openalex"})
        if "is_oa" in source.keys():
            is_oa = source.get("is_oa")
            apc = source.get("apc_prices")
            has_apc = False
            if apc:
                for item in apc:
                    if isinstance(item, dict) and item.get("price"):
                        has_apc = True
                        break
            oa_reg = {
                "provenance": "openalex",
                "is_open_access": bool(is_oa),
                "open_access_diamond": is_oa and not has_apc
            }
            if oa_reg not in source_db["open_access"]:
                source_db["open_access"].append(oa_reg)

        update_fields = {
            "updated": source_db["updated"],
            "names": source_db["names"],
            "external_ids": source_db["external_ids"],
            "types": source_db["types"],
            "subjects": source_db["subjects"],
            "open_access": source_db["open_access"],
            "global_products_count": global_counts["global_products_count"],
            "global_citations_count": global_counts["global_citations_count"],
        }
        if open_access_start_year is not None:
            update_fields["open_access_start_year"] = open_access_start_year

        collection.update_one(
            {"_id": source_db["_id"]},
            {"$set": update_fields},
        )
    else:
        entry = empty_source.copy()
        entry.update(global_counts)
        entry["updated"] = [
            {"source": "openalex", "time": int(time())}]
        entry["names"].append(
            {"lang": "en", "name": source["display_name"], "source": "openalex"})
        entry["external_ids"].append(
            {"source": "openalex", "id": source["id"]})
        for issn in iter_issns(source.get("issn")):
            append_external_id(entry, "issn", issn)
        append_external_id(entry, "issn_l", source.get("issn_l"))
        if "type" in source.keys():
            if source["type"]:
                entry["types"].append(
                    {"source": "openalex", "type": source["type"]})
        if "publisher" in source.keys():
            if source["publisher"]:
                entry["publisher"] = {
                    "name": source["publisher"], "country_code": source["country_code"] if "country_code" in source.keys() else ""
                }
        if "apc_usd" in source.keys():
            if source["apc_usd"]:
                entry["apc"] = {"currency": "USD",
                                "charges": source["apc_usd"]}
        if open_access_start_year is not None:
            entry["open_access_start_year"] = open_access_start_year
        if "is_oa" in source.keys():
            is_oa = source.get("is_oa")
            apc = source.get("apc_prices")
            has_apc = False
            if apc:
                for item in apc:
                    if isinstance(item, dict) and item.get("price"):
                        has_apc = True
                        break
            entry["open_access"].append(
                {
                    "provenance": "openalex",
                    "is_open_access": bool(is_oa),
                    "open_access_diamond": is_oa and not has_apc
                }
            )
        if "abbreviated_title" in source.keys():
            if source["abbreviated_title"]:
                entry["abbreviations"].append(
                    source["abbreviated_title"])
        if "alternate_titles" in source.keys():
            if source["alternate_titles"]:
                for name in source["alternate_titles"]:
                    entry["abbreviations"].append(name)
        if source["homepage_url"]:
            entry["external_urls"].append(
                {"source": "site", "url": source["homepage_url"]})
        if source["societies"]:
            for soc in source["societies"]:
                entry["external_urls"].append(
                    {"source": soc["organization"], "url": soc["url"]})

        collection.insert_one(entry)


class Kahi_openalex_sources(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]

        self.collection.create_index("external_ids.id")

        self.openalex_client = MongoClient(
            config["openalex_sources"]["database_url"])

        if config["openalex_sources"]["database_name"] not in self.openalex_client.list_database_names():
            raise RuntimeError(
                f'''Database {config["openalex_sources"]["database_name"]} was not found''')

        self.openalex_db = self.openalex_client[config["openalex_sources"]
                                                ["database_name"]]

        if config["openalex_sources"]["collection_name"] not in self.openalex_db.list_collection_names():
            raise RuntimeError(
                f'''Collection {config["openalex_sources"]["collection_name"]} was not found on database {config["openalex_sources"]["database_name"]}''')

        self.openalex_collection = self.openalex_db[config["openalex_sources"]
                                                    ["collection_name"]]

        self.n_jobs = config["openalex_sources"]["num_jobs"]
        self.client.close()

    def process_openalex(self):
        source_cursor = self.openalex_collection.find(no_cursor_timeout=True)
        client = MongoClient(self.mongodb_url)
        Parallel(
            n_jobs=self.n_jobs,
            verbose=10,
            backend="threading")(
            delayed(process_one)(
                source,
                client,
                self.config["database_name"],
                self.empty_source(),
            ) for source in source_cursor
        )
        client.close()

    def run(self):
        self.process_openalex()
        return 0
