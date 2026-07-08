from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed


def process_relation(sub, client, db_name):
    db = client[db_name]
    collection = db["subjects"]
    relations = []
    relation_ids = set()
    for rel in (sub.get("related_concepts") or []) + (sub.get("ancestors") or []):
        if not rel or not rel.get("id"):
            continue
        sub_db = collection.find_one(
            {"external_ids.id": rel["id"]})
        if sub_db:
            if sub_db["_id"] in relation_ids:
                continue
            relation_ids.add(sub_db["_id"])
            name = sub_db["names"][0]["name"]
            for n in sub_db["names"]:
                if n["lang"] == "en":
                    name = n["name"]
                    break
            rel_entry = {
                "id": sub_db["_id"],
                "name": name,
                "level": sub_db["level"]
            }
            relations.append(rel_entry)
        else:
            print("Could not find related concept in colombia db")
    if len(relations) > 0:
        collection.update_one({"external_ids.id": sub["id"]}, {
            "$set": {"relations": relations}})


class Kahi_openalex_subjects(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["subjects"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index([("names.name", TEXT)])

        self.openalex_client = MongoClient(
            config["openalex_subjects"]["database_url"])

        if config["openalex_subjects"]["database_name"] not in list(self.openalex_client.list_database_names()):
            raise Exception("Database not found in mongodb client")

        self.openalex_db = self.openalex_client[config["openalex_subjects"]
                                                ["database_name"]]

        if config["openalex_subjects"]["collection_name"] not in list(self.openalex_db.list_collection_names()):
            raise Exception("Collection not found in openalex database")

        self.openalex_collection = self.openalex_db[config["openalex_subjects"]
                                                    ["collection_name"]]

        self.n_jobs = config["openalex_subjects"]["num_jobs"]

        self.inserted_concepts = []
        self.inserted_concepts_ids_tuples = []

        self.relations_inserted_ids = []
        for reg in self.collection.find():
            oa_id = ""
            for ext in reg["external_ids"]:
                if ext["source"] == "openalex":
                    oa_id = ext["id"]
                    break
            if oa_id != "":
                self.inserted_concepts.append(oa_id)
                self.inserted_concepts_ids_tuples.append((reg["_id"], oa_id))
                if reg["relations"] != []:
                    self.relations_inserted_ids.append(oa_id)

    def process_openalex(self):
        openalex_subjects = list(self.openalex_collection.find(
            {"id": {"$nin": self.inserted_concepts}}))
        for sub in openalex_subjects:
            if sub["id"] in self.inserted_concepts:
                continue
            db_reg = self.collection.find_one(
                {"external_ids.id": sub["id"]})
            if db_reg:
                self.inserted_concepts.append(sub["id"])
                self.inserted_concepts_ids_tuples.append(
                    (db_reg["_id"], sub["id"]))
                continue
            entry = self.empty_subjects()
            entry["updated"] = [
                {"source": "openalex", "time": int(time())}]
            sources_inserted_ids = []
            entry["external_ids"].append(
                {"source": "openalex", "id": sub["id"]})
            sources_inserted_ids.append("openalex")
            for source, idx in (sub.get("ids") or {}).items():
                if source in sources_inserted_ids:
                    continue
                if idx is None:
                    continue
                entry["external_ids"].append({"source": source, "id": idx})
                sources_inserted_ids.append(source)
            entry["level"] = sub.get("level")
            entry["names"].append(
                {"name": sub.get("display_name"), "lang": "en"})
            inserted_lang_names = ["en"]
            if sub.get("international"):
                if sub["international"].get("display_name"):
                    for lang, name in sub["international"]["display_name"].items():
                        if lang in inserted_lang_names:
                            continue
                        entry["names"].append({"name": name, "lang": lang})
                        inserted_lang_names.append(lang)
            if sub.get("description"):
                entry["descriptions"].append(
                    {"description": sub["description"], "lang": "en"})
            if sub.get("wikidata"):
                entry["external_urls"].append(
                    {"source": "wikidata", "url": sub["wikidata"]})
            if sub.get("image_url"):
                entry["external_urls"].append(
                    {"source": "image", "url": sub["image_url"]})

            response = self.collection.insert_one(entry)
            self.inserted_concepts.append(sub["id"])
            self.inserted_concepts_ids_tuples.append(
                (response.inserted_id, sub["id"]))

    def process_relations(self):
        openalex_data = list(self.openalex_collection.find(
            {"id": {"$nin": self.relations_inserted_ids}}, {"id": 1, "ancestors": 1, "related_concepts": 1}))
        client = MongoClient(self.config["database_url"])
        Parallel(
            n_jobs=self.n_jobs,
            backend="threading",
            verbose=10
        )(delayed(process_relation)(sub, client, self.config["database_name"]) for sub in openalex_data)
        client.close()

    def run(self):
        print("Inserting the subjects")
        self.process_openalex()
        print("Creating relations")
        self.process_relations()
        return 0
