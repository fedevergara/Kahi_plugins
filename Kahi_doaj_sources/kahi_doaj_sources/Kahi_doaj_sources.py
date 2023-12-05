from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from time import time


class Kahi_doaj_sources(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]

        self.collection.create_index("external_ids.id")

        self.doaj_client = MongoClient(config["doaj_sources"]["database_url"])
        self.doaj_db = self.doaj_client[config["doaj_sources"]
                                        ["database_name"]]
        self.doaj_collection = self.doaj_db[config["doaj_sources"]
                                            ["collection_name"]]

        self.verbose = self.config["doaj_sources"]["verbose"]

        self.already_in_db = []

    def update_doaj(self, reg, entry):
        for upd in entry["updated"]:
            if upd["source"] == "doaj":
                return
        del (entry["_id"])
        entry["updated"].append({"source": "doaj", "time": int(time())})
        for name in entry["names"]:
            if name == reg["title"]:
                entry["names"].append(
                    {"lang": "en", "name": reg["title"], "source": "doaj"})
        entry["keywords"].extend(reg["keywords"])
        entry["keywords"] = list(set(entry["keywords"]))
        entry["languages"] = reg["language"] if reg["language"] != entry["languages"] else entry["languages"]
        if not entry["publisher"]:
            entry["publisher"] = {"country_code": reg["publisher"]
                                  ["country"], "name": reg["publisher"]["name"], "id": ""}
        entry["open_access_start_year"] = reg["oa_start"] if "oa_start" in reg.keys(
        ) else None
        for ref, url in reg["ref"].items():
            ref_found = False
            for ext in entry["external_urls"]:
                if ext["source"] == ref and ext["url"] == url:
                    ref_found = True
                    break
            if ref_found is False:
                entry["external_urls"].append({"source": ref, "url": url})

        entry["review_process"] = reg["editorial"]["review_process"]
        entry["plagiarism_detection"] = reg["plagiarism"]["detection"]
        entry["publication_time_weeks"] = reg["publication_time_weeks"]
        entry["copyright"] = reg["copyright"]
        entry["licenses"] = reg["license"]
        entry["waiver"] = reg["waiver"]

        if "apc" in reg.keys():
            if reg["apc"]["has_apc"]:
                entry["apc"] = {"charges": reg["apc"]["max"][-1]["price"],
                                "currency": reg["apc"]["max"][-1]["currency"]}

        subjects_source = {}
        sources = [sub["source"] for sub in entry["subjects"]]
        if "subject" in reg.keys():
            if reg["subject"]:
                for sub in reg["subject"]:
                    sub_entry = {
                        "id": "",
                        "name": sub["term"],
                        "external_ids": [{"source": sub["scheme"], "id": sub["code"]}]
                    }
                    if sub["scheme"] in subjects_source.keys():
                        subjects_source[sub["scheme"]].append(
                            sub_entry)
                    else:
                        subjects_source[sub["scheme"]] = [sub_entry]
        for source, subs in subjects_source.items():
            if source not in sources:
                entry["subjects"].append({
                    "source": source,
                    "subjects": subs
                })

        return entry

    def process_doaj(self, verbose=0):
        reg_list = list(self.doaj_collection.find())
        for i, oldreg in enumerate(reg_list):
            reg = oldreg["bibjson"]
            if "eissn" in reg.keys():
                reg_db = self.collection.find_one(
                    {"external_ids.id": reg["eissn"]})
                if reg_db:
                    _id = reg_db["_id"]
                    self.already_in_db.append(reg["eissn"])
                    entry = self.update_doaj(reg, reg_db)
                    if entry:
                        self.collection.update_one(
                            {"_id": _id}, {"$set": entry})
                    continue
            if "pissn" in reg.keys():
                reg_db = self.collection.find_one(
                    {"external_ids.id": reg["pissn"]})
                if reg_db:
                    _id = reg_db["_id"]
                    self.already_in_db.append(reg["pissn"])
                    entry = self.update_doaj(reg, reg_db)
                    if entry:
                        self.collection.update_one(
                            {"_id": _id}, {"$set": entry})
                    continue

            entry = self.empty_source()
            entry["updated"] = [{"source": "doaj", "time": int(time())}]
            entry["names"] = [
                {"lang": "en", "name": reg["title"], "source": "doaj"}]
            entry["keywords"] = reg["keywords"]
            entry["languages"] = reg["language"]
            entry["publisher"] = {"country_code": reg["publisher"]
                                  ["country"], "name": reg["publisher"]["name"], "id": ""}
            entry["open_access_start_year"] = reg["oa_start"] if "oa_start" in reg.keys(
            ) else None
            entry["external_urls"] = [
                {"source": ref, "url": url} for ref, url in reg["ref"].items()]
            entry["review_process"] = reg["editorial"]["review_process"]
            entry["plagiarism_detection"] = reg["plagiarism"]["detection"]
            entry["publication_time_weeks"] = reg["publication_time_weeks"]
            entry["copyright"] = reg["copyright"]
            entry["licenses"] = reg["license"]

            if "apc" in reg.keys():
                if reg["apc"]["has_apc"]:
                    entry["apc"] = {"charges": reg["apc"]["max"][-1]["price"],
                                    "currency": reg["apc"]["max"][-1]["currency"]}

            subjects_source = {}
            if "subject" in reg.keys():
                if reg["subject"]:
                    for sub in reg["subject"]:
                        sub_entry = {
                            "id": "",
                            "name": sub["term"],
                            "external_ids": [{"source": sub["scheme"], "id": sub["code"]}]
                        }
                        if sub["scheme"] in subjects_source.keys():
                            subjects_source[sub["scheme"]].append(
                                sub_entry)
                        else:
                            subjects_source[sub["scheme"]] = [sub_entry]
            for source, subs in subjects_source.items():
                entry["subjects"].append({
                    "source": source,
                    "subjects": subs
                })

            if "eissn" in reg.keys():
                entry["external_ids"].append(
                    {"source": "eissn", "id": reg["eissn"]})
            if "pissn" in reg.keys():
                entry["external_ids"].append(
                    {"source": "pissn", "id": reg["pissn"]})

            entry["waiver"] = reg["waiver"]

            self.collection.insert_one(entry)
            if verbose > 4:
                if i % 1000 == 0:
                    print(
                        f"""Processed  {i} of {len(reg_list)}""")
            for ext in entry["external_ids"]:
                self.already_in_db.append(ext["id"])

        if verbose >= 4:
            print(
                f"""Inserted {self.collection.count_documents({"updated.source":"doaj"})} sources""")

        self.client.close()

    def run(self):
        self.process_doaj(verbose=self.verbose)
        return 0
