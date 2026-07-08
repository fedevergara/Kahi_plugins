from kahi.KahiBase import KahiBase
from pymongo import MongoClient, InsertOne, UpdateOne, ASCENDING
from time import time
from langid import classify
from kahi_impactu_utils.Utils import check_date_format
import re


class Kahi_scienti_sources(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]

        self.collection.create_index("external_ids.id")

        self.already_in_db = []
        # checking if the databases and collections are available
        self.check_databases_and_collections()
        # creating indexes for the scienti sources
        self.create_source_indexes()

    def check_databases_and_collections(self):
        for db_info in self.config["scienti_sources"]["databases"]:
            client = MongoClient(db_info["database_url"])
            if db_info['database_name'] not in client.list_database_names():
                raise Exception("Database {} not found".format(
                    db_info['database_name']))
            if db_info['collection_name'] not in client[db_info['database_name']].list_collection_names():
                raise Exception("Collection {}.{} not found".format(db_info['database_name'],
                                                                    db_info['collection_name']))
            client.close()

    def create_source_indexes(self):
        for db_info in self.config["scienti_sources"]["databases"]:
            database_url = db_info.get('database_url', '')
            database_name = db_info.get('database_name', '')
            collection_name = db_info.get('collection_name', '')

            if database_url and database_name and collection_name:
                client = MongoClient(database_url)
                db = client[database_name]
                collection = db[collection_name]

                collection.create_index(
                    [("details.article.journal_others.TXT_ISSN", ASCENDING)])
                collection.create_index(
                    [("details.article.journal.TXT_ISSN_SEP", ASCENDING)])
                collection.create_index(
                    [("details.article.journal.TXT_ISSN", ASCENDING)])
                client.close()

    def _normalize_issn(self, value):
        if value is None:
            return ""
        text = str(value).strip().upper()
        text = text.replace("-", "").replace(" ", "")
        if len(text) != 8:
            return ""
        if not re.match(r"^[0-9]{7}[0-9X]$", text):
            return ""
        return f"{text[:4]}-{text[4:]}"

    def _issn_values(self, value):
        normalized = self._normalize_issn(value)
        if not normalized:
            return []
        return [normalized, normalized.replace("-", "")]

    def _journal_query_for_issn(self, issn):
        values = self._issn_values(issn)
        return {
            "$or": [
                {"details.article.journal.TXT_ISSN_SEP": {"$in": values}},
                {"details.article.journal.TXT_ISSN": {"$in": values}},
                {"details.article.journal_others.TXT_ISSN": {"$in": values}},
            ]
        }

    def _find_source_by_issn(self, issn):
        values = self._issn_values(issn)
        if not values:
            return None
        return self.collection.find_one({"external_ids.id": {"$in": values}})

    def _append_external_id(self, entry, source, identifier):
        identifier = str(identifier)
        for ext in entry["external_ids"]:
            if ext.get("source") == source and str(ext.get("id")) == identifier:
                return
        entry["external_ids"].append({"source": source, "id": identifier})

    def process_editorials(self, editorials_list):
        unique_codes = list(set(editorials_list))
        # Capture current timestamp once for use in all operations
        now = int(time())

        # Query all documents whose external_ids.id is in our unique_codes list
        existing_docs = list(self.collection.find(
            {"external_ids.id": {"$in": unique_codes}},
            {"external_ids.id": 1}
        ))
        # Build a set of codes already present in the DB for O(1) membership checks
        existing_ids = {doc["external_ids"][0]["id"] for doc in existing_docs}

        # Query scienti_collection for details matching any of our editorial codes
        scienti_docs = list(self.scienti_collection.find(
            {"details.book.editorial.COD_EDITORIAL": {"$in": unique_codes}},
            {"details": 1}
        ))
        # Map each editorial code to its corresponding scienti document for quick lookup
        scienti_map = {}
        for doc in scienti_docs:
            details = doc.get("details", [])
            if details:
                # Drill down to book.editorial arrays
                entry = details[0]
                for book in entry.get("book", []):
                    for ed in book.get("editorial", []):
                        code = ed.get("COD_EDITORIAL")
                        if code:
                            scienti_map[code] = doc

        ops = []
        for code in unique_codes:
            if code in existing_ids:
                # If the editorial code exists, prepare an UpdateOne to add new sources
                ops.append(UpdateOne(
                    {"external_ids.id": str(code)},
                    {"$addToSet": {
                        "updated": {"source": "scienti", "time": now},
                        "external_ids": {"source": "scienti", "id": str(code)}
                    }}
                ))
            else:
                # If the editorial code does not exist, prepare an InsertOne
                reg = scienti_map.get(code)
                if not reg:
                    continue
                # Initialize a new document using the empty template
                entry = self.empty_source()
                entry["updated"] = [{"source": "scienti", "time": now}]
                # Extract editorial name from nested details structure
                details = reg.get("details", [])
                editorial_list = (
                    details[0].get("book", [])[0]
                    .get("editorial", [])
                ) if details else []
                # Clean and title-case the editorial text name, defaulting to empty string
                name = editorial_list[0].get(
                    "TXT_NME_EDITORIAL", "").title().strip() if editorial_list else ""
                entry["names"] = [
                    {"lang": "es", "name": name, "source": "scienti"}]
                # Set the primary external ID for the editorial
                entry["external_ids"] = [
                    {"source": "scienti", "id": str(code)}]
                # Queue an insert operation
                ops.append(InsertOne(entry))

        if ops:
            # Execute all operations in bulk
            self.collection.bulk_write(ops, ordered=False)

    def update_scienti(self, reg, entry, issn):
        updated_scienti = False
        for upd in entry["updated"]:
            if upd["source"] == "scienti":
                updated_scienti = True
                entry["updated"].remove(upd)
                entry["updated"].append(
                    {"source": "scienti", "time": int(time())})
                break
        if not updated_scienti:
            entry["updated"].append({"source": "scienti", "time": int(time())})
        journal = None
        for detail in reg["details"]:
            if "article" in detail.keys():
                paper = detail["article"][0]
                if "journal" in paper.keys():
                    journal = paper["journal"][0]
                    break
        if not journal:
            return
        if "TPO_REVISTA" in journal.keys():
            entry["types"].append(
                {"source": "scienti", "type": journal["TPO_REVISTA"]})
        self._append_external_id(entry, "scienti", journal["COD_REVISTA"])

        rankings_list = []
        ranks = []
        dates = [(rank["from_date"], rank["to_date"])
                 for rank in entry["ranking"] if rank["source"] == "scienti"]
        for reg_scienti in self.scienti_collection.find(self._journal_query_for_issn(issn)):
            paper = None
            journal = None
            if "details" not in reg_scienti.keys():
                continue
            for detail in reg_scienti["details"]:
                if "article" in detail.keys():
                    paper = detail["article"][0]
                    if "journal" in paper.keys():
                        journal = paper["journal"][0]
                        break

            if journal is None:
                continue
            if "TPO_CLASIFICACION" not in journal.keys():
                continue
            DTA_CREACION = check_date_format(paper["DTA_CREACION"])
            if not journal["TPO_CLASIFICACION"] in ranks:
                ranking = {
                    "from_date": DTA_CREACION,
                    "to_date": DTA_CREACION,
                    "rank": journal["TPO_CLASIFICACION"],
                    "issn": issn,
                    "order": None,
                    "source": "scienti"
                }
                rankings_list.append(ranking)
                ranks.append(journal["TPO_CLASIFICACION"])
                dates_tuple = (DTA_CREACION, DTA_CREACION)

                dates.append(dates_tuple)
            else:
                idx = ranks.index(journal["TPO_CLASIFICACION"])
                date1, date2 = dates[idx]
                if isinstance(date1, (int, float)) and isinstance(date2, (int, float)):
                    if date1 > DTA_CREACION:
                        date1 = DTA_CREACION
                    if date2 < DTA_CREACION:
                        date2 = DTA_CREACION
                    dates[idx] = (date1, date2)

        self.collection.update_one({"_id": entry["_id"]}, {"$set": {
            "types": entry["types"],
            "external_ids": entry["external_ids"],
            "updated": entry["updated"],
            "ranking": entry["ranking"] + rankings_list
        }})

    def process_scienti(self, config, verbose=0):
        self.scienti_client = MongoClient(config["database_url"])

        self.scienti_db = self.scienti_client[config["database_name"]]

        self.scienti_collection = self.scienti_db[config["collection_name"]]
        issn_list = list(self.scienti_collection.distinct(
            "details.article.journal.TXT_ISSN_SEP"))
        issn_list.extend(self.scienti_collection.distinct(
            "details.article.journal.TXT_ISSN"))
        issn_list.extend(self.scienti_collection.distinct(
            "details.article.journal_others.TXT_ISSN"))
        issn_list = sorted(
            {
                self._normalize_issn(issn)
                for issn in issn_list
                if self._normalize_issn(issn)
            }
        )
        # extracting the editorial codes
        editorials_list = list(self.scienti_collection.distinct(
            "details.book.editorial.COD_EDITORIAL"))
        editorials_list = [str(code) for code in editorials_list]

        for issn in issn_list:
            reg_db = self._find_source_by_issn(issn)
            if reg_db:
                reg_scienti = self.scienti_collection.find_one(
                    self._journal_query_for_issn(issn))
                if reg_scienti:
                    self.update_scienti(reg_scienti, reg_db, issn)
            else:
                reg_scienti = self.scienti_collection.find_one(
                    self._journal_query_for_issn(issn))
                if reg_scienti:
                    journal = None
                    if "details" not in reg_scienti.keys():
                        continue
                    for detail in reg_scienti["details"]:
                        if "article" in detail.keys():
                            paper = detail["article"][0]
                            if "journal" in paper.keys():
                                journal = paper["journal"][0]
                                break
                            elif "journal_others" in paper.keys():
                                journal = paper["journal_others"][0]
                                break
                    if not journal:
                        continue
                    entry = self.empty_source()
                    entry["updated"] = [
                        {"source": "scienti", "time": int(time())}]
                    lang = classify(journal["TXT_NME_REVISTA"])[0]
                    entry["names"] = [
                        {"lang": lang, "name": journal["TXT_NME_REVISTA"], "source": "scienti"}]
                    entry["external_ids"].append({"source": "issn", "id": issn})
                    self._append_external_id(entry, "scienti", journal["COD_REVISTA"])
                    if "TPO_REVISTA" in journal.keys():
                        entry["types"].append(
                            {"source": "scienti", "type": journal["TPO_REVISTA"]})
                    if "editorial" in journal.keys():
                        entry["publisher"] = {
                            "country_code": "", "name": journal["editorial"][0]["TXT_NME_EDITORIAL"]}
                    rankings_list = []
                    ranks = []
                    dates = []
                    for reg_scienti in self.scienti_collection.find(self._journal_query_for_issn(issn)):
                        paper = None
                        journal = None
                        if "details" not in reg_scienti.keys():
                            continue
                        for detail in reg_scienti["details"]:
                            if "article" in detail.keys():
                                paper = detail["article"][0]
                                if "journal" in paper.keys():
                                    journal = paper["journal"][0]
                                    break
                        if journal:
                            if "TPO_CLASIFICACION" not in journal.keys():
                                continue
                            DTA_CREACION = check_date_format(
                                paper["DTA_CREACION"])

                            if not journal["TPO_CLASIFICACION"] in ranks:
                                from_date = DTA_CREACION
                                to_date = DTA_CREACION
                                ranking = {
                                    "from_date": from_date,
                                    "to_date": to_date,
                                    "rank": journal["TPO_CLASIFICACION"],
                                    "issn": issn,
                                    "order": None,
                                    "source": "scienti"
                                }
                                rankings_list.append(ranking)
                                ranks.append(journal["TPO_CLASIFICACION"])
                                dates_tuple = (from_date, to_date)
                                dates.append(dates_tuple)
                            else:
                                # if is already ranked but dates changed
                                idx = ranks.index(journal["TPO_CLASIFICACION"])
                                date1, date2 = dates[idx]
                                if isinstance(date1, (int, float)) and isinstance(date2, (int, float)):
                                    if date1 > DTA_CREACION:
                                        date1 = DTA_CREACION
                                    if date2 < DTA_CREACION:
                                        date2 = DTA_CREACION
                                    dates[idx] = (date1, date2)
                    entry["ranking"] = rankings_list
                    self.collection.insert_one(entry)

        if editorials_list:
            self.process_editorials(editorials_list)

        self.scienti_client.close()

    def run(self):
        start_time = time()
        for config in self.config["scienti_sources"]["databases"]:
            print("Processing {}.{} database".format(
                config["database_name"], config["collection_name"]))
            self.process_scienti(config, verbose=5)
        print("Execution time: {} minutes".format(
            round((time() - start_time) / 60, 2)))
        return 0
