from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from kahi_impactu_utils.Utils import get_id_from_url, split_names
from re import sub


def process_one_insert(person_db, oa_author, client, db_name, empty_person, related_works):
    """"
    Insert a new OpenAlex author record into the database.

    Parameters:
    person_db (dict):
        The existing OpenAlex author record in the database.
    oa_author (dict):
        The new OpenAlex author record to insert into the database.
    client (MongoClient):
        The MongoDB client to use for database operations.
    db_name (str):
        The name of the database to use.
    empty_person (dict):
        A template for an empty person document.
    related_works (list):
        A list of related works to associate with the person.
    max_tries (int):
        The maximum number of attempts to insert the record into the database.
    """
    db = client[db_name]
    collection = db["person"]

    entry = empty_person.copy()
    entry["updated"].append({"source": "openalex", "time": int(time())})

    entry["full_name"] = sub(
        r'\s+', ' ', oa_author["display_name"].replace(".", " ")).strip()
    author_data = split_names(entry["full_name"])
    entry["last_names"] = author_data["last_names"]
    entry["first_names"] = author_data["first_names"]
    entry["initials"] = author_data["initials"]
    for name in oa_author["display_name_alternatives"]:
        if not name.lower() in entry["aliases"]:
            entry["aliases"].append(name.lower())
    for source, idx in oa_author["ids"].items():
        idx = get_id_from_url(idx)
        if idx:
            entry["external_ids"].append(
                {"provenance": "openalex", "source": source, "id": idx})

    if "last_known_institutions" in oa_author.keys():
        if oa_author["last_known_institutions"]:
            for inst in oa_author["last_known_institutions"]:
                aff_reg = None
                aff_reg = db["affiliations"].find_one(
                    {"external_ids.id": inst["id"]})
                if not aff_reg:
                    if "ror" in inst.keys():
                        aff_reg = db["affiliations"].find_one(
                            {"external_ids.id": inst["ror"]})
                if aff_reg:
                    name = aff_reg["names"][0]["name"]
                    for n in aff_reg["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    entry["affiliations"].append({
                        "id": aff_reg["_id"],
                        "name": name,
                        "types": aff_reg["types"],
                        "start_date": -1,
                        "end_date": -1
                    })
    elif "last_known_institution" in oa_author.keys():
        if oa_author["last_known_institution"]:
            aff_reg = None
            for source, idx in oa_author["last_known_institution"].items():
                aff_reg = db["affiliations"].find_one(
                    {"external_ids.id": idx})
                if aff_reg:
                    break
            if aff_reg:
                name = aff_reg["names"][0]["name"]
                for n in aff_reg["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    elif n["lang"] == "en":
                        name = n["name"]
                entry["affiliations"].append({
                    "id": aff_reg["_id"],
                    "name": name,
                    "types": aff_reg["types"],
                    "start_date": -1,
                    "end_date": -1
                })

    for rwork in related_works:
        for key in rwork["ids"].keys():
            if key == "doi":
                rec = {"provenance": "openalex",
                       "source": key, "id": rwork["ids"][key], "year": rwork["publication_year"], "institutions": rwork["authorships"]["institutions"]}
                if rec not in entry["related_works"]:
                    entry["related_works"].append(rec)
                break
    collection.insert_one(entry)


def process_one_update(person_db, oa_author, client, db_name, empty_person, related_works):
    """"
    Update an existing OpenAlex author record in the database with new information.

    Parameters:
    person_db (dict):
        The existing OpenAlex author record in the database.
    oa_author (dict):
        The new OpenAlex author record to update the existing record with.
    client (MongoClient):
        The MongoDB client to use for database operations.
    db_name (str):
        The name of the database to use.
    empty_person (dict):
        A template for an empty person document.
    related_works (list):
        A list of related works to associate with the person.
    """
    db = client[db_name]
    collection = db["person"]

    entry = empty_person.copy()
    entry["updated"] = person_db.get("updated", [])
    entry["updated"].append({
        "source": "openalex",
        "time": int(time())
    })

    # Iterate over OpenAlex IDs and add any that are not already in the document
    new_external_ids = []
    for source, url in oa_author.get("ids", {}).items():
        idx = get_id_from_url(url)
        if idx:
            record = {"provenance": "openalex", "source": source, "id": idx}
            if record not in person_db.get("external_ids", []):
                new_external_ids.append(record)
    entry["external_ids"] = person_db.get(
        "external_ids", []) + new_external_ids

    if "last_known_institutions" in oa_author.keys():
        if oa_author["last_known_institutions"]:
            for inst in oa_author["last_known_institutions"]:
                aff_reg = None
                aff_reg = db["affiliations"].find_one(
                    {"external_ids.id": inst["id"]})
                if not aff_reg:
                    if "ror" in inst.keys():
                        aff_reg = db["affiliations"].find_one(
                            {"external_ids.id": inst["ror"]})
                if aff_reg:
                    name = aff_reg["names"][0]["name"]
                    for n in aff_reg["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    entry["affiliations"].append({
                        "id": aff_reg["_id"],
                        "name": name,
                        "types": aff_reg["types"],
                        "start_date": -1,
                        "end_date": -1
                    })

    elif "last_known_institution" in oa_author.keys():
        if oa_author["last_known_institution"]:
            aff_reg = None
            for source, idx in oa_author["last_known_institution"].items():
                aff_reg = db["affiliations"].find_one(
                    {"external_ids.id": idx})
                if aff_reg:
                    break
            if aff_reg:
                name = aff_reg["names"][0]["name"]
                for n in aff_reg["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    elif n["lang"] == "en":
                        name = n["name"]
                entry["affiliations"].append({
                    "id": aff_reg["_id"],
                    "name": name,
                    "types": aff_reg["types"],
                    "start_date": -1,
                    "end_date": -1
                })

    for rwork in related_works:
        for key in rwork["ids"].keys():
            if key == "doi":
                rec = {"provenance": "openalex",
                       "source": key, "id": rwork["ids"][key], "year": rwork["publication_year"], "institutions": rwork["authorships"]["institutions"]}
                if rec not in entry["related_works"]:
                    entry["related_works"].append(rec)
                break

    # Update the person document in the database
    collection.update_one(
        {"_id": person_db["_id"]},
        {
            "$addToSet": {
                "updated": {"$each": entry["updated"]},
                "external_ids": {"$each": entry["external_ids"]},
                "affiliations": {"$each": entry.get("affiliations", [])},
                "related_works": {"$each": entry.get("related_works", [])}
            }
        }
    )


def process_one(oa_author, client, db_name, empty_person, related_works):
    """"
    Process a single OpenAlex record to extract personal details and either update or insert the corresponding document
    in the 'person' collection based on whether the record exists.

    Parameters:
    oa_author (dict):
        The OpenAlex author record to process.
    client (MongoClient):
        The MongoDB client to use for database operations.
    db_name (str):
        The name of the database to use.
    empty_person (dict):
        A template for an empty person document.
    related_works (list):
        A list of related works to associate with the person.
    """
    db = client[db_name]
    collection = db["person"]

    person_db = None
    for source, idx in oa_author["ids"].items():
        idx = get_id_from_url(idx)
        if idx:
            person_db = collection.find_one({"external_ids.id": idx})
            if person_db:
                break
    if person_db:
        return process_one_update(
            person_db, oa_author, client, db_name, empty_person, related_works)
    else:
        return process_one_insert(
            person_db, oa_author, client, db_name, empty_person, related_works)


class Kahi_openalex_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.openalex_client = MongoClient(
            config["openalex_person"]["database_url"])
        if config["openalex_person"]["database_name"] not in self.openalex_client.list_database_names():
            raise Exception("Database {} not found in {}".format(
                config["openalex_person"]['database_name'], config["openalex_person"]["database_url"]))
        self.openalex_db = self.openalex_client[config["openalex_person"]
                                                ["database_name"]]
        if config["openalex_person"]["collection_name"] not in self.openalex_db.list_collection_names():
            raise Exception("Collection {}.{} not found in {}".format(config["openalex_person"]['database_name'],
                                                                      config["openalex_person"]['collection_name'], config["openalex_person"]["database_url"]))
        self.openalex_collection = self.openalex_db[config["openalex_person"]
                                                    ["collection_name"]]
        self.openalex_collection_works = self.openalex_db[config["openalex_person"]
                                                          ["collection_name_works"]]
        self.openalex_collection_works.create_index("authorships.author.id")

        self.n_jobs = config["openalex_person"]["num_jobs"] if "num_jobs" in config["openalex_person"].keys(
        ) else 1
        self.verbose = config["openalex_person"]["verbose"] if "verbose" in config["openalex_person"].keys(
        ) else 0

        self.client.close()

    def process_openalex(self):
        author_cursor = self.openalex_collection.find(no_cursor_timeout=True)
        client = MongoClient(self.mongodb_url)
        Parallel(
            n_jobs=self.n_jobs,
            verbose=self.verbose,
            backend="threading")(
            delayed(process_one)(
                author,
                client,
                self.config["database_name"],
                self.empty_person(),
                list(self.openalex_collection_works.aggregate(
                    # related works
                    [{"$project": {"ids": 1, "publication_year": 1, "authorships": 1}},
                     {"$match": {"authorships.author.id": author["id"]}},
                     {"$unwind": "$authorships"},
                     {"$match": {"authorships.author.id": author["id"]}},
                     {"$project": {"ids": 1, "publication_year": 1, "authorships.institutions": 1}}]))
            ) for author in author_cursor
        )
        client.close()

    def run(self):
        self.process_openalex()
        return 0
