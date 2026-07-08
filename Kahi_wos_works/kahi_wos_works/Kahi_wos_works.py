from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from time import time
from joblib import Parallel, delayed
from thefuzz import fuzz

from kahi_impactu_utils.Utils import doi_processor, lang_poll


def parse_wos(reg, empty_work, verbose=0):
    entry = empty_work.copy()
    entry["updated"] = [{"source": "wos", "time": int(time())}]
    if "TI" in reg.keys():
        lang = lang_poll(reg["TI"])
        entry["titles"].append(
            {"title": reg["TI"], "lang": lang, "source": "wos"})
    if "AB" in reg.keys():
        if reg["AB"] and reg["AB"] == reg["AB"]:
            entry["abstract"] = reg["AB"].strip()
    if "DT" in reg.keys():
        if reg["DT"] and reg["DT"] == reg["DT"]:
            entry["types"].append(
                {"source": "wos", "type": reg["DT"].strip().lower()})
    if "PY" in reg.keys():
        if reg["PY"] and reg["PY"] == reg["PY"]:
            entry["year_published"] = int(reg["PY"].strip())
    if "BP" in reg.keys():
        if reg["BP"] and reg["BP"] == reg["BP"]:
            entry["bibliographic_info"]["start_page"] = reg["BP"]
    if "EP" in reg.keys():
        if reg["EP"] and reg["EP"] == reg["EP"]:
            entry["bibliographic_info"]["end_page"] = reg["EP"]
    if "VL" in reg.keys():
        if reg["VL"] and reg["VL"] == reg["VL"]:
            entry["bibliographic_info"]["volume"] = reg["VL"].strip()
    if "IS" in reg.keys():
        if reg["IS"] and reg["IS"] == reg["IS"]:
            entry["bibliographic_info"]["issue"] = reg["IS"].strip()

    count = None
    if "Z9" in reg.keys():
        if reg["Z9"] and reg["Z9"] == reg["Z9"]:
            try:
                count = int(reg["Z9"].replace("\n", ""))
            except Exception as e:
                if verbose > 4:
                    print(
                        f"""Variable Z9 (citations) could not be converted to integer for {reg["DI"]}""")
                    print(e)
                count = None
            entry["citations_count"].append({"source": "wos", "count": count})

    if "DI" in reg.keys():
        if reg["DI"]:
            ext = {"source": "doi", "id": doi_processor(reg["DI"])}
            entry["external_ids"].append(ext)
    if "UT" in reg.keys():
        if reg["UT"]:
            ext = {"source": "wos", "id": reg["UT"].strip().split(":")[1]}
            entry["external_ids"].append(ext)

    source = {"external_ids": []}
    if "SO" in reg.keys():
        if reg["SO"]:
            source["name"] = reg["SO"].rstrip()
    if "SN" in reg.keys():
        if reg["SN"]:
            source["external_ids"].append(
                {"source": "pissn", "id": reg["SN"].rstrip()})
    if "EI" in reg.keys():
        if reg["EI"]:
            source["external_ids"].append(
                {"source": "eissn", "id": reg["EI"].rstrip()})
    if "BN" in reg.keys():
        if reg["BN"]:
            source["external_ids"].append(
                {"source": "isbn", "id": reg["BN"].rstrip()})
    entry["source"] = source

    # authors_section
    if "C1" in reg.keys():
        if reg["C1"]:
            orcid_list = []
            researcherid_list = []
            if "RI" in reg.keys():
                if reg["RI"] and reg["RI"] == reg["RI"]:
                    reg["RI"] = reg["RI"].strip()
                    if reg["RI"][0] == ";":
                        reg["RI"] = reg["RI"][1:]
                    researcherid_list = reg["RI"].replace(
                        "\n", "").rstrip().replace("; ", ";").split(";")
            if "OI" in reg.keys():
                if reg["OI"] and reg["OI"] == reg["OI"]:
                    orcid_list = reg["OI"].rstrip().replace(
                        "; ", ";").split(";")
            for auwaf in reg["C1"].strip().replace(".", "").split("\n"):
                aulen = len(auwaf.split(";"))
                if aulen == 1:
                    auaff = auwaf.split("] ")
                    if len(auaff) == 1:
                        aff = auwaf
                        authors = [""]
                        if "AF" in reg.keys():
                            if len(reg["AF"].rstrip().split("\n")) == 1:
                                authors = reg["AF"].rstrip()
                    else:
                        aff = auaff[1]
                        authors = [auaff[0][1:]]
                else:
                    aff = auwaf.split("] ")[1]
                    authors = auwaf.split("] ")[0][1:].split("; ")
                try:
                    instname = "".join(aff.split(", ")[0])
                except Exception as e:
                    if verbose > 4:
                        print(
                            f"""Institution name could not be extracted for {reg["DI"]}""")
                        print(e)
                    instname = ""
                for author in authors:
                    entry_ext = []
                    for res in researcherid_list:
                        try:
                            name, rid = res.split("/")[-2:]
                        except Exception as e:
                            print(
                                f"""{reg["DI"]} does not provide researcherid """)
                            print(e)
                        ratio = fuzz.partial_ratio(name, author)
                        if ratio > 90:
                            entry_ext.append(
                                {"source": "researcherid", "id": rid})
                            break
                        elif ratio > 50:
                            ratio = fuzz.token_set_ratio(name, author)
                            if ratio > 90:
                                entry_ext.append(
                                    {"source": "researcherid", "id": rid})
                                break
                            elif ratio > 50:
                                ratio = fuzz.partial_token_set_ratio(
                                    name, author)
                                if ratio > 95:
                                    entry_ext.append(
                                        {"source": "researcherid", "id": rid})
                                    break
                    for res in orcid_list:
                        try:
                            name, rid = res.split("/")[-2:]
                        except Exception as e:
                            print(f"""{reg["DI"]} does not provide orcid""")
                            print(e)
                        ratio = fuzz.partial_ratio(name, author)
                        if ratio > 90:
                            entry_ext.append({"source": "orcid", "id": rid})
                            break
                        elif ratio > 50:
                            ratio = fuzz.token_set_ratio(name, author)
                            if ratio > 90:
                                entry_ext.append(
                                    {"source": "orcid", "id": rid})
                                break
                            elif ratio > 50:
                                ratio = fuzz.partial_token_set_ratio(
                                    name, author)
                                if ratio > 95:
                                    entry_ext.append(
                                        {"source": "orcid", "id": rid})
                                    break
                    author_entry = {
                        "full_name": author,
                        "types": [],
                        "external_ids": entry_ext,
                        "affiliations": [{
                            "name": instname
                        }]
                    }
                    entry["authors"].append(author_entry)

    return entry


def process_one(wos_reg, db, collection, empty_work, verbose=0):
    doi = None
    # register has doi
    if wos_reg["DI"]:
        if isinstance(wos_reg["DI"], str):
            doi = doi_processor(wos_reg["DI"])
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update the register
            # updated
            for upd in colav_reg["updated"]:
                if upd["source"] == "wos":
                    # client.close()
                    return None  # Register already on db
                    # Could be updated with new information when wos database
                    # changes
            entry = parse_wos(
                wos_reg, empty_work.copy(), verbose=verbose)
            colav_reg["updated"].append(
                {"source": "wos", "time": int(time())})
            # titles
            colav_reg["titles"].extend(entry["titles"])
            # external_ids
            ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
            for ext in entry["external_ids"]:
                if ext["id"] not in ext_ids:
                    colav_reg["external_ids"].append(ext)
                    ext_ids.append(ext["id"])
            # types
            colav_reg["types"].extend(entry["types"])
            # bibliographic info
            if "is_open_acess" not in colav_reg["bibliographic_info"].keys():
                if "is_open_access" in entry["bibliographic_info"].keys():
                    colav_reg["bibliographic_info"]["is_open_acess"] = entry["bibliographic_info"]["is_open_access"]
            if "open_access_status" not in colav_reg["bibliographic_info"].keys(
            ):
                if "open_access_status" in entry["bibliographic_info"].keys():
                    colav_reg["bibliographic_info"]["open_access_status"] = entry["bibliographic_info"]["open_access_status"]
            if "start_page" not in colav_reg["bibliographic_info"].keys():
                if "start_page" in entry["bibliographic_info"].keys():
                    colav_reg["bibliographic_info"]["start_page"] = entry["bibliographic_info"]["start_page"]
            if "end_page" not in colav_reg["bibliographic_info"].keys():
                if "end_page" in entry["bibliographic_info"].keys():
                    colav_reg["bibliographic_info"]["end_page"] = entry["bibliographic_info"]["end_page"]
            if "volume" not in colav_reg["bibliographic_info"].keys():
                if "volume" in entry["bibliographic_info"].keys():
                    colav_reg["bibliographic_info"]["volume"] = entry["bibliographic_info"]["volume"]
            if "issue" not in colav_reg["bibliographic_info"].keys():
                if "issue" in entry["bibliographic_info"].keys():
                    colav_reg["bibliographic_info"]["issue"] = entry["bibliographic_info"]["issue"]

            # external urls
            urls_sources = [url["source"]
                            for url in colav_reg["external_urls"]]
            for ext in entry["external_urls"]:
                if ext["url"] not in urls_sources:
                    colav_reg["external_urls"].append(ext)
                    urls_sources.append(ext["url"])

            # citations count
            if entry["citations_count"]:
                colav_reg["citations_count"].extend(entry["citations_count"])

            collection.update_one(
                {"_id": colav_reg["_id"]},
                {"$set": {
                    "updated": colav_reg["updated"],
                    "titles": colav_reg["titles"],
                    "external_ids": colav_reg["external_ids"],
                    "types": colav_reg["types"],
                    "bibliographic_info": colav_reg["bibliographic_info"],
                    "external_urls": colav_reg["external_urls"],
                    "citations_count": colav_reg["citations_count"],
                    "citations_by_year": colav_reg["citations_by_year"]
                }}
            )
        else:  # insert a new register
            # parse
            entry = parse_wos(
                wos_reg, empty_work.copy(), verbose=verbose)
            # link
            source_db = None
            if "external_ids" in entry["source"].keys():
                for ext in entry["source"]["external_ids"]:
                    source_db = db["sources"].find_one(
                        {"external_ids.id": ext["id"]})
                    if source_db:
                        break
            if source_db:
                name = source_db["names"][0]["name"]
                for n in source_db["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    if n["lang"] == "en":
                        name = n["name"]
                entry["source"] = {
                    "id": source_db["_id"],
                    "name": name
                }
            else:
                if len(entry["source"]["external_ids"]) == 0:
                    if verbose > 4:
                        print(
                            f'Register with doi: {wos_reg["DI"]} does not provide a source')
                else:
                    if verbose > 4:
                        print("No source found for\n\t",
                              entry["source"]["external_ids"])
                entry["source"] = {
                    "id": "",
                    "name": entry["source"]["name"]
                }

            # search authors and affiliations in db
            for i, author in enumerate(entry["authors"]):
                author_db = None
                for ext in author["external_ids"]:
                    author_db = db["person"].find_one(
                        {"external_ids.id": ext["id"]})
                    if author_db:
                        break
                if author_db:
                    sources = [ext["source"]
                               for ext in author_db["external_ids"]]
                    ids = [ext["id"] for ext in author_db["external_ids"]]
                    for ext in author["external_ids"]:
                        if ext["id"] not in ids:
                            author_db["external_ids"].append(ext)
                            sources.append(ext["source"])
                            ids.append(ext["id"])
                    entry["authors"][i] = {
                        "id": author_db["_id"],
                        "full_name": author_db["full_name"],
                        "affiliations": author["affiliations"]
                    }
                    if "external_ids" in author.keys():
                        del (author["external_ids"])
                else:
                    author_db = db["person"].find_one(
                        {"full_name": author["full_name"]})
                    if author_db:
                        sources = [ext["source"]
                                   for ext in author_db["external_ids"]]
                        ids = [ext["id"] for ext in author_db["external_ids"]]
                        for ext in author["external_ids"]:
                            if ext["id"] not in ids:
                                author_db["external_ids"].append(ext)
                                sources.append(ext["source"])
                                ids.append(ext["id"])
                        entry["authors"][i] = {
                            "id": author_db["_id"],
                            "full_name": author_db["full_name"],
                            "affiliations": author["affiliations"]
                        }
                    else:
                        entry["authors"][i] = {
                            "id": "",
                            "full_name": author["full_name"],
                            "affiliations": author["affiliations"]
                        }
                for j, aff in enumerate(author["affiliations"]):
                    aff_db = None
                    if "external_ids" in aff.keys():
                        for ext in aff["external_ids"]:
                            aff_db = db["affiliations"].find_one(
                                {"external_ids.id": ext["id"]})
                            if aff_db:
                                break
                    if aff_db:
                        name = aff_db["names"][0]["name"]
                        for n in aff_db["names"]:
                            if n["source"] == "ror":
                                name = n["name"]
                                break
                            if n["lang"] == "en":
                                name = n["name"]
                            if n["lang"] == "es":
                                name = n["name"]
                        entry["authors"][i]["affiliations"][j] = {
                            "id": aff_db["_id"],
                            "name": name,
                            "types": aff_db["types"]
                        }
                    else:
                        aff_db = db["affiliations"].find_one(
                            {"names.name": aff["name"]})
                        if aff_db:
                            name = aff_db["names"][0]["name"]
                            for n in aff_db["names"]:
                                if n["source"] == "ror":
                                    name = n["name"]
                                    break
                                if n["lang"] == "en":
                                    name = n["name"]
                                if n["lang"] == "es":
                                    name = n["name"]
                            entry["authors"][i]["affiliations"][j] = {
                                "id": aff_db["_id"],
                                "name": name,
                                "types": aff_db["types"]
                            }
                        else:
                            entry["authors"][i]["affiliations"][j] = {
                                "id": "",
                                "name": aff["name"],
                                "types": []
                            }

            entry["author_count"] = len(entry["authors"])
            # insert in mongo
            collection.insert_one(entry)
            # insert in elasticsearch
    else:  # does not have a doi identifier
        # elasticsearch section
        pass


class Kahi_wos_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.collection.create_index("year_published")
        self.collection.create_index("authors.affiliations.id")
        self.collection.create_index("authors.id")
        self.collection.create_index([("titles.title", TEXT)])

        self.wos_client = MongoClient(
            config["wos_works"]["database_url"])
        if config["wos_works"]["database_name"] not in self.wos_client.list_database_names():
            raise Exception(
                f"Database {config['wos_works']['database_name']} found not in "
                f"{config['wos_works']['database_url']}")
        self.wos_db = self.wos_client[config["wos_works"]
                                            ["database_name"]]
        if config["wos_works"]["collection_name"] not in self.wos_db.list_collection_names():
            raise Exception(
                f"Collection {config['wos_works']['database_name']}."
                f"{config['wos_works']['collection_name']} found not in "
                f"{config['wos_works']['database_url']}")
        self.wos_collection = self.wos_db[config["wos_works"]
                                                ["collection_name"]]

        self.n_jobs = config["wos_works"]["num_jobs"] if "num_jobs" in config["wos_works"].keys(
        ) else 1
        self.verbose = config["wos_works"]["verbose"] if "verbose" in config["wos_works"].keys(
        ) else 0

    def process_wos(self):
        paper_list = list(self.wos_collection.find())

        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["works"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(process_one)(
                    paper,
                    db,
                    collection,
                    self.empty_work(),
                    verbose=self.verbose
                ) for paper in paper_list
            )

    def run(self):
        self.process_wos()
        return 0
