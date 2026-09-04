from kahi.KahiBase import KahiBase
from pymongo import MongoClient, ASCENDING, TEXT
from time import time
from thefuzz import fuzz
from kahi_impactu_utils.Utils import check_date_format


def normalize_scienti_rank(value):
    """Translate the Scienti code for recognized groups."""
    return "Reconocido" if value == "00" else value


class Kahi_scienti_affiliations(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("names.name")
        self.collection.create_index("types.type")
        self.collection.create_index([("names.name", TEXT)])

        self.verbose = config["scienti_affiliations"]["verbose"] if "verbose" in config["scienti_affiliations"].keys(
        ) else 0

        name_index = False
        for key, val in self.collection.index_information().items():
            if key == "names.name_text":
                name_index = True
                break
        if not name_index:
            self.collection.create_index([("names.name", TEXT)])
            print("Text index created on names.name field")

        # checking if the databases and collections are available
        self.check_databases_and_collections()
        # creating indexes for the scienti sources
        self.create_source_indexes()

    def check_databases_and_collections(self):
        for db_info in self.config["scienti_affiliations"]["databases"]:
            client = MongoClient(db_info["database_url"])
            if db_info['database_name'] not in client.list_database_names():
                raise Exception("Database {} not found".format(
                    db_info['database_name']))
            if db_info['collection_name'] not in client[db_info['database_name']].list_collection_names():
                raise Exception("Collection {}.{} not found in {}".format(db_info['database_name'],
                                                                          db_info['collection_name'], db_info["database_url"]))
            client.close()

    def create_source_indexes(self):
        for db_info in self.config["scienti_affiliations"]["databases"]:
            database_url = db_info.get('database_url', '')
            database_name = db_info.get('database_name', '')
            collection_name = db_info.get('collection_name', '')

            if database_url and database_name and collection_name:
                client = MongoClient(database_url)
                db = client[database_name]
                collection = db[collection_name]

                collection.create_index(
                    [('group.institution.TXT_NIT', ASCENDING)])
                collection.create_index([('group.COD_ID_GRUPO', ASCENDING)])
                client.close()

    def process_scienti_institutions(self, config, verbose=0):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        for cod_inst in scienti.distinct("group.institution.TXT_NIT"):
            if not cod_inst:
                continue
            reg_scienti = scienti.find_one(
                {"group.institution.TXT_NIT": cod_inst})
            for inst in reg_scienti["group"][0]["institution"]:
                token = inst["NME_INST"]
                stopwords = ["y", "and", "de", "la", "los", "las", "el", "o", "or", "un", "una", "uno", "en", "por", "para", "según", "a", "ante",
                             "con", "de", "sin", "so", "tras", "e", "u", "del", "and", "or", "from", "to", "after", "about", "by", "in", "out", "next",
                             "under", "our", "your", "yours", "them", "their", "my", "it", "we", "have", "had", "be", "do", "are", "him", "her", "hers", "his",
                             "then", "where", "why", "how", "what", "which", "who", "whom", "all", "any", "both", "each", "few", "at", "this", "these", "those",
                             "that", "if", "as", "with", "while", "against", "about", "here", "there", "off", "of", "-"]
                inst_name = " ".join(
                    [w for w in token.lower().split() if w not in stopwords])
                inst_name = inst_name.replace("universidad", "").replace(
                    "institución universitaria", "").replace("industrial", "")
                inst_name = inst_name.replace("corporación", "").replace(
                    "fundación", "").replace("instituto", "").strip()
                col_list = self.collection.find(
                    {"$text": {"$search": inst_name}}).limit(30)
                reg_col = None
                name = None
                highest_score = 0
                highest_name = None
                if inst["NME_INST"] == 'Universidad Autónoma Latinoamericana - Unaula':
                    reg_col = self.collection.find_one(
                        {"names.name": "Universidad Autónoma Latinoamericana"})
                if "colciencias" in inst_name:
                    reg_col = self.collection.find_one(
                        {"names.name": "Colciencias"})
                if inst["NME_INST"] == "UNIVERSIDAD CATOLICA DE ORIENTE":
                    reg_col = self.collection.find_one(
                        {"names.name": "Universidad Católica de Oriente"})
                if inst["NME_INST"] == "UNIVERSIDAD ":
                    reg_col = self.collection.find_one(
                        {"names.name": "Universidad Católica de Oriente"})

                if not reg_col:
                    for reg in col_list:
                        for name in reg["names"]:
                            if inst["NME_INST"].lower() == name["name"].lower():
                                name = name["name"]
                                reg_col = reg
                                break
                        if reg_col:
                            break
                        for name in reg["names"]:
                            score = fuzz.ratio(
                                inst["NME_INST"].lower(), name["name"].lower())
                            if score > 90:
                                name = name["name"]
                                reg_col = reg
                                break
                            elif score > 70:
                                score = fuzz.partial_ratio(
                                    inst["NME_INST"].lower(), name["name"].lower())
                                if score > 93:
                                    reg_col = reg
                                    name = name["name"]
                                    break
                                else:
                                    if score > highest_score:
                                        highest_score = score
                                        highest_name = name["name"]
                            else:
                                if score > highest_score:
                                    highest_score = score
                                    highest_name = name["name"]
                        if reg_col:
                            break
                if reg_col:
                    found_updated = False
                    for upd in reg_col["updated"]:
                        if upd["source"] == "scienti":
                            found_updated = True
                            break
                    if found_updated:
                        continue
                    name = reg_col["names"][0]["name"]
                    reg_col["updated"].append(
                        {"source": "scienti", "time": int(time())})
                    reg_col["external_ids"].append(
                        {"source": "minciencias", "id": inst["COD_INST"]})
                    reg_col["external_ids"].append(
                        {"source": "nit", "id": inst["TXT_NIT"] + "-" + inst["TXT_DIGITO_VERIFICADOR"]})
                    if inst["SGL_INST"] not in reg_col["abbreviations"]:
                        reg_col["abbreviations"].append(inst["SGL_INST"])
                    if "URL_HOME_PAGE" in inst.keys():
                        if {"source": "site", "url": inst["URL_HOME_PAGE"]} not in reg_col["external_urls"]:
                            reg_col["external_urls"].append(
                                {"source": "site", "url": inst["URL_HOME_PAGE"]})
                    self.collection.update_one({"_id": reg_col["_id"]},
                                               {"$set": {
                                                   "updated": reg_col["updated"],
                                                   "external_ids": reg_col["external_ids"],
                                                   "abbreviations": reg_col["abbreviations"],
                                                   "external_urls": reg_col["external_urls"]
                                               }})
                else:
                    if self.verbose == 4:
                        print(inst_name)
                        print("Almost similar (", highest_score, "): ",
                              inst["NME_INST"], " - ", highest_name)

    def extract_subject(self, subjects, data):
        subjects.append({
            "id": "",
            "name": data["TXT_NME_AREA"],
            "level": data["NRO_NIVEL"],
            "external_ids": [{"source": "OCDE", "id": data["COD_AREA_CONOCIMIENTO"]}]
        })
        if "knowledge_area" in data.keys():
            self.extract_subject(subjects, data["knowledge_area"][0])
        return subjects

    def process_scienti_groups(self, config, verbose=0):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        for group_id in scienti.distinct("group.COD_ID_GRUPO", {"group.COD_ID_GRUPO": {"$ne": None}}):
            db_reg = self.collection.find_one({"external_ids.id": group_id})
            if db_reg:
                continue
            entry = self.empty_affiliation()
            entry["updated"].append({"time": int(time()), "source": "scienti"})
            entry["external_ids"].append(
                {"source": "minciencias", "id": group_id})
            entry["types"].append({"source": "scienti", "type": "group"})

            group = scienti.find_one({"group.COD_ID_GRUPO": group_id})
            group = group["group"][0]

            if group:
                entry["external_ids"].append(
                    {"source": "scienti", "id": group["NRO_ID_GRUPO"]})
                entry["names"].append(
                    {"name": group["NME_GRUPO"], "lang": "es", "source": "scienti"})
                entry["birthdate"] = check_date_format(
                    str(group["ANO_FORMACAO"]) + "-" + str(group["MES_FORMACAO"]))
                if group["STA_ELIMINADO"] == "F":
                    entry["status"].append(
                        {"source": "minciencias", "status": "activo"})
                if group["STA_ELIMINADO"] == "T" or group["STA_ELIMINADO"] == "V":
                    entry["status"].append(
                        {"source": "minciencias", "status": "eliminado"})

                entry["description"].append({
                    "source": "scienti",
                    "description": {
                        "TXT_PLAN_TRABAJO": group["TXT_PLAN_TRABAJO"] if "TXT_PLAN_TRABAJO" in group.keys() else "",
                        "TXT_ESTADO_ARTE": group["TXT_ESTADO_ARTE"] if "TXT_ESTADO_ARTE" in group.keys() else "",
                        "TXT_OBJETIVOS": group["TXT_OBJETIVOS"]if "TXT_OBJETIVOS" in group.keys() else "",
                        "TXT_PROD_DESTACADA": group["TXT_PROD_DESTACADA"]if "TXT_PROD_DESTACADA" in group.keys() else "",
                        "TXT_RETOS": group["TXT_RETOS"]if "TXT_RETOS" in group.keys() else "",
                        "TXT_VISION": group["TXT_VISION"] if "TXT_VISION" in group.keys() else ""
                    }
                })

                if "TXT_CLASIF" in group.keys() and "DTA_CLASIF" in group.keys():
                    entry["ranking"].append({
                        "source": "scienti",
                        "rank": normalize_scienti_rank(group["TXT_CLASIF"]),
                        "from_date": check_date_format(group["DTA_CLASIF"]),
                        "to_date": check_date_format(group["DTA_FIN_CLASIF"])
                    })

                subjects = self.extract_subject([], group["knowledge_area"][0])
                if len(subjects) > 0:
                    entry["subjects"].append({
                        "source": "OCDE",
                        "subjects": subjects
                    })

                for reg in scienti.find({"group.COD_ID_GRUPO": group_id}):
                    for inst in reg["group"][0]["institution"]:
                        if not inst["TXT_NIT"] or not inst["TXT_DIGITO_VERIFICADOR"]:
                            continue
                        db_inst = self.collection.find_one(
                            {"external_ids.id": inst["TXT_NIT"] + "-" + inst["TXT_DIGITO_VERIFICADOR"]})
                        if db_inst:
                            name = db_inst["names"][0]["name"]
                            for n in db_inst["names"]:
                                if n["lang"] == "es":
                                    name = n["name"]
                                    break
                                elif n["lang"] == "en":
                                    name = n["name"]
                            rel_entry = {
                                "name": name, "id": db_inst["_id"], "types": db_inst["types"]}
                            if rel_entry not in entry["relations"]:
                                entry["relations"].append(rel_entry)
                entry["_id"] = group_id
                self.collection.insert_one(entry)

    def run(self):
        for config in self.config["scienti_affiliations"]["databases"]:
            if self.verbose > 4:
                start_time = time()
            if self.verbose > 0:
                print("Processing {}.{} database".format(
                    config["database_name"], config["collection_name"]))
            self.process_scienti_institutions(config, verbose=self.verbose)
            self.process_scienti_groups(config, verbose=self.verbose)
        if self.verbose > 4:
            print("Execution time: {} minutes".format(
                round((time() - start_time) / 60, 2)))
        return 0
