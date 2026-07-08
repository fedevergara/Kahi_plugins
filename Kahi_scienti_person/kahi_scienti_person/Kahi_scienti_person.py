from kahi.KahiBase import KahiBase
from pymongo import MongoClient, ASCENDING, TEXT
from time import time
from kahi_impactu_utils.Utils import check_date_format, get_id_from_url, parse_sex, doi_processor
from kahi_impactu_utils.String import title_case
import re


class Kahi_scienti_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("external_ids.id.COD_RH")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])

        self.verbose = config["scienti_person"]["verbose"] if "verbose" in config["scienti_person"].keys(
        ) else 0

        # checking if the databases and collections are available
        self.check_databases_and_collections()
        # creating indexes for the scienti sources
        self.create_source_indexes()

    def check_databases_and_collections(self):
        for db_info in self.config["scienti_person"]["databases"]:
            client = MongoClient(db_info["database_url"])
            if db_info['database_name'] not in client.list_database_names():
                raise Exception("Database {} not found".format(
                    db_info['database_name']))
            if db_info['collection_name'] not in client[db_info['database_name']].list_collection_names():
                raise Exception("Collection {}.{} not found in {}".format(db_info['database_name'],
                                                                          db_info['collection_name'], db_info["database_url"]))
            client.close()

    def create_source_indexes(self):
        for db_info in self.config["scienti_person"]["databases"]:
            database_url = db_info.get('database_url', '')
            database_name = db_info.get('database_name', '')
            collection_name = db_info.get('collection_name', '')

            if database_url and database_name and collection_name:
                client = MongoClient(database_url)
                db = client[database_name]
                collection = db[collection_name]

                collection.create_index(
                    [('author.NRO_DOCUMENTO_IDENT', ASCENDING)])
                collection.create_index([('author.COD_RH', ASCENDING)])
                collection.create_index([('author_others', ASCENDING)])
                collection.create_index(
                    [('re_author_others.author_others.COD_RH_REF', ASCENDING)])
                collection.create_index(
                    [('re_author_others.author.COD_RH', ASCENDING)])
                client.close()

    def update_inserted(self, config, verbose=0):
        # The authors that have been inserted by staff are updated.
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        for person in self.collection.find():
            idx = None
            for ext in person["external_ids"]:
                if ext["source"] == "Cédula de Ciudadanía":
                    idx = ext["id"]
                elif ext["source"] == "Cédula de Extranjería":
                    idx = ext["id"]
            scienti_reg = scienti.find_one({"author.NRO_DOCUMENTO_IDENT": idx})
            if scienti_reg:
                author = scienti_reg["author"][0]
                rec = {"provenance": "scienti",
                       "source": "scienti", "id": {"COD_RH": author["COD_RH"]}}
                if rec not in person["external_ids"]:
                    person["external_ids"].append(rec)
                if "COD_ORCID" in author.keys():
                    if author["COD_ORCID"]:
                        orcid_id = None
                        if "orcid" in author["COD_ORCID"]:
                            orcid_id = get_id_from_url(author["COD_ORCID"])
                        else:
                            orcid_id = "https://orcid.org/" + \
                                author["COD_ORCID"]
                        if orcid_id:
                            rec = {"provenance": "scienti",
                                   "source": "orcid", "id": orcid_id}
                            if rec not in person["external_ids"]:
                                person["external_ids"].append(rec)
                if "staff" not in [source["source"] for source in person["updated"]]:
                    first_names = re.sub(
                        r'\s+', ' ', author["TXT_NAMES_RH"].replace(".", " ")).strip()
                    person["first_names"] = title_case(first_names).strip().split()
                    person["last_names"] = []
                    if "TXT_PRIM_APELL" in author.keys():
                        lname = re.sub(
                            r'\s+', ' ', author["TXT_PRIM_APELL"].replace(".", " ")).strip()
                        if lname:
                            person["last_names"].append(title_case(lname))
                    if "TXT_SEG_APELL" in author.keys():
                        if author["TXT_SEG_APELL"] is not None:
                            lname = re.sub(
                                r'\s+', ' ', author["TXT_SEG_APELL"].replace(".", " ")).strip()
                            if lname:
                                person["last_names"].append(title_case(lname))
                    full_name = " ".join(person["first_names"]) + " " + " ".join(person["last_names"])
                    person["full_name"] = title_case(full_name)
                    person["initials"] = "".join([p[0].upper() for p in person["first_names"]])

                marital = None
                if "TPO_ESTADO_CIVIL" in author.keys():
                    if author["TPO_ESTADO_CIVIL"] == "C":
                        marital = "Married"
                    elif author["TPO_ESTADO_CIVIL"] == "S":
                        marital = "Single"
                    elif author["TPO_ESTADO_CIVIL"] == "U":
                        marital = "Domestic Partnership"
                    elif author["TPO_ESTADO_CIVIL"] == "D":
                        marital = "Divorced"
                    elif author["TPO_ESTADO_CIVIL"] == "V":
                        marital = "Widowed"

                if "city" in author.keys():
                    city = author["city"][0]
                    birthplace = {
                        "city": city["TXT_NME_MUNICIPIO"].capitalize(),
                        "state": city["department"][0]["TXT_NME_DEPARTAMENTO"].capitalize(),
                        "country": city["department"][0]["country"][0]["TXT_NME_PAIS"].capitalize()
                    }

                for upd in person["updated"]:
                    if upd["source"] == "scienti":
                        upd["time"] = int(time())

                if 'scienti' not in [x['source'] for x in person['updated']]:
                    person['updated'].append(
                        {"time": int(time()), "source": "scienti"})

                rank = person["ranking"]
                ranks = []
                for prod in scienti.find({"author.NRO_DOCUMENTO_IDENT": idx}):
                    group_entry = {}
                    if "group" in prod.keys():
                        for group in prod["group"]:
                            group_db = self.db["affiliations"].find_one(
                                {"external_ids.id": group["COD_ID_GRUPO"]})
                            if group_db:
                                name = group_db["names"][0]["name"]
                                for n in group_db["names"]:
                                    if n["lang"] == "es":
                                        name = n["name"]
                                        break
                                    elif n["lang"] == "en":
                                        name = n["name"]
                                aff_found = False
                                for aff in person["affiliations"]:
                                    if group_db["_id"] == aff["id"]:
                                        aff_found = True
                                        break
                                if aff_found:
                                    continue
                                time_str = ""
                                if len(str(prod["NRO_ANO_PRESENTA"])) == 4:
                                    time_str += str(prod["NRO_ANO_PRESENTA"])
                                else:
                                    continue
                                if len(str(prod["NRO_MES_PRESENTA"])) < 2:
                                    time_str += "-0" + \
                                        str(prod["NRO_MES_PRESENTA"])
                                elif len(str(prod["NRO_MES_PRESENTA"])) == 2:
                                    time_str += "-" + \
                                        str(prod["NRO_MES_PRESENTA"])
                                aff_time = check_date_format(time_str)
                                name = title_case(name)
                                group_entry = {
                                    "id": group_db["_id"], "name": name, "types": group_db["types"], "start_date": aff_time, "end_date": -1}
                                if group_entry not in person["affiliations"]:
                                    person["affiliations"].append(group_entry)

                    au = prod["author"][0]
                    if "TPO_PERFIL" in au.keys():
                        if au["TPO_PERFIL"] not in ranks:
                            date = ""
                            if "DTA_CREACION" in prod.keys():
                                date = check_date_format(prod["DTA_CREACION"])
                            rank_entry = {
                                "date": date,
                                "rank": au["TPO_PERFIL"],
                                "source": "scienti"
                            }
                            rank.append(rank_entry)
                            ranks.append(au["TPO_PERFIL"])

                    # Related works
                    doi = ""
                    if "TXT_DOI" in prod.keys() and prod["TXT_DOI"]:
                        doi = doi_processor(prod["TXT_DOI"])
                        if doi:
                            rec = {
                                "provenance": "scienti",
                                "source": "doi",
                                "id": doi}
                            if rec not in person["related_works"]:
                                person["related_works"].append(rec)

                    if "TXT_WEB_PRODUCTO" in prod.keys() and prod["TXT_WEB_PRODUCTO"] and "10." in prod["TXT_WEB_PRODUCTO"]:
                        doi = doi_processor(prod["TXT_WEB_PRODUCTO"])
                        if doi:
                            extracted_doi = re.compile(
                                r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', re.IGNORECASE).match(doi)
                            if extracted_doi:
                                doi = extracted_doi.group(0)
                                for keyword in ['abstract', 'homepage', 'tpmd200765', 'event_abstract']:
                                    doi = doi.split(
                                        f'/{keyword}')[0] if keyword in doi else doi
                            rec = {
                                "provenance": "scienti",
                                "source": "doi",
                                "id": doi}
                            if rec not in person["related_works"]:
                                person["related_works"].append(rec)

                    scienti_rec = ""
                    cod_rh = prod.get("COD_RH", "")
                    cod_producto = prod.get("COD_PRODUCTO", "")
                    if cod_rh and cod_producto:
                        scienti_rec = {"COD_RH": cod_rh,
                                       "COD_PRODUCTO": cod_producto}
                    if scienti_rec:
                        rec = {
                            "provenance": "scienti",
                            "source": "scienti",
                            "id": scienti_rec
                        }
                        if rec not in person["related_works"]:
                            person["related_works"].append(rec)

                self.collection.update_one({"_id": person["_id"]}, {"$set": {
                    "external_ids": person["external_ids"],
                    "first_names": person["first_names"],
                    "last_names": person["last_names"],
                    "initials": person["initials"],
                    "full_name": person["full_name"],
                    "updated": person["updated"],
                    "affiliations": person["affiliations"],
                    "ranking": rank,
                    "marital_status": marital,
                    "birthplace": birthplace,
                    "related_works": person["related_works"],
                }})

    def insert_scienti(self, config, verbose=0):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        for rh in scienti.distinct("author.COD_RH"):
            author_db = self.collection.find_one(
                {"external_ids.id.COD_RH": rh})
            if author_db:
                continue
            author_scienti = scienti.find_one({"author.COD_RH": rh})
            if author_scienti:
                author = author_scienti["author"][0]
                if "NRO_DOCUMENTO_IDENT" in author.keys():
                    author_db = self.collection.find_one(
                        {"external_ids.id": author["NRO_DOCUMENTO_IDENT"]})
                if not author_db:
                    if "COD_ORCID" in author.keys():
                        if author["COD_ORCID"]:
                            orcid_id = None
                            if "orcid" in author["COD_ORCID"]:
                                orcid_id = get_id_from_url(author["COD_ORCID"])
                            else:
                                orcid_url = "https://orcid.org/" + \
                                    author["COD_ORCID"]
                                orcid_id = get_id_from_url(orcid_url)
                            if orcid_id:
                                author_db = self.collection.find_one(
                                    {"external_ids.id": orcid_id})
                if not author_db:
                    if "AUTHOR_ID_SCP" in author.keys():
                        if author["AUTHOR_ID_SCP"]:
                            scopus_id = None
                            # check if the id is a url
                            if "scopus" in author["AUTHOR_ID_SCP"]:
                                scopus_id = get_id_from_url(
                                    author["AUTHOR_ID_SCP"])
                            else:
                                scopus_url = "https://www.scopus.com/authid/detail.uri?authorId=" + \
                                    author["AUTHOR_ID_SCP"]
                                scopus_id = get_id_from_url(scopus_url)
                            # get_id_from_url can return None (we need to double check)
                            if scopus_id:
                                author_db = self.collection.find_one(
                                    {"external_ids.id": scopus_id})
                if not author_db:
                    entry = self.empty_person()
                    entry["updated"].append(
                        {"time": int(time()), "source": "scienti"})
                    if author["NRO_DOCUMENTO_IDENT"]:
                        if author["TPO_DOCUMENTO_IDENT"] == "P":
                            rec = {"provenance": "scienti",
                                   "source": "Passport", "id": author["NRO_DOCUMENTO_IDENT"]}
                            if rec not in entry["external_ids"]:
                                entry["external_ids"].append(rec)
                        if author["TPO_DOCUMENTO_IDENT"] == "C":
                            rec = {"provenance": "scienti",
                                   "source": "Cédula de Ciudadanía", "id": author["NRO_DOCUMENTO_IDENT"]}
                            if rec not in entry["external_ids"]:
                                entry["external_ids"].append(rec)
                        if author["TPO_DOCUMENTO_IDENT"] == "E":
                            rec = {"provenance": "scienti",
                                   "source": "Cédula de Extranjería", "id": author["NRO_DOCUMENTO_IDENT"]}
                            if rec not in entry["external_ids"]:
                                entry["external_ids"].append(rec)
                    entry["external_ids"].append(
                        {"provenance": "scienti", "source": "scienti", "id": {"COD_RH": author["COD_RH"]}})
                    if "COD_ORCID" in author.keys():
                        if author["COD_ORCID"]:
                            orcid_id = None
                            if "orcid" in author["COD_ORCID"]:
                                orcid_id = get_id_from_url(author["COD_ORCID"])
                            else:
                                orcid_url = "https://orcid.org/" + \
                                    author["COD_ORCID"]
                                orcid_id = get_id_from_url(orcid_url)
                            if orcid_id:
                                entry["external_ids"].append(
                                    {"provenance": "scienti",
                                     "source": "orcid", "id": orcid_id})
                    if "AUTHOR_ID_SCP" in author.keys():
                        if author["AUTHOR_ID_SCP"]:
                            scopus_id = None
                            if "scopus" in author["AUTHOR_ID_SCP"]:
                                scopus_id = get_id_from_url(
                                    author["AUTHOR_ID_SCP"])
                            else:
                                scopus_url = "https://www.scopus.com/authid/detail.uri?authorId=" + \
                                    author["AUTHOR_ID_SCP"]
                                scopus_id = get_id_from_url(scopus_url)
                            # get_id_from_url can return None (we need to double check)
                            if scopus_id:
                                entry["external_ids"].append({
                                    "provenance": "scienti",
                                    "source": "scopus",
                                    "id": scopus_id})
                    # implement the right split names here
                    entry["first_names"] = title_case(
                        re.sub(r'\s+', ' ', author["TXT_NAMES_RH"].strip())).strip().split()
                    entry["last_names"] = []
                    if "TXT_PRIM_APELL" in author.keys():
                        entry["last_names"].append(
                            title_case(re.sub(r'\s+', ' ', author["TXT_PRIM_APELL"].strip())))
                    if "TXT_SEG_APELL" in author.keys():
                        if author["TXT_SEG_APELL"] is not None:
                            entry["last_names"].append(
                                title_case(re.sub(r'\s+', ' ', author["TXT_SEG_APELL"].strip())))
                    entry["full_name"] = " ".join(
                        entry["first_names"]) + " " + " ".join(entry["last_names"])
                    entry["initials"] = "".join(
                        [p[0].upper() for p in entry["first_names"]])
                    if "TXT_CITACION_BIBLIO" in author.keys():
                        if author["TXT_CITACION_BIBLIO"] is not None:
                            entry["aliases"].append(
                                author["TXT_CITACION_BIBLIO"].lower())
                    if "TPO_SEXO" in author.keys():
                        if author["TPO_SEXO"] is not None:
                            entry["sex"] = parse_sex(author["TPO_SEXO"])
                    if "TPO_PERFIL" in author.keys():
                        ranking = {
                            "date": "", "rank": author["TPO_PERFIL"], "source": "scienti"}
                        if ranking not in entry["ranking"]:
                            entry["ranking"].append(ranking)
                    if "institution" in author_scienti.keys():
                        aff_db = db["affiliations"].find_one(
                            {"external_ids.id": author_scienti["institution"][0]["COD_INST"]})
                        if aff_db:
                            name = aff_db["names"][0]["name"]
                            for n in aff_db["names"]:
                                if n["lang"] == "es":
                                    name = n["name"]
                                    break
                                elif n["lang"] == "en":
                                    name = n["name"]
                            name = title_case(name)
                            entry["affiliations"].append({
                                "id": aff_db["_id"],
                                "name": name,
                                "types": aff_db["types"],
                                "start_date": -1,
                                "end_date": -1
                            })

                    if "DTA_NACIM" in author.keys():
                        entry["birthdate"] = check_date_format(
                            author["DTA_NACIM"])

                    if "TPO_ESTADO_CIVIL" in author.keys():
                        if author["TPO_ESTADO_CIVIL"] == "C":
                            entry["marital_status"] = "Married"
                        elif author["TPO_ESTADO_CIVIL"] == "S":
                            entry["marital_status"] = "Single"
                        elif author["TPO_ESTADO_CIVIL"] == "U":
                            entry["marital_status"] = "Domestic Partnership"
                        elif author["TPO_ESTADO_CIVIL"] == "D":
                            entry["marital_status"] = "Divorced"
                        elif author["TPO_ESTADO_CIVIL"] == "V":
                            entry["marital_status"] = "Widowed"

                    if "city" in author.keys():
                        city = author["city"][0]
                        entry["birthplace"] = {
                            "city": city["TXT_NME_MUNICIPIO"].capitalize(),
                            "state": city["department"][0]["TXT_NME_DEPARTAMENTO"].capitalize(),
                            "country": city["department"][0]["country"][0]["TXT_NME_PAIS"].capitalize()
                        }

                    rank = []
                    ranks = []
                    for prod in scienti.find({"author.COD_RH": rh}):
                        if "group" in prod.keys():
                            for group in prod["group"]:
                                group_entry = {}
                                group_db = self.db["affiliations"].find_one(
                                    {"external_ids.id": group["COD_ID_GRUPO"]})
                                if group_db:
                                    name = group_db["names"][0]["name"]
                                    for n in group_db["names"]:
                                        if n["lang"] == "es":
                                            name = n["name"]
                                            break
                                        elif n["lang"] == "en":
                                            name = n["name"]
                                    aff_found = False
                                    for aff in entry["affiliations"]:
                                        if group_db["_id"] == aff["id"]:
                                            aff_found = True
                                            break
                                    if aff_found:
                                        continue
                                    time_str = ""
                                    if len(str(prod["NRO_ANO_PRESENTA"])) == 4:
                                        time_str += str(prod["NRO_ANO_PRESENTA"])
                                    else:
                                        continue
                                    if len(str(prod["NRO_MES_PRESENTA"])) < 2:
                                        time_str += "-0" + \
                                            str(prod["NRO_MES_PRESENTA"])
                                    elif len(str(prod["NRO_MES_PRESENTA"])) == 2:
                                        time_str += "-" + \
                                            str(prod["NRO_MES_PRESENTA"])
                                    aff_time = check_date_format(time_str)
                                    group_entry = {
                                        "id": group_db["_id"], "name": name, "types": group_db["types"], "start_date": aff_time, "end_date": -1}
                                    if group_entry not in entry["affiliations"]:
                                        entry["affiliations"].append(
                                            group_entry)
                        au = prod["author"][0]
                        if "TPO_PERFIL" not in au.keys():
                            continue
                        if au["TPO_PERFIL"] in ranks:
                            continue
                        date = ""
                        if "DTA_CREACION" in prod.keys():
                            date = check_date_format(prod["DTA_CREACION"])
                        rank_entry = {
                            "date": date,
                            "rank": au["TPO_PERFIL"],
                            "source": "scienti"
                        }
                        rank.append(rank_entry)
                        ranks.append(au["TPO_PERFIL"])

                        # Related works
                        doi = ""
                        if "TXT_DOI" in prod.keys() and prod["TXT_DOI"]:
                            doi = doi_processor(prod["TXT_DOI"])
                            if doi:
                                entry["related_works"].append(
                                    {"provenance": "scienti",
                                     "source": "doi",
                                     "id": doi})
                        if "TXT_WEB_PRODUCTO" in prod.keys() and prod["TXT_WEB_PRODUCTO"] and "10." in prod["TXT_WEB_PRODUCTO"]:
                            doi = doi_processor(prod["TXT_WEB_PRODUCTO"])
                            if doi:
                                extracted_doi = re.compile(
                                    r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', re.IGNORECASE).match(doi)
                                if extracted_doi:
                                    doi = extracted_doi.group(0)
                                    for keyword in ['abstract', 'homepage', 'tpmd200765', 'event_abstract']:
                                        doi = doi.split(
                                            f'/{keyword}')[0] if keyword in doi else doi
                                entry["related_works"].append(
                                    {"provenance": "scienti",
                                     "source": "doi",
                                     "id": doi})

                        scienti_rec = ""
                        cod_rh = prod.get("COD_RH", "")
                        cod_producto = prod.get("COD_PRODUCTO", "")
                        if cod_rh and cod_producto:
                            scienti_rec = {"COD_RH": cod_rh,
                                           "COD_PRODUCTO": cod_producto}
                        if scienti_rec:
                            rec = {
                                "provenance": "scienti",
                                "source": "scienti",
                                "id": scienti_rec
                            }
                            if rec not in entry["related_works"]:
                                entry["related_works"].append(rec)
                    if rank:
                        entry["ranking"] = rank

                    self.collection.insert_one(entry)

    def insert_scienti_others(self, config, verbose=0):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        author_others = scienti.find({
            "$or": [
                {
                    "re_author_others.author_others": {"$exists": True},
                    "re_author_others": {"$elemMatch": {"author_others": {"$elemMatch": {"COD_RH_REF": {"$ne": None}}}}}},
                {
                    "re_author_others.author": {"$exists": True},
                    "re_author_others": {"$elemMatch": {"author": {"$elemMatch": {"COD_RH": {"$ne": None}}}}}}]}, {
            "re_author_others": 1,
            "COD_RH": 1,
            "COD_PRODUCTO": 1,
            "TXT_DOI": 1,
            "TXT_WEB_PRODUCTO": 1})
        for author_others_reg in author_others:
            for re_author in author_others_reg["re_author_others"]:
                if "author_others" in re_author.keys():
                    author_others_key = "author_others"
                elif "author" in re_author.keys():
                    author_others_key = "author"
                else:
                    continue
                for author in re_author[author_others_key]:
                    author_db = None
                    if "COD_RH_REF" in author.keys():
                        if author["COD_RH_REF"]:
                            author_db = self.collection.find_one(
                                {"external_ids.id.COD_RH": author["COD_RH_REF"]})
                    else:
                        if "COD_RH" in author.keys():
                            if author["COD_RH"]:
                                author_db = self.collection.find_one(
                                    {"external_ids.id.COD_RH": author["COD_RH"]})

                    if not author_db and "NRO_DOC_IDENTIFICACION" in author.keys():
                        if author["NRO_DOC_IDENTIFICACION"]:
                            author_db = self.collection.find_one(
                                {"external_ids.id": author["NRO_DOC_IDENTIFICACION"]})
                    else:
                        if not author_db and "NRO_DOCUMENTO_IDENT" in author.keys():
                            if author["NRO_DOCUMENTO_IDENT"]:
                                author_db = self.collection.find_one(
                                    {"external_ids.id": author["NRO_DOCUMENTO_IDENT"]})

                    if not author_db and "COD_ORCID" in author.keys():
                        if author["COD_ORCID"]:
                            orcid_id = None
                            if "orcid" in author["COD_ORCID"]:
                                orcid_id = get_id_from_url(author["COD_ORCID"])
                            else:
                                orcid_url = "https://orcid.org/" + \
                                    author["COD_ORCID"]
                                orcid_id = get_id_from_url(orcid_url)
                            if orcid_id:
                                author_db = self.collection.find_one(
                                    {"external_ids.id": orcid_id})

                    if not author_db and "AUTOR_ID_SCP" in author.keys():
                        if author["AUTOR_ID_SCP"]:
                            scopus_id = None
                            if "scopus" in author["AUTOR_ID_SCP"]:
                                scopus_id = get_id_from_url(
                                    author["AUTOR_ID_SCP"])
                            else:
                                scopus_url = "https://www.scopus.com/authid/detail.uri?authorId=" + \
                                    author["AUTOR_ID_SCP"]
                                scopus_id = get_id_from_url(scopus_url)
                            # get_id_from_url can return None (we need to double check)
                            if scopus_id:
                                author_db = self.collection.find_one(
                                    {"external_ids.id": scopus_id})

                    if author_db:
                        # The other author is already in the database, we need to update the entry
                        # Related works
                        rec = {
                            "provenance": "scienti",
                            "source": "scienti",
                            "id": {
                                "COD_RH": author_others_reg["COD_RH"],
                                "COD_PRODUCTO": author_others_reg["COD_PRODUCTO"]}}
                        if rec not in author_db["related_works"]:
                            author_db["related_works"].append(rec)

                        self.collection.update_one({"_id": author_db["_id"]}, {"$set": {
                            "related_works": author_db["related_works"]
                        }})
                        continue

                    cod_rh = author["COD_RH_REF"] if "COD_RH_REF" in author.keys() else author["COD_RH"]
                    if not cod_rh:
                        continue  # We need the COD_RH to insert the author

                    entry = self.empty_person()
                    entry["updated"].append(
                        {"time": int(time()), "source": "scienti"})

                    n_doc_ident_key = "NRO_DOC_IDENTIFICACION" if "NRO_DOC_IDENTIFICACION" in author.keys() else "NRO_DOCUMENTO_IDENT"
                    tpo_doc_ident_key = "TPO_DOC_IDENTIFICACION" if "TPO_DOC_IDENTIFICACION" in author.keys() else "TPO_DOCUMENTO_IDENT"
                    if n_doc_ident_key in author.keys() and tpo_doc_ident_key in author.keys():
                        if author[n_doc_ident_key]:
                            if author[tpo_doc_ident_key] == "P":
                                rec = {"provenance": "scienti",
                                       "source": "Passport",
                                       "id": author[n_doc_ident_key]}
                                if rec not in entry["external_ids"]:
                                    entry["external_ids"].append(rec)
                            if author[tpo_doc_ident_key] == "C":
                                rec = {"provenance": "scienti",
                                       "source": "Cédula de Ciudadanía",
                                       "id": author[n_doc_ident_key]}
                                if rec not in entry["external_ids"]:
                                    entry["external_ids"].append(rec)
                            if author[tpo_doc_ident_key] == "E":
                                rec = {"provenance": "scienti",
                                       "source": "Cédula de Extranjería",
                                       "id": author[n_doc_ident_key]}
                                if rec not in entry["external_ids"]:
                                    entry["external_ids"].append(rec)
                    if "COD_ORCID" in author.keys():
                        if author["COD_ORCID"]:
                            orcid_id = None
                            if "orcid" in author["COD_ORCID"]:
                                orcid_id = get_id_from_url(author["COD_ORCID"])
                            else:
                                orcid_id = "https://orcid.org/" + \
                                    author["COD_ORCID"]
                            if orcid_id:
                                rec = {
                                    "provenance": "scienti",
                                    "source": "orcid",
                                    "id": orcid_id}
                                if rec not in entry["external_ids"]:
                                    entry["external_ids"].append(rec)
                    if "COD_RH_REF" in author.keys():
                        rec = {"provenance": "scienti",
                               "source": "scienti", "id": {"COD_RH": author["COD_RH_REF"]}}
                        if rec not in entry["external_ids"]:
                            entry["external_ids"].append(rec)
                    else:
                        rec = {"provenance": "scienti",
                               "source": "scienti", "id": {"COD_RH": author["COD_RH"]}}
                        if rec not in entry["external_ids"]:
                            entry["external_ids"].append(rec)
                    if "AUTOR_ID_SCP" in author.keys():
                        if author["AUTOR_ID_SCP"]:
                            scopus_id = None
                            if "scopus" in author["AUTOR_ID_SCP"]:
                                scopus_id = get_id_from_url(
                                    author["AUTOR_ID_SCP"])
                            else:
                                scopus_id = "https://www.scopus.com/authid/detail.uri?authorId=" + \
                                    author["AUTOR_ID_SCP"]
                            if scopus_id:
                                rec = {"provenance": "scienti",
                                       "source": "scopus", "id": scopus_id}
                                if rec not in entry["external_ids"]:
                                    entry["external_ids"].append(rec)
                    # Related works
                    doi = ""
                    if "TXT_DOI" in author_others_reg.keys() and author_others_reg["TXT_DOI"]:
                        doi = doi_processor(author_others_reg["TXT_DOI"])
                        if doi:
                            rec = {
                                "provenance": "scienti",
                                "source": "doi",
                                "id": doi}
                            if rec not in entry["related_works"]:
                                entry["related_works"].append(rec)
                    if "TXT_WEB_PRODUCTO" in author_others_reg.keys() and author_others_reg["TXT_WEB_PRODUCTO"] and "10." in author_others_reg["TXT_WEB_PRODUCTO"]:
                        doi = doi_processor(
                            author_others_reg["TXT_WEB_PRODUCTO"])
                        if doi:
                            extracted_doi = re.compile(
                                r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', re.IGNORECASE).match(doi)
                            if extracted_doi:
                                doi = extracted_doi.group(0)
                                for keyword in ['abstract', 'homepage', 'tpmd200765', 'event_abstract']:
                                    doi = doi.split(
                                        f'/{keyword}')[0] if keyword in doi else doi
                            rec = {
                                "provenance": "scienti",
                                "source": "doi",
                                "id": doi}
                            if rec not in entry["related_works"]:
                                entry["related_works"].append(rec)

                    scienti_rec = ""
                    cod_rh = author_others_reg.get("COD_RH", "")
                    cod_producto = author_others_reg.get("COD_PRODUCTO", "")
                    if cod_rh and cod_producto:
                        scienti_rec = {"COD_RH": cod_rh,
                                       "COD_PRODUCTO": cod_producto}
                    if scienti_rec:
                        rec = {
                            "provenance": "scienti",
                            "source": "scienti",
                            "id": scienti_rec
                        }
                        if rec not in entry["related_works"]:
                            entry["related_works"].append(rec)

                    if entry["external_ids"] == []:
                        continue
                    if "TXT_NME_RH" in author.keys() and author["TXT_NME_RH"]:
                        entry["first_names"] = title_case(
                            author["TXT_NME_RH"]).strip().split()
                    elif "TXT_NAMES_RH" in author.keys() and author["TXT_NAMES_RH"]:
                        entry["first_names"] = title_case(
                            re.sub(r'\s+', ' ', author["TXT_NAMES_RH"].strip())).strip().split()
                    entry["last_names"] = []
                    if "TXT_PRIM_APELL" in author.keys():
                        entry["last_names"].append(
                            title_case(re.sub(r'\s+', ' ', author["TXT_PRIM_APELL"].strip())))
                    if "TXT_SEG_APELL" in author.keys():
                        if author["TXT_SEG_APELL"] is not None:
                            entry["last_names"].append(
                                title_case(re.sub(r'\s+', ' ', author["TXT_SEG_APELL"].strip())))
                    entry["full_name"] = " ".join(
                        entry["first_names"]) + " " + " ".join(entry["last_names"])
                    entry["initials"] = "".join(
                        [p[0].upper() for p in entry["first_names"]])

                    if "DTA_NACIMIENTO" in author.keys():
                        entry["birthdate"] = check_date_format(
                            author["DTA_NACIMIENTO"])

                    self.collection.insert_one(entry)

    def run(self):
        for config in self.config["scienti_person"]["databases"]:
            if self.verbose > 0:
                print("Processing {}.{} database".format(
                    config["database_name"], config["collection_name"]))
            if self.verbose > 4:
                start_time = time()
                print("Updating already inserted entries")
            self.update_inserted(config, verbose=self.verbose)
            if self.verbose > 4:
                print("Inserting new entries")
            self.insert_scienti(config, verbose=self.verbose)
            if self.verbose > 4:
                print("Processing authors_others")
            self.insert_scienti_others(config, verbose=self.verbose)
        if self.verbose > 4:
            print("Execution time: {} minutes".format(
                round((time() - start_time) / 60, 2)))
        return 0
