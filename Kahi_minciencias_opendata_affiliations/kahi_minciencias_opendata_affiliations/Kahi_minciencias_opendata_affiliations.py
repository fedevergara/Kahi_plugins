from kahi_impactu_utils.Utils import check_date_format
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import DuplicateKeyError
from joblib import Parallel, delayed
from datetime import datetime as dt
from kahi.KahiBase import KahiBase
from unidecode import unidecode
from thefuzz import fuzz
from copy import deepcopy
import hashlib
from time import time
import re


def is_strict_institution_match(match):
    """Accept exact names or high-confidence similarities only."""
    if not match:
        return False
    scores = match["scores"]
    return any([
        match["normalized_name"] == match["normalized_source_name"],
        scores["ratio"] >= 94,
        scores["token_sort_ratio"] >= 96,
        scores["token_set_ratio"] >= 95
        and match["distinctive_token_overlap"] >= 0.80
        and match["distinctive_name_token_count"] >= 3,
        scores["token_set_ratio"] >= 99
        and match["distinctive_token_overlap"] >= 0.80,
        scores["wratio"] >= 96
        and match["distinctive_token_overlap"] >= 0.80,
    ])


class Kahi_minciencias_opendata_affiliations(KahiBase):

    config = {}
    missing_group_names = {
        "COL0014761": "Política, Género y Democracia",
        "COL0058004": "Estudios Hobbesianos",
        "COL0095339": "Grupo de Investigación Tlamatinime sobre Ontología Latinoamericana",
        "COL0011438": "Resiliencia y Saneamiento RESA",
    }

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("types.type")
        self.collection.create_index("names.name")
        self.collection.create_index([("names.name", TEXT)])

        self.openadata_client = MongoClient(
            config["minciencias_opendata_affiliations"]["database_url"])
        if config["minciencias_opendata_affiliations"]["database_name"] not in self.openadata_client.list_database_names():
            raise Exception("Database {} not found in {}".format(
                config["minciencias_opendata_affiliations"]['database_name'], config["minciencias_opendata_affiliations"]["database_url"]))

        self.openadata_db = self.openadata_client[config["minciencias_opendata_affiliations"]["database_name"]]

        if config["minciencias_opendata_affiliations"]["collection_name"] not in self.openadata_db.list_collection_names():
            raise Exception("Collection {} not found in {}".format(
                config["minciencias_opendata_affiliations"]['collection_name'], config["minciencias_opendata_affiliations"]["database_url"]))

        self.openadata_collection = self.openadata_db[
            config["minciencias_opendata_affiliations"]["collection_name"]]
        self.openadata_collection.create_index(
            [("cod_grupo_gr", ASCENDING), ("ano_convo", DESCENDING)],
            name="cod_grupo_gr_1_ano_convo_-1",
        )

        self.n_jobs = config["minciencias_opendata_affiliations"]["num_jobs"] if "num_jobs" in config["minciencias_opendata_affiliations"].keys(
        ) else 1

        self.verbose = config["minciencias_opendata_affiliations"][
            "verbose"] if "verbose" in config["minciencias_opendata_affiliations"].keys() else 0

        self.inserted_cod_grupo = []
        self.institution_match_cache = {}

        for reg in self.collection.find({"types.type": "group"}):
            for ext in reg["external_ids"]:
                if ext["source"] == "minciencias":
                    self.inserted_cod_grupo.append(ext["id"])

    def rename_institution(self, name):
        if name == "Colegio Mayor Nuestra Señora del Rosario".lower() or name == "Colegio Mayor de Nuestra Señora del Rosario".lower() or name == 'Colegio Mayor Nuestra Senora Del Rosario'.lower():
            return "universidad del rosario"
        elif name == "universidad de la guajira":
            return "guajira"
        elif "minuto" in name and "dios" in name:
            return "minuto dios"
        elif "salle" in name:
            return "universidad salle"
        elif "icesi" in name:
            return "icesi"
        elif "sede" in name:
            return name.split("sede")[0].strip()  # Keep only the first part of the name if it contains "sede"
        elif name == "universidad militar nueva granada":
            return "nueva granada"
        elif "pamplona" in name:
            return "pamplona"
        elif "sucre" in name:
            return "sucre"
        elif "santo tomás" in name or "santo tomas" in name:
            return "santo tomas"
        elif name == "universidad simón bolívar":
            return "simon bolivar"
        elif "unidades" in name and "santander" in name:
            return "unidades tecnológicas santander"
        elif "popayán" in name:
            return "popayán"
        elif any(
            value in name
            for value in [
                "tecnológico metropolitano",
                "tecnologico metropolitano",
                "institucion universitaria itm",
                "institución universitaria itm",
                "institucion universitaria - itm",
                "institución universitaria - itm",
            ]
        ):
            return "instituto tecnologico metropolitano"
        elif "cesmag" in name:
            return "estudios superiores maría goretti"
        elif "distrital francisco" in name:
            return "distrital francisco josé"
        elif name in ["universidad industrial de santander", "uis"]:
            return "universidad industrial de santander"
        elif name in ["universidad de santander", "udes"]:
            return "universidad de santander"
        elif "universidad" in name and "francisco" in name and "paula" in name and "santander" in name:
            return "universidad francisco paula santander"
        elif "magdalena" in name:
            return "magdalena"
        elif "corporacion universitaria iberoamericana" == name:
            return "iberoamericana"
        elif name in ["corporacion universitaria adventista", "corporación universitaria adventista"]:
            return "colombia adventist university"
        elif name in ["fundacion hospitalaria san vicente de paul", "fundación hospitalaria san vicente de paúl"]:
            return "hospital universitario de san vicente fundacion"
        elif name in ["alcaldia de medellin", "alcaldía de medellín"]:
            return "municipality of medellin"
        else:
            return name

    def normalize_institution_name(self, name):
        if name is None:
            return ""
        name = unidecode(str(name).lower())
        name = name.replace("(colombia)", "").replace("bogotá", "")
        name = re.sub(r"[^\w\s]", " ", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name

    def normalize_inst_aval(self, name):
        name = name.lower().strip()
        name = self.rename_institution(name)
        return self.normalize_institution_name(name)

    def get_institution_name(self, institution):
        name = ""
        for n in institution.get("names", []):
            if n.get("lang") == "es" and n.get("name"):
                return n["name"]
            elif n.get("lang") == "en" and n.get("name") and not name:
                name = n["name"]
        if not name and institution.get("names"):
            name = institution["names"][0].get("name", "")
        return name

    def aval_institution_id(self, inst_aval):
        digest = hashlib.sha1(inst_aval.encode("utf-8")).hexdigest()[:6]
        return "IUA{}".format(digest)

    def affiliation_address_from_group(self, reg):
        return {
            "lat": "",
            "lng": "",
            "postcode": "",
            "state": reg.get("nme_departamento_gr", ""),
            "city": reg.get("nme_municipio_gr", ""),
            "country": "Colombia",
            "country_code": "CO"
        }

    def affiliation_address_from_institution(self, institution, reg):
        addresses = institution.get("addresses", [])
        if not addresses:
            return self.affiliation_address_from_group(reg)
        return {
            "lat": addresses[0].get("lat", None),
            "lng": addresses[0].get("lng", None),
            "postcode": addresses[0].get("postcode", None),
            "state": addresses[0].get("state", None),
            "city": addresses[0].get("city", None),
            "country": addresses[0].get("country", None),
            "country_code": addresses[0].get("country_code", None)
        }

    def append_unique(self, values, item):
        if item not in values:
            values.append(item)

    def get_or_create_aval_institution(self, inst_aval, collection, reg):
        inst_aval = inst_aval.strip()
        if not inst_aval:
            return None

        institution = self.find_matching_institution(inst_aval, reg=reg)
        if institution:
            return institution

        institution_id = self.aval_institution_id(inst_aval)
        existing = collection.find_one({"_id": institution_id})
        if existing:
            return existing

        entry = self.empty_affiliation()
        entry["_id"] = institution_id
        entry["updated"].append({"source": "minciencias", "time": int(time())})
        entry["names"].append(
            {"source": "minciencias", "lang": "es", "name": inst_aval})
        entry["types"].append({"source": "minciencias", "type": "other"})
        entry["addresses"].append(self.affiliation_address_from_group(reg))
        entry["external_ids"].append(
            {"source": "minciencias", "id": institution_id})

        try:
            collection.insert_one(entry)
        except DuplicateKeyError:
            pass
        return collection.find_one({"_id": institution_id})

    def add_aval_institution_relations(self, reg, entry, collection):
        if "inst_aval" not in reg:
            return

        for inst_aval in reg["inst_aval"].split("|"):
            institution = self.get_or_create_aval_institution(
                inst_aval, collection, reg)
            if institution:
                synthetic_id = self.aval_institution_id(inst_aval.strip())
                if institution["_id"] != synthetic_id:
                    entry["relations"] = [
                        relation for relation in entry["relations"]
                        if relation.get("id") != synthetic_id
                    ]
                relation = {
                    "types": institution.get("types", []),
                    "id": institution["_id"],
                    "name": self.get_institution_name(institution)
                }
                if not any(rel.get("id") == relation["id"] for rel in entry["relations"]):
                    entry["relations"].append(relation)
                self.append_unique(
                    entry["addresses"],
                    self.affiliation_address_from_institution(institution, reg),
                )
            else:
                self.append_unique(
                    entry["addresses"],
                    self.affiliation_address_from_group(reg),
                )

    def institution_candidate_names(self, institution):
        names = []
        for item in institution.get("names", []):
            name = item.get("name")
            if name and name not in names:
                names.append(name)
        for field in ["aliases", "abbreviations"]:
            for item in institution.get(field, []):
                name = item.get("name") if isinstance(item, dict) else item
                if name and name not in names:
                    names.append(name)
        return names

    def get_institution_candidates(self, inst_aval):
        projection = {
            "names": 1,
            "aliases": 1,
            "abbreviations": 1,
            "types": 1,
            "addresses": 1,
            "external_ids": 1,
            "relations": 1,
            "score": {"$meta": "textScore"}
        }
        base_query = {
            "types.type": {"$ne": "group"},
            "$or": [
                {"addresses.country": "Colombia"},
                {"addresses.country_code": "CO"},
                {"addresses.0": {"$exists": False}},
            ],
        }
        candidates = []
        seen = set()
        for search in ['"{}"'.format(inst_aval), inst_aval]:
            query = base_query.copy()
            query["$text"] = {"$search": search}
            cursor = self.collection.find(query, projection).sort(
                [("score", {"$meta": "textScore"})]).limit(100)
            for candidate in cursor:
                if candidate["_id"] not in seen:
                    candidates.append(candidate)
                    seen.add(candidate["_id"])
        return candidates

    def institution_match_score(self, institution, inst_aval):
        best = None
        stopwords = {"de", "del", "la", "las", "los", "el", "y", "e", "en", "para", "por", "a"}
        for name in self.institution_candidate_names(institution):
            name_mod = self.normalize_institution_name(name)
            compare_inst_aval = inst_aval
            inst_tokens = set(compare_inst_aval.split())

            name_tokens = set(name_mod.split())
            token_overlap = len(inst_tokens & name_tokens) / max(
                1, min(len(inst_tokens), len(name_tokens)))
            distinctive_inst_tokens = inst_tokens - stopwords
            distinctive_name_tokens = name_tokens - stopwords
            distinctive_token_overlap = len(distinctive_inst_tokens & distinctive_name_tokens) / max(
                1, min(len(distinctive_inst_tokens), len(distinctive_name_tokens)))
            scores = {
                "ratio": fuzz.ratio(name_mod, compare_inst_aval),
                "token_sort_ratio": fuzz.token_sort_ratio(name_mod, compare_inst_aval),
                "token_set_ratio": fuzz.token_set_ratio(name_mod, compare_inst_aval),
                "wratio": fuzz.WRatio(name_mod, compare_inst_aval),
            }
            score = max(scores.values())
            current = {
                "name": name,
                "normalized_name": name_mod,
                "normalized_source_name": compare_inst_aval,
                "score": score,
                "scores": scores,
                "token_overlap": token_overlap,
                "distinctive_token_overlap": distinctive_token_overlap,
                "distinctive_name_token_count": len(distinctive_name_tokens)
            }
            if best is None or current["score"] > best["score"]:
                best = current
        return best

    def institution_address_score(self, institution, reg):
        if not reg:
            return 0
        source_state = self.normalize_institution_name(
            reg.get("nme_departamento_gr", ""))
        source_city = self.normalize_institution_name(
            reg.get("nme_municipio_gr", ""))
        score = 0
        for address in institution.get("addresses", []):
            state = self.normalize_institution_name(address.get("state", ""))
            city = self.normalize_institution_name(address.get("city", ""))
            current = int(bool(source_state and state == source_state)) * 2
            current += int(bool(source_city and city == source_city)) * 3
            score = max(score, current)
        return score

    def institution_candidate_rank(self, candidate, match, reg):
        has_catalog_id = any(
            ext.get("source") == "minciencias"
            and re.fullmatch(r"\d{12}", str(ext.get("id", "")))
            for ext in candidate.get("external_ids", []))
        is_synthetic = str(candidate.get("_id", "")).startswith("IUA")
        is_root = not candidate.get("relations")
        return (
            match["normalized_name"] == match["normalized_source_name"],
            has_catalog_id,
            not is_synthetic,
            self.institution_address_score(candidate, reg),
            is_root,
            match["score"],
            match["distinctive_token_overlap"],
        )

    def find_matching_institution(self, inst_aval, reg=None):
        inst_aval = self.normalize_inst_aval(inst_aval)
        cache_key = (
            inst_aval,
            self.normalize_institution_name(reg.get("nme_departamento_gr", "")) if reg else "",
            self.normalize_institution_name(reg.get("nme_municipio_gr", "")) if reg else "",
        )
        if cache_key in self.institution_match_cache:
            return self.institution_match_cache[cache_key]
        candidates = self.get_institution_candidates(inst_aval)
        matches = []
        for candidate in candidates:
            match = self.institution_match_score(candidate, inst_aval)
            if is_strict_institution_match(match):
                matches.append((
                    candidate,
                    match,
                    self.institution_candidate_rank(candidate, match, reg),
                ))
        if not matches:
            self.institution_match_cache[cache_key] = None
            return None
        matches.sort(key=lambda item: item[2], reverse=True)
        if len(matches) > 1:
            top_rank = matches[0][2]
            second_rank = matches[1][2]
            tied = top_rank == second_rank
            close_fuzzy = (
                not top_rank[0]
                and top_rank[:5] == second_rank[:5]
                and top_rank[5] - second_rank[5] < 3
            )
            if tied or close_fuzzy:
                self.institution_match_cache[cache_key] = None
                return None
        self.institution_match_cache[cache_key] = matches[0][0]
        return self.institution_match_cache[cache_key]

    def group_name(self, reg):
        if "nme_grupo_gr" in reg.keys():
            return reg["nme_grupo_gr"]
        return self.missing_group_names.get(reg["cod_grupo_gr"], "")

    def process_one(self, reg, collection, empty_affiliation, verbose):
        if "cod_grupo_gr" not in reg.keys() or not reg["cod_grupo_gr"]:
            return
        idgr = reg["cod_grupo_gr"]
        if idgr:
            db_reg = collection.find_one({"external_ids.id": idgr})
            if db_reg:
                if idgr not in self.inserted_cod_grupo:
                    self.inserted_cod_grupo.append(idgr)
                if "minciencias" not in [idx["source"] for idx in db_reg["updated"]]:
                    db_reg["updated"].append(
                        {"time": int(time()), "source": "minciencias"})
                if not db_reg["year_established"]:
                    date_established = check_date_format(
                        reg["fcreacion_gr"]) if "fcreacion_gr" in reg.keys() else ""
                    if date_established:
                        db_reg["year_established"] = dt.fromtimestamp(
                            date_established).year
                if not db_reg["addresses"]:
                    if not db_reg["relations"]:
                        pass
                    else:
                        if not db_reg["relations"][0]["id"]:
                            pass
                        else:
                            aff_db = collection.find_one({"_id": db_reg["relations"][0]["id"]})
                            if aff_db:
                                self.append_unique(
                                    db_reg["addresses"],
                                    self.affiliation_address_from_institution(aff_db, reg),
                                )
                self.add_aval_institution_relations(reg, db_reg, collection)
                collection.update_one(
                    {"_id": db_reg["_id"]},
                    {"$set": {
                        "updated": db_reg["updated"],
                        "year_established": db_reg.get("year_established"),
                        "addresses": db_reg.get("addresses"),
                        "relations": db_reg.get("relations")
                    }}, upsert=True)
                if verbose > 4:
                    print("Updated group {}".format(idgr))
                return

            self.inserted_cod_grupo.append(idgr)
            entry = deepcopy(empty_affiliation)
            entry["updated"].append(
                {"source": "minciencias", "time": int(time())})
            entry["names"].append(
                {"source": "minciencias", "lang": "es", "name": self.group_name(reg)})
            entry["types"].append({"source": "minciencias", "type": "group"})
            year_established = ""
            date_established = check_date_format(reg["fcreacion_gr"]) if "fcreacion_gr" in reg.keys() else ""
            if date_established:
                year_established = dt.fromtimestamp(date_established).year
            entry["year_established"] = year_established
            entry["external_ids"].append(
                {"source": "minciencias", "id": reg["cod_grupo_gr"]})
            entry["subjects"].append({
                "provenance": "minciencias",
                "source": "OECD",
                "subjects": [
                    {
                        "level": 0,
                        "name": reg["nme_gran_area_gr"] if "nme_gran_area_gr" in reg.keys() else "",
                        "id": "",
                        "external_ids": [{"source": "OECD", "id": reg["id_area_con_gr"][0] if "id_area_con_gr" in reg.keys() else ""}]
                    },
                    {
                        "level": 1,
                        "name": reg["nme_area_gr"] if "nme_area_gr" in reg.keys() else "",
                        "id": "",
                        "external_ids": [{"source": "OECD", "id": reg["id_area_con_gr"][1] if "id_area_con_gr" in reg.keys() else ""}]
                    },
                ]
            })

            self.add_aval_institution_relations(reg, entry, collection)
            entry_rank = {
                "source": "minciencias",
                "rank": reg["nme_clasificacion_gr"] if "nme_clasificacion_gr" in reg.keys() else "",
                "order": reg["orden_clas_gr"] if "orden_clas_gr" in reg.keys() else "",
                "date": check_date_format(reg["ano_convo"] if "ano_convo" in reg.keys() else ""),
            }
            entry["ranking"].append(entry_rank)
            # END CLASSIFICATION SECTION
            entry["_id"] = idgr
            self.collection.insert_one(entry)
            if verbose > 4:
                print("Inserted group {}".format(idgr))

    def process_openadata(self):
        # Pipeline to find duplicate documents and keep the one with the highest edad_anos_gr in each group
        pipeline = [
            {
                "$sort": {"ano_convo": -1}  # Sort documents by edad_anos_gr in descending order
            },
            {
                "$group": {
                    "_id": "$cod_grupo_gr",  # Group documents by the group code
                    "doc": {"$first": "$$ROOT"}  # Select the first document of each group
                }
            },
            {
                "$replaceRoot": {"newRoot": "$doc"}  # Replace the root of the document with the selected documents
            }
        ]
        affiliation_cursor = self.openadata_collection.aggregate(
            pipeline, allowDiskUse=True)
        with MongoClient(self.mongodb_url) as client:
            db = client[self.config["database_name"]]
            collection = db["affiliations"]

            Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading")(
                delayed(self.process_one)(
                    aff,
                    collection,
                    self.empty_affiliation(),
                    self.verbose,
                ) for aff in affiliation_cursor
            )
            client.close()

    def run(self):
        self.process_openadata()
        self.client.close()
        return 0
