from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from pandas import read_csv
from time import time
from datetime import datetime as dt
from joblib import Parallel, delayed


def process_one(url, db_name, empty_person, auid, cv, articulos, subset, verbose):
    client = MongoClient(url)
    db = client[db_name]
    collection = db["person"]

    reg_db = collection.find_one({"external_ids.id": auid})
    if reg_db:
        return
    if len(cv) < 1:
        return
    cv = cv.iloc[0]

    if "765376" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Femenino"
    if "1385093" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Femenino"
    if "37351" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Femenino"
    if "0000000082" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Femenino"
    if "1393305" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "1426093" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "1455061" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "37796" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "702404" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "896519" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "287938" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "1317792" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "1506130" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"
    if "34201" in cv["ID_PERSONA_PD"]:
        cv["Sexo"] = "Masculino"

    if "1340197" in cv["ID_PERSONA_PD"]:
        cv["INICIALES"] = "G"
        cv["NOMBRES"] = "Gabriel"
        cv["PRIMER APELLIDO"] = "de la Luz"
        cv["SEGUNDO APELLIDO"] = "Rodríguez"
        cv["Nombre"] = "Gabriel de la Luz Rodríguez"

    years = []
    for idx, reg in articulos.iterrows():
        years.append(int(reg["FCREACION_PD"].split("/")[-1]))
    articulos.loc[:, "year"] = years
    articulos.sort_values("year", inplace=True)

    reg = subset.iloc[-1]

    entry = empty_person.copy()
    entry["updated"].append(
        {"source": "minciencias", "time": int(time())})
    entry["full_name"] = cv["Nombre"]
    entry["first_names"] = cv["NOMBRES"].strip().split()
    entry["last_names"].extend(cv["PRIMER APELLIDO"].split("-"))
    if isinstance(cv["SEGUNDO APELLIDO"], str):
        entry["last_names"].append(cv["SEGUNDO APELLIDO"])
    entry["initials"] = cv["INICIALES"].replace(
        ".", "").replace(" ", "").strip(),
    if isinstance(cv["Nombre en citaciones"], str):
        entry["aliases"].append(cv["Nombre en citaciones"].lower())
    entry["sex"] = cv["Sexo"][0].lower()

    entry["external_ids"].append({
        "source": "minciencias",
        "id": cv["ID_PERSONA_PD"]
    })

    if isinstance(cv["Google Scholar"], str):
        if "&" == cv["Google Scholar"][-1]:
            cv["Google Scholar"] = cv["Google Scholar"][:-1]
        entry["external_ids"].append({
            "source": "scholar",
            "id": cv["Google Scholar"].split("user=")[-1].split("&")[0]
        })
    if isinstance(cv["ResearchGate"], str):
        entry["external_ids"].append({
            "source": "researchgate",
            "id": cv["ResearchGate"].split("/")[-1]
        })
    if isinstance(cv["Linkedln"], str):
        try:
            entry["external_ids"].append({
                "source": "linkedin",
                "id": cv["Linkedln"].split("in/")[-1]
            })
        except Exception as e:
            print(e)

    if isinstance(cv["ORCID"], str):
        if "orcid" not in cv["ORCID"].split("/")[-1]:
            entry["external_ids"].append({
                "source": "orcid",
                "id": cv["ORCID"].split("/")[-1]
            })
    if isinstance(cv["Scopus"], str):
        if "authorid" in cv["Scopus"].lower():
            entry["external_ids"].append({
                "source": "scopus",
                "id": cv["Scopus"].split("authorId=")[-1]
            })

    entry["subjects"].append({
        "source": "OECD",
        "subjects": [
            {
                "level": 0,
                "name": reg["NME_GRAN_AREA_GR"],
                "id":"",
                "external_ids":[{"source": "OECD", "id": reg["ID_AREA_CON_GR"][0]}]
            },
            {
                "level": 1,
                "name": reg["NME_AREA_GR"],
                "id":"",
                "external_ids":[{"source": "OECD", "id": reg["ID_AREA_CON_GR"][1]}]
            },
        ]
    })
    groups_cod = []
    inst_cod = []
    # print("Adding affiliations to ",auid)
    for idx, reg in articulos.iterrows():
        if reg["COD_GRUPO_GR"] in groups_cod:
            continue
        groups_cod.append(reg["COD_GRUPO_GR"])
        group_db = db["affiliations"].find_one(
            {"external_ids.id": reg["COD_GRUPO_GR"]})
        if group_db:
            name = group_db["names"][0]["name"]
            for n in group_db["names"]:
                if n["lang"] == "es":
                    name = n["name"]
                    break
                elif n["lang"] == "en":
                    name = n["name"]
            entry["affiliations"].append({
                "name": name,
                "id": group_db["_id"],
                "types": group_db["types"],
                "start_date": reg["FCREACION_PD"].split("/")[-1],
                "end_date": "",
                "position": ""
            })
            if "relations" in group_db.keys():
                if group_db["relations"]:
                    for rel in group_db["relations"]:
                        if rel["id"] in inst_cod:
                            continue
                        inst_cod.append(rel["id"])
                        if "names" in rel.keys():
                            name = rel["names"][0]["name"]
                            for n in rel["names"]:
                                if n["lang"] == "es":
                                    name = n["name"]
                                    break
                                elif n["lang"] == "en":
                                    name = n["name"]
                        else:
                            name = rel["name"]
                        entry["affiliations"].append({
                            "name": name,
                            "id": rel["id"],
                            "types": rel["types"] if "types" in rel.keys() else [],
                            "start_date": reg["FCREACION_PD"].split("/")[-1],
                            "end_date": "",
                            "position": ""
                        })
    # print("Adding ranks to ", auid)
    for idx, reg in subset.iterrows():
        try:
            entry_rank = {
                "source": "minciencias",
                "rank": reg["NME_CLAS_PR"],
                "id": reg["ID_CLAS_PR"],
                "order": reg["ORDEN_CLAS_PR"],
                "date": int(dt.strptime(reg["ANO_CONVO"], "%d/%m/%Y").timestamp())
            }
            entry["ranking"].append(entry_rank)
        except Exception as e:
            if verbose > 4:
                print(e)

    collection.insert_one(entry)


class Kahi_minciencias_opendata_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name.name", TEXT)])

        self.researchers_file = config["minciencias_opendata_person"]["researchers"]
        self.cvlac_file = config["minciencias_opendata_person"]["cvlac"]
        self.groups_file = config["minciencias_opendata_person"]["groups_production"]

        self.investigadores_minciencias = read_csv(
            self.researchers_file, dtype={"ID_PERSONA_PR": str})
        self.cvlac = read_csv(self.cvlac_file, dtype={"ID_PERSONA_PD": str})
        self.articulos_grupos = read_csv(
            self.groups_file, dtype={"ID_PERSONA_PD": str})

        self.n_jobs = config["minciencias_opendata_person"]["num_jobs"] if "num_jobs" in config["minciencias_opendata_person"].keys(
        ) else 1

        self.verbose = config["minciencias_opendata_person"][
            "verbose"] if "verbose" in config["minciencias_opendata_person"].keys() else 0

    def process_openadata(self):
        Parallel(
            n_jobs=self.n_jobs,
            verbose=10,
            backend="multiprocessing")(
            delayed(process_one)(
                self.mongodb_url,
                self.config["database_name"],
                self.empty_person(),
                auid,
                self.cvlac[self.cvlac["ID_PERSONA_PD"] == auid],
                self.articulos_grupos[self.articulos_grupos["ID_PERSONA_PD"] == auid],
                self.investigadores_minciencias[self.investigadores_minciencias["ID_PERSONA_PR"] == auid],
                self.verbose
            ) for auid in self.investigadores_minciencias["ID_PERSONA_PR"].unique()
        )

    def run(self):
        self.process_openadata()
        return 0
