from kahi.KahiBase import KahiBase
from copy import deepcopy
from datetime import datetime
from pymongo import MongoClient, ASCENDING, TEXT
from pymongo.errors import DuplicateKeyError
import re
from time import time
from thefuzz import fuzz
from unicodedata import normalize as unicode_normalize
from urllib.parse import urlparse
from kahi_impactu_utils.Utils import check_date_format


SOURCE = "scienti"
MINCIENCIAS_SOURCE = "minciencias"
INVALID_TEXT_VALUES = {
    "",
    "-",
    "n/a",
    "na",
    "n.a.",
    "nd",
    "ne",
    "no",
    "no aplica",
    "no tiene",
    "none",
    "null",
    "pendiente",
    "prueba",
}
INVALID_NITS = {
    "000000000",
    "000000010",
    "111111111",
    "123456789",
    "999999999",
}


def clean_text(value):
    """Return source text without accepting known placeholder values."""
    if value is None:
        return ""
    value = str(value).strip()
    return "" if value.lower() in INVALID_TEXT_VALUES else value


def normalize_institution_name(value):
    """Normalize an institution name for matching without changing stored text."""
    value = unicode_normalize("NFKD", clean_text(value)).encode(
        "ascii", "ignore").decode("ascii").lower()
    value = re.sub(r"[^\w\s]", " ", value)
    return " ".join(value.split())


def append_unique(items, item):
    if item and item not in items:
        items.append(item)


def append_unique_external_id(items, item):
    """Deduplicate identifiers by source and value, regardless of provenance."""
    if not item.get("id"):
        return
    if not any(
            ext.get("source") == item["source"] and ext.get("id") == item["id"]
            for ext in items):
        items.append(item)


def first_dict(value):
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    return {}


def source_country_code(value):
    """Normalize only country codes whose meaning is explicit in the source."""
    value = clean_text(value).upper()
    if value in {"1", "COL", "CO"}:
        return "CO"
    return value if len(value) == 2 and value.isalpha() else ""


def scienti_country(reg):
    """Return the best supported country name/code pair from an institution."""
    direct = first_dict(reg.get("country"))
    city = first_dict(reg.get("city"))
    city_department = first_dict(city.get("department"))
    nested = first_dict(city_department.get("country"))
    raw_country = clean_text(reg.get("SGL_PAIS")).upper()

    for country in [direct, nested]:
        nested_raw = clean_text(country.get("SGL_PAIS")).upper()
        if raw_country and not raw_country.isdigit() and nested_raw and raw_country != nested_raw:
            continue
        name = clean_text(country.get("TXT_NME_PAIS"))
        code = clean_text(country.get("SGL_PAIS_ISO_2")).upper()
        if name or code:
            return name, code

    if source_country_code(raw_country) == "CO":
        return "Colombia", "CO"
    return "", source_country_code(raw_country)


def scienti_address(reg):
    """Build a standard address, rejecting known cross-country join conflicts."""
    country, country_code = scienti_country(reg)
    raw_country = clean_text(reg.get("SGL_PAIS")).upper()
    raw_department = clean_text(reg.get("SGL_DEPARTAMENTO")).upper()
    department = first_dict(reg.get("department"))
    city_reg = first_dict(reg.get("city"))

    state = clean_text(department.get("TXT_NME_DEPARTAMENTO"))
    department_country = clean_text(department.get("SGL_PAIS")).upper()
    if state and raw_country and not raw_country.isdigit() and department_country not in {"", raw_country}:
        state = ""

    city = clean_text(reg.get("TXT_CIUDAD_INST"))
    if not city and city_reg:
        city_country = clean_text(city_reg.get("SGL_PAIS")).upper()
        city_department_code = clean_text(city_reg.get("SGL_DEPARTAMENTO")).upper()
        country_matches = not raw_country or raw_country.isdigit() or not city_country or (
            source_country_code(raw_country) == source_country_code(city_country)
            if source_country_code(raw_country) and source_country_code(city_country)
            else raw_country == city_country
        )
        department_matches = (
            not raw_department
            or raw_department.isdigit()
            or not city_department_code
            or raw_department == city_department_code
        )
        if country_matches and department_matches:
            city = clean_text(city_reg.get("TXT_NME_MUNICIPIO"))
            if not state:
                city_department = first_dict(city_reg.get("department"))
                state = clean_text(city_department.get("TXT_NME_DEPARTAMENTO"))

    if not any([state, city, country, country_code]):
        return None
    return {
        "lat": "",
        "lng": "",
        "postcode": "",
        "state": state,
        "city": city,
        "country": country,
        "country_code": country_code,
    }


def normalize_nit(reg):
    """Return a plausible Colombian NIT without its verification digit."""
    _, country_code = scienti_country(reg)
    if country_code and country_code != "CO":
        return ""
    raw = clean_text(reg.get("TXT_NIT")).split("-", maxsplit=1)[0]
    digits = "".join(char for char in raw if char.isdigit())
    if not 7 <= len(digits) <= 10:
        return ""
    if digits in INVALID_NITS or len(set(digits)) == 1:
        return ""
    return digits


def normalize_url(value):
    """Return a usable public URL, adding a scheme when necessary."""
    value = clean_text(value)
    if not value or " " in value or "@" in value:
        return ""
    if not value.startswith(("http://", "https://")):
        value = "https://" + value
    parsed = urlparse(value)
    if not parsed.hostname or "." not in parsed.hostname:
        return ""
    if parsed.hostname.lower() in {"example.com", "www.example.com"}:
        return ""
    return value


def parse_scienti_datetime(value):
    if isinstance(value, datetime):
        return value
    value = clean_text(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def scienti_year_established(reg):
    date = parse_scienti_datetime(reg.get("DTA_CONSTITUCION"))
    if date and 1500 <= date.year <= datetime.now().year:
        return date.year
    return None


def scienti_updated_time(reg):
    date = parse_scienti_datetime(reg.get("DTA_ACTUALIZACION"))
    return int(date.timestamp()) if date else int(time())


def scienti_external_ids(reg):
    """Map stable identifiers into the existing affiliation schema."""
    identifiers = []
    cod_inst = clean_text(reg.get("COD_INST"))
    if cod_inst and cod_inst != "000000000000":
        identifiers.append({
            "provenance": SOURCE,
            "source": MINCIENCIAS_SOURCE,
            "id": cod_inst,
        })
    internal_id = clean_text(reg.get("ID_INSTITUCION"))
    if internal_id:
        identifiers.append({
            "provenance": SOURCE,
            "source": SOURCE,
            "id": internal_id,
        })
    nit = normalize_nit(reg)
    if nit:
        identifiers.append({
            "provenance": SOURCE,
            "source": "nit",
            "id": nit,
        })
    cod_ies = clean_text(reg.get("COD_IES"))
    if cod_ies and cod_ies.isdigit():
        identifiers.append({
            "provenance": SOURCE,
            "source": "snies",
            "id": cod_ies,
        })
    return identifiers


def mark_scienti_updated(entry, reg):
    updated_time = scienti_updated_time(reg)
    for item in entry["updated"]:
        if item.get("source") == SOURCE:
            item["time"] = max(item.get("time", 0), updated_time)
            return
    entry["updated"].append({"source": SOURCE, "time": updated_time})


def merge_scienti_address(entry, address):
    """Fill missing address values without replacing better upstream geography."""
    if not address:
        return
    if not entry["addresses"]:
        entry["addresses"].append(address)
        return
    current = entry["addresses"][0]
    current_country = clean_text(current.get("country_code")) or clean_text(
        current.get("country"))
    source_country = clean_text(address.get("country_code")) or clean_text(
        address.get("country"))
    if current_country and source_country and current_country.lower() != source_country.lower():
        return
    for field in ["state", "city", "country", "country_code"]:
        if not current.get(field) and address.get(field):
            current[field] = address[field]


def apply_scienti_institution_data(entry, reg):
    """Enrich an affiliation using only fields in the current entity schema."""
    for field in [
            "updated", "names", "aliases", "abbreviations", "types", "status",
            "relations", "addresses", "external_urls", "external_ids", "subjects",
            "ranking", "description", "citation_count"]:
        entry.setdefault(field, [])

    name = clean_text(reg.get("NME_INST"))
    if name:
        append_unique(entry["names"], {
            "source": SOURCE, "name": name, "lang": "es"})

    abbreviation = clean_text(reg.get("SGL_INST"))
    if abbreviation and normalize_institution_name(abbreviation) != normalize_institution_name(name):
        append_unique(entry["abbreviations"], abbreviation)

    for external_id in scienti_external_ids(reg):
        append_unique_external_id(entry["external_ids"], external_id)

    inferred_type = "education" if clean_text(reg.get("COD_IES")) else "other"
    if inferred_type == "education" or not entry["types"]:
        append_unique(entry["types"], {
            "provenance": SOURCE, "source": SOURCE, "type": inferred_type})

    year = scienti_year_established(reg)
    if year and entry.get("year_established") in {None, "", -1}:
        entry["year_established"] = year

    merge_scienti_address(entry, scienti_address(reg))

    website = normalize_url(reg.get("URL_HOME_PAGE"))
    if website:
        append_unique(entry["external_urls"], {
            "provenance": SOURCE, "source": "site", "url": website})

    mark_scienti_updated(entry, reg)
    return entry


def normalize_scienti_rank(value):
    """Translate the Scienti code for recognized groups."""
    return "Reconocido" if value == "00" else value


def inherit_parent_addresses(entry, parents):
    """Copy unique parent addresses when the affiliation has none."""
    if entry.get("addresses"):
        return False
    for parent in parents:
        for address in parent.get("addresses", []):
            if address not in entry["addresses"]:
                entry["addresses"].append(deepcopy(address))
    return bool(entry["addresses"])


def institution_record_score(reg):
    """Prefer the most complete copy when catalogs overlap."""
    fields = [
        "NME_INST", "SGL_INST", "TXT_NIT", "TXT_DIGITO_VERIFICADOR",
        "COD_IES", "URL_HOME_PAGE", "DTA_CONSTITUCION", "DTA_ACTUALIZACION",
        "TXT_CIUDAD_INST", "country", "department", "city",
    ]
    return sum(bool(reg.get(field)) for field in fields)


def candidate_names(affiliation):
    return [
        item.get("name")
        for item in affiliation.get("names", [])
        if item.get("name")
    ]


def institution_name_match(affiliation, source_names):
    """Return the strongest strict name match for an affiliation."""
    best = None
    stopwords = {"de", "del", "la", "las", "los", "el", "y", "e", "en", "para", "por", "a"}
    for source_name in source_names:
        source_name = normalize_institution_name(source_name)
        if not source_name:
            continue
        source_tokens = set(source_name.split()) - stopwords
        for name in candidate_names(affiliation):
            normalized_name = normalize_institution_name(name)
            if not normalized_name:
                continue
            name_tokens = set(normalized_name.split()) - stopwords
            overlap = len(source_tokens & name_tokens) / max(
                1, min(len(source_tokens), len(name_tokens)))
            scores = {
                "ratio": fuzz.ratio(normalized_name, source_name),
                "token_sort_ratio": fuzz.token_sort_ratio(normalized_name, source_name),
                "token_set_ratio": fuzz.token_set_ratio(normalized_name, source_name),
                "wratio": fuzz.WRatio(normalized_name, source_name),
            }
            exact = normalized_name == source_name
            strict = any([
                scores["ratio"] >= 94,
                scores["token_sort_ratio"] >= 96,
                scores["token_set_ratio"] >= 98 and overlap >= 0.80 and min(
                    len(source_tokens), len(name_tokens)) >= 2,
                scores["wratio"] >= 96 and overlap >= 0.80 and min(
                    len(source_tokens), len(name_tokens)) >= 2,
            ])
            current = {
                "exact": exact,
                "strict": strict,
                "score": max(scores.values()),
                "overlap": overlap,
            }
            if best is None or (current["exact"], current["score"], current["overlap"]) > (
                    best["exact"], best["score"], best["overlap"]):
                best = current
    return best


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
            try:
                database_name = db_info["database_name"]
                if database_name not in client.list_database_names():
                    raise Exception("Database {} not found".format(database_name))
                available = client[database_name].list_collection_names()
                required = [
                    db_info["collection_name"],
                    db_info.get("institution_collection_name", "institution"),
                ]
                for collection_name in required:
                    if collection_name not in available:
                        raise Exception(
                            "Collection {}.{} not found in {}".format(
                                database_name, collection_name, db_info["database_url"])
                        )
            finally:
                client.close()

    def create_source_indexes(self):
        for db_info in self.config["scienti_affiliations"]["databases"]:
            database_url = db_info.get('database_url', '')
            database_name = db_info.get('database_name', '')
            collection_name = db_info.get('collection_name', '')
            institution_collection_name = db_info.get(
                "institution_collection_name", "institution")

            if database_url and database_name and collection_name:
                client = MongoClient(database_url)
                try:
                    db = client[database_name]
                    collection = db[collection_name]
                    collection.create_index(
                        [('group.institution.TXT_NIT', ASCENDING)])
                    collection.create_index([('group.COD_ID_GRUPO', ASCENDING)])
                    institutions = db[institution_collection_name]
                    institutions.create_index([('COD_INST', ASCENDING)])
                    institutions.create_index([('ID_INSTITUCION', ASCENDING)])
                    institutions.create_index([('COD_INST_MACRO', ASCENDING)])
                finally:
                    client.close()

    def load_institution_catalog(self, configs):
        """Load one best copy of every COD_INST from overlapping Scienti dumps."""
        catalog = {}
        for config in configs:
            client = MongoClient(config["database_url"])
            try:
                collection_name = config.get(
                    "institution_collection_name", "institution")
                institutions = client[config["database_name"]][collection_name]
                for reg in institutions.find({}, {"_id": 0}):
                    cod_inst = clean_text(reg.get("COD_INST"))
                    name = clean_text(reg.get("NME_INST"))
                    if not cod_inst or cod_inst == "000000000000" or not name:
                        continue
                    previous = catalog.get(cod_inst)
                    if previous is None or institution_record_score(reg) > institution_record_score(previous):
                        catalog[cod_inst] = reg
            finally:
                client.close()
        return catalog

    def catalog_unique_values(self, catalog, field):
        """Return values associated with only one logical Scienti institution."""
        owners = {}
        for cod_inst, reg in catalog.items():
            if field == "TXT_NIT":
                value = normalize_nit(reg)
            else:
                value = clean_text(reg.get(field))
            if not value:
                continue
            owner = clean_text(reg.get("ID_INSTITUCION")) or cod_inst
            owners.setdefault(value, set()).add(owner)
        return {value for value, values in owners.items() if len(values) == 1}

    def find_by_external_id(self, source, value):
        if not value:
            return None
        return self.collection.find_one({
            "external_ids": {"$elemMatch": {"source": source, "id": value}},
            "types.type": {"$ne": "group"},
        })

    def institution_candidates(self, reg):
        source_names = [reg.get("NME_INST"), reg.get("NME_INST_FILTRO")]
        candidates = []
        seen = set()
        name = clean_text(reg.get("NME_INST"))
        if name:
            direct = self.collection.find({
                "names.name": {"$regex": "^{}$".format(re.escape(name)), "$options": "i"},
                "types.type": {"$ne": "group"},
            }).limit(20)
            for candidate in direct:
                if candidate["_id"] not in seen:
                    candidates.append(candidate)
                    seen.add(candidate["_id"])

        query = normalize_institution_name(reg.get("NME_INST_FILTRO")) or normalize_institution_name(name)
        if query:
            cursor = self.collection.find({
                "$text": {"$search": query},
                "types.type": {"$ne": "group"},
            }, {"names": 1, "addresses": 1, "external_ids": 1, "relations": 1,
                "score": {"$meta": "textScore"}}).sort(
                    [("score", {"$meta": "textScore"})]).limit(100)
            for candidate in cursor:
                if candidate["_id"] not in seen:
                    candidates.append(candidate)
                    seen.add(candidate["_id"])
        return candidates, source_names

    def countries_compatible(self, affiliation, reg):
        _, source_code = scienti_country(reg)
        if not source_code:
            return True
        target_codes = {
            clean_text(address.get("country_code")).upper()
            for address in affiliation.get("addresses", [])
            if clean_text(address.get("country_code"))
        }
        return not target_codes or source_code in target_codes

    def has_different_scienti_code(self, affiliation, cod_inst):
        codes = {
            ext.get("id")
            for ext in affiliation.get("external_ids", [])
            if ext.get("source") == MINCIENCIAS_SOURCE
            and re.fullmatch(r"\d{12}", str(ext.get("id", "")))
        }
        return bool(codes and cod_inst not in codes)

    def match_institution_by_name(self, reg):
        candidates, source_names = self.institution_candidates(reg)
        matches = []
        cod_inst = clean_text(reg.get("COD_INST"))
        for candidate in candidates:
            if not self.countries_compatible(candidate, reg):
                continue
            if self.has_different_scienti_code(candidate, cod_inst):
                continue
            match = institution_name_match(candidate, source_names)
            if not match or not (match["exact"] or match["strict"]):
                continue
            has_authoritative_id = any(
                ext.get("source") in {"ror", "snies"}
                for ext in candidate.get("external_ids", []))
            matches.append((candidate, match, has_authoritative_id))
        if not matches:
            return None
        matches.sort(
            key=lambda item: (
                item[1]["exact"], item[2], item[1]["score"], item[1]["overlap"]),
            reverse=True,
        )
        if len(matches) > 1:
            first = matches[0]
            second = matches[1]
            first_rank = (first[1]["exact"], first[2], first[1]["score"], first[1]["overlap"])
            second_rank = (second[1]["exact"], second[2], second[1]["score"], second[1]["overlap"])
            if first_rank == second_rank:
                return None
        return self.collection.find_one({"_id": matches[0][0]["_id"]})

    def find_scienti_institution(self, reg, unique_nits, unique_cod_ies):
        """Match by stable identifiers first, then by an unambiguous strict name."""
        cod_inst = clean_text(reg.get("COD_INST"))
        institution = self.find_by_external_id(MINCIENCIAS_SOURCE, cod_inst)
        if institution:
            return institution

        internal_id = clean_text(reg.get("ID_INSTITUCION"))
        institution = self.find_by_external_id(SOURCE, internal_id)
        if institution:
            return institution

        nit = normalize_nit(reg)
        if nit in unique_nits:
            institution = self.find_by_external_id("nit", nit)
            if institution:
                return institution

        cod_ies = clean_text(reg.get("COD_IES"))
        if cod_ies in unique_cod_ies:
            institution = self.find_by_external_id("snies", cod_ies)
            if institution:
                return institution
        return self.match_institution_by_name(reg)

    def save_scienti_institution(self, institution, reg):
        if institution:
            entry = apply_scienti_institution_data(institution, reg)
            self.collection.update_one(
                {"_id": entry["_id"]},
                {"$set": {
                    "updated": entry["updated"],
                    "names": entry["names"],
                    "abbreviations": entry["abbreviations"],
                    "types": entry["types"],
                    "year_established": entry.get("year_established"),
                    "addresses": entry["addresses"],
                    "external_urls": entry["external_urls"],
                    "external_ids": entry["external_ids"],
                }},
            )
            return entry["_id"], False

        cod_inst = clean_text(reg.get("COD_INST"))
        entry = apply_scienti_institution_data(self.empty_affiliation(), reg)
        entry["_id"] = "scienti_{}".format(cod_inst)
        try:
            self.collection.insert_one(entry)
            return entry["_id"], True
        except DuplicateKeyError:
            current = self.collection.find_one({"_id": entry["_id"]})
            return self.save_scienti_institution(current, reg)[0], False

    def add_catalog_relations(self, catalog, cod_to_affiliation):
        changed = 0
        for cod_inst, reg in catalog.items():
            macro = clean_text(reg.get("COD_INST_MACRO"))
            child_id = cod_to_affiliation.get(cod_inst)
            parent_id = cod_to_affiliation.get(macro)
            if not macro or macro == cod_inst or not child_id or not parent_id or child_id == parent_id:
                continue
            child = self.collection.find_one({"_id": child_id})
            parent = self.collection.find_one({"_id": parent_id})
            if not child or not parent:
                continue
            parent_name = next(iter(candidate_names(parent)), "")
            relation = {
                "id": parent_id,
                "name": parent_name,
                "types": parent.get("types", []),
            }
            relations = child.get("relations", [])
            existing = next(
                (item for item in relations if item.get("id") == parent_id), None)
            if existing == relation:
                continue
            if existing:
                existing.update(relation)
            else:
                relations.append(relation)
            self.collection.update_one(
                {"_id": child_id}, {"$set": {"relations": relations}})
            changed += 1
        return changed

    def process_scienti_institutions(self, configs, verbose=0):
        """Consolidate and ingest the root institution catalogs before groups."""
        if isinstance(configs, dict):
            configs = [configs]
        catalog = self.load_institution_catalog(configs)
        unique_nits = self.catalog_unique_values(catalog, "TXT_NIT")
        unique_cod_ies = self.catalog_unique_values(catalog, "COD_IES")
        cod_to_affiliation = {}
        inserted = 0

        records = sorted(
            catalog.items(),
            key=lambda item: (
                clean_text(item[1].get("COD_INST_MACRO")) != item[0], item[0]),
        )
        for cod_inst, reg in records:
            institution = self.find_scienti_institution(
                reg, unique_nits, unique_cod_ies)
            affiliation_id, was_inserted = self.save_scienti_institution(
                institution, reg)
            cod_to_affiliation[cod_inst] = affiliation_id
            inserted += int(was_inserted)

        relations = self.add_catalog_relations(catalog, cod_to_affiliation)
        if verbose > 0:
            print(
                "Processed {} Scienti institutions: {} inserted, {} enriched, "
                "{} hierarchy relations".format(
                    len(catalog), inserted, len(catalog) - inserted, relations)
            )
        return cod_to_affiliation

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

    def embedded_parent_institution(self, inst):
        """Resolve an embedded group institution using catalog identifiers first."""
        cod_inst = clean_text(inst.get("COD_INST"))
        institution = self.find_by_external_id(MINCIENCIAS_SOURCE, cod_inst)
        if institution:
            return institution

        internal_id = clean_text(inst.get("ID_INSTITUCION"))
        institution = self.find_by_external_id(SOURCE, internal_id)
        if institution:
            return institution

        nit = normalize_nit(inst)
        if not nit:
            return None
        institution = self.find_by_external_id("nit", nit)
        if institution:
            return institution
        verification_digit = clean_text(inst.get("TXT_DIGITO_VERIFICADOR"))
        legacy_nit = "{}-{}".format(nit, verification_digit) if verification_digit else ""
        return self.find_by_external_id("nit", legacy_nit)

    def group_parent_institutions(self, scienti, group_id):
        parents = []
        seen = set()
        for reg in scienti.find({"group.COD_ID_GRUPO": group_id}):
            for group in reg.get("group", []):
                if group.get("COD_ID_GRUPO") != group_id:
                    continue
                for inst in group.get("institution", []):
                    parent = self.embedded_parent_institution(inst)
                    if parent and parent["_id"] not in seen:
                        parents.append(parent)
                        seen.add(parent["_id"])
        return parents

    def enrich_group_parents(self, entry, parents):
        changed = False
        for parent in parents:
            name = ""
            for item in parent.get("names", []):
                if item.get("name") and item.get("lang") == "es":
                    name = item["name"]
                    break
                if item.get("name") and not name:
                    name = item["name"]
            relation = {
                "name": name,
                "id": parent["_id"],
                "types": parent.get("types", []),
            }
            current = next(
                (item for item in entry.get("relations", [])
                 if item.get("id") == parent["_id"]), None)
            if current is None:
                entry["relations"].append(relation)
                changed = True
            elif current != relation:
                current.update(relation)
                changed = True
        return inherit_parent_addresses(entry, parents) or changed

    def process_scienti_groups(self, config, verbose=0):
        client = MongoClient(config["database_url"])
        db = client[config["database_name"]]
        scienti = db[config["collection_name"]]
        for group_id in scienti.distinct("group.COD_ID_GRUPO", {"group.COD_ID_GRUPO": {"$ne": None}}):
            db_reg = self.collection.find_one({"external_ids.id": group_id})
            parent_institutions = self.group_parent_institutions(
                scienti, group_id)
            if db_reg:
                if self.enrich_group_parents(db_reg, parent_institutions):
                    self.collection.update_one(
                        {"_id": db_reg["_id"]},
                        {"$set": {
                            "addresses": db_reg["addresses"],
                            "relations": db_reg["relations"],
                        }},
                    )
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

                self.enrich_group_parents(entry, parent_institutions)
                entry["_id"] = group_id
                self.collection.insert_one(entry)
        client.close()

    def run(self):
        if self.verbose > 4:
            start_time = time()
        configs = self.config["scienti_affiliations"]["databases"]
        self.process_scienti_institutions(configs, verbose=self.verbose)
        for config in configs:
            if self.verbose > 0:
                print("Processing {}.{} database".format(
                    config["database_name"], config["collection_name"]))
            self.process_scienti_groups(config, verbose=self.verbose)
        if self.verbose > 4:
            print("Execution time: {} minutes".format(
                round((time() - start_time) / 60, 2)))
        return 0
