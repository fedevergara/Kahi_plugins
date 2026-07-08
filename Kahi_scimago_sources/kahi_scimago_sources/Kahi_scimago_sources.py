from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from datetime import datetime as dt
from time import time
from pandas import read_csv
import iso3166
import re


class Kahi_scimago_sources(KahiBase):

    config = {}

    _CAT_RE = re.compile(r"^(?P<name>.+?)\s*\((?P<q>Q[1-4])\)\s*$")

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]

        self.collection.create_index("external_ids.id")

        self.scimago_config = self.config["scimago_sources"]
        self.scimago_file_paths = self.scimago_config.get("file_path")
        self.scimago_collection = None

        if not self.scimago_file_paths:
            self.scimago_client = MongoClient(self.scimago_config["database_url"])
            database_name = self.scimago_config["database_name"]
            collection_name = self.scimago_config["collection_name"]

            if database_name not in self.scimago_client.list_database_names():
                raise Exception(
                    f"Database {database_name} missing from {self.scimago_config['database_url']}"
                )

            self.scimago_db = self.scimago_client[database_name]
            if collection_name not in self.scimago_db.list_collection_names():
                raise Exception(
                    f"Collection {database_name}.{collection_name} missing from {database_name}"
                )

            self.scimago_collection = self.scimago_db[collection_name]

        self.verbose = self.scimago_config.get("verbose", 0)

        self.already_in_db = []

    def _get_value(self, reg, key, default=None):
        if hasattr(reg, "get"):
            value = reg.get(key, default)
        else:
            value = reg[key] if key in reg else default
        return default if value is None else value

    def _normalize_spaces(self, s: str) -> str:
        return " ".join(s.strip().split())

    def _iter_issns(self, issn_list):
        if not issn_list:
            return []
        issns = []
        for issn in str(issn_list).split(","):
            issn = issn.strip()
            if len(issn) >= 8:
                issns.append(issn)
        return issns

    def _sourceid_values(self, sjr):
        sourceid = self._get_value(sjr, "Sourceid")
        if sourceid is None or sourceid == "":
            return []
        values = [str(sourceid)]
        try:
            numeric = int(sourceid)
        except (TypeError, ValueError):
            numeric = None
        if numeric is not None:
            values.append(numeric)
        return values

    def _find_by_scimago_sourceid(self, sjr):
        values = self._sourceid_values(sjr)
        if not values:
            return None
        return self.collection.find_one(
            {
                "external_ids": {
                    "$elemMatch": {
                        "source": "scimago",
                        "id": {"$in": values},
                    }
                }
            }
        )

    def parse_scimago_categories(self, raw: str):
        """
        Convert a raw scimago categories string into a list of dicts with
        'category_name' and 'quartile' keys.

        Args:
            raw (str): Raw categories string from scimago data.
        Returns:
            List[Dict[str, Optional[str]]]: List of parsed categories.
        """
        if not raw or not isinstance(raw, str):
            return []

        out = []
        for part in raw.split(";"):
            item = self._normalize_spaces(part)
            if not item:
                continue

            m = self._CAT_RE.match(item)
            if m:
                out.append({
                    "category_name": self._normalize_spaces(m.group("name")),
                    "quartile": m.group("q").strip()
                })
            else:
                out.append({"category_name": item, "quartile": None})

        seen = set()
        uniq = []
        for x in out:
            key = (x["category_name"], x["quartile"])
            if key not in seen:
                seen.add(key)
                uniq.append(x)
        return uniq

    def upsert_scimago_category_rankings(self, sjr, entry):
        """
        Upsert scimago category rankings into the entry's ranking list.
        Args:
            sjr (pd.Series): Scimago journal record.
            entry (dict): The source entry to update.
        """
        entry.setdefault("ranking", [])

        from_ts = int(self.scimago_start_ts)
        to_ts = int(self.scimago_end_ts)
        order_val = int(self._get_value(sjr, "Rank")) if self._get_value(sjr, "Rank") else None

        cats = self.parse_scimago_categories(self._get_value(sjr, "Categories", ""))

        existing = set()
        for r in entry["ranking"]:
            if r.get("source") == "scimago Category Quartile":
                existing.add((r.get("from_date"), r.get("to_date"), r.get("category_name")))

        for c in cats:
            cat_name = c["category_name"]
            q = c["quartile"]  # Q1..Q4
            key = (from_ts, to_ts, cat_name)
            if key in existing:
                continue

            entry["ranking"].append({
                "to_date": to_ts,
                "from_date": from_ts,
                "rank": q,
                "order": order_val,
                "source": "scimago Category Quartile",
                "category_name": cat_name
            })

    def update_scimago(self, sjr, entry):
        _id = entry["_id"]
        del (entry["_id"])

        if "scimago" not in [upd["source"] for upd in entry["updated"]]:
            entry["updated"].append(
                {"source": "scimago", "time": int(time())})

        found_scimago_name = False
        for name in entry["names"]:
            if name.get("name") == self._get_value(sjr, "Title") and name.get("source") == "scimago":
                found_scimago_name = True
                break
        if not found_scimago_name:
            entry["names"].append(
                {"lang": "en", "name": self._get_value(sjr, "Title"), "source": "scimago"})
        sourceid = str(self._get_value(sjr, "Sourceid"))
        found_scimagoid = False
        for ext in entry["external_ids"]:
            if ext["source"] == "scimago" and str(ext.get("id")) == sourceid:
                found_scimagoid = True
            elif ext["source"] == "scimago":
                ext["id"] = str(ext.get("id"))
        if not found_scimagoid:
            entry["external_ids"].append(
                {"source": "scimago", "id": sourceid})
        found_scimago_type = False
        for typ in entry["types"]:
            if typ["source"] == "scimago":
                found_scimago_type = True
                break
        if not found_scimago_type:
            entry["types"].append({"source": "scimago", "type": self._get_value(sjr, "Type")})

        ids = [extid["id"] for extid in entry["external_ids"]]
        for extid in self._iter_issns(self._get_value(sjr, "Issn")):
            extid = extid.strip()
            extid = extid[:4] + "-" + extid[4:]
            if extid not in ids:
                entry["external_ids"].append({"source": "issn", "id": extid})

        rankings = [(rank["source"], rank["from_date"], rank["to_date"])
                    for rank in entry["ranking"]]

        if ("scimago Best Quartile", self.scimago_start_ts, self.scimago_end_ts) not in rankings:
            entry["ranking"].append({
                "to_date": self.scimago_end_ts,
                "from_date": self.scimago_start_ts,
                "rank": self._get_value(sjr, "SJR Best Quartile"),
                "order": int(self._get_value(sjr, "Rank")) if self._get_value(sjr, "Rank") else None,
                "source": "scimago Best Quartile"
            })
        if ("scimago hindex", self.scimago_start_ts, self.scimago_end_ts) not in rankings:
            entry["ranking"].append({
                "to_date": self.scimago_end_ts,
                "from_date": self.scimago_start_ts,
                "rank": int(self._get_value(sjr, "H index")),
                "order": int(self._get_value(sjr, "Rank")) if self._get_value(sjr, "Rank") else None,
                "source": "scimago hindex"
            })
        if self._get_value(sjr, "Open Access") and self._get_value(sjr, "Open Access Diamond"):
            oa_rec = ({
                "provenance": "scimago",
                "is_open_access": True if self._get_value(sjr, "Open Access") == "Yes" else False,
                "open_access_diamond": True if self._get_value(sjr, "Open Access Diamond") == "Yes" else False
            })
            if oa_rec not in entry["open_access"]:
                entry["open_access"].append(oa_rec)
        if ("scimago", self.scimago_start_ts, self.scimago_end_ts) not in rankings:
            if self._get_value(sjr, "SJR"):
                rank = ""
                if isinstance(self._get_value(sjr, "SJR"), str):
                    rank = float(self._get_value(sjr, "SJR").replace(",", "."))
                else:
                    rank = self._get_value(sjr, "SJR")
                entry["ranking"].append({
                    "to_date": self.scimago_end_ts,
                    "from_date": self.scimago_start_ts,
                    "rank": rank,
                    "order": int(self._get_value(sjr, "Rank")) if self._get_value(sjr, "Rank") else None,
                    "source": "scimago"
                })
        # Upsert category rankings
        self.upsert_scimago_category_rankings(sjr, entry)

        scimago_subjects = []
        for cat in self._get_value(sjr, "Categories", "").split(";"):
            cat_clean = self._normalize_spaces(cat)
            m = self._CAT_RE.match(cat_clean)
            if m:
                cat_name = self._normalize_spaces(m.group("name"))
            else:
                cat_name = cat_clean.split(" (")[0].strip()
            scimago_subjects.append({
                "id": "",
                "name": cat_name,
                "level": None,
                "external_ids": []
            })
        found_scimago_subjects = False
        scimago_subjects_index = -1
        for sub in entry["subjects"]:
            if sub["source"] == "scimago":
                found_scimago_subjects = True
                scimago_subjects_index = entry["subjects"].index(sub)
                break
        if found_scimago_subjects:
            for sub in scimago_subjects:
                if sub not in entry["subjects"][scimago_subjects_index]["subjects"]:
                    entry["subjects"][scimago_subjects_index]["subjects"].append(
                        sub)
        else:
            entry["subjects"].append({
                "source": "scimago",
                "subjects": scimago_subjects
            })

        self.collection.update_one({"_id": _id}, {"$set": entry})

    def process_scimago(self):
        for sjr in self.scimago:
            issn_list = self._get_value(sjr, "Issn")
            db_reg = self._find_by_scimago_sourceid(sjr)
            ext_ids = []
            found_issn = None
            if not db_reg:
                for issn in self._iter_issns(issn_list):
                    issn = issn.strip()
                    extid = issn[:4] + "-" + issn[4:]
                    db_reg = self.collection.find_one({"external_ids.id": extid})
                    if db_reg:
                        found_issn = issn
                        break
                    ext_ids.append({"source": "issn", "id": extid})
            if db_reg:
                self.already_in_db.append(found_issn)
                self.update_scimago(sjr, db_reg)
            else:
                entry = self.empty_source()
                entry["updated"] = [{"source": "scimago", "time": int(time())}]
                entry["types"].append(
                    {"source": "scimago", "type": self._get_value(sjr, "Type")})
                entry["external_ids"] = ext_ids
                entry["external_ids"].append(
                    {"source": "scimago", "id": str(self._get_value(sjr, "Sourceid"))})
                entry["names"] = [
                    {"lang": "en", "name": self._get_value(sjr, "Title"), "source": "scimago"}]
                country = None
                try:
                    country_name = self._get_value(sjr, "Country")
                    if country_name == "United States":
                        country_name = "United States of America"
                    elif country_name == "United Kingdom":
                        country_name = "United Kingdom of Great Britain and Northern Ireland"
                    elif country_name == "South Korea":
                        country_name = "Korea, Republic of"
                    elif country_name == "Czech Republic":
                        country_name = "Czechia"
                    elif country_name == "Taiwan":
                        country_name = "Taiwan, Province of China"
                    elif country_name == "Iran":
                        country_name = "Iran, Islamic Republic of"
                    elif country_name == "Moldova":
                        country_name = "Moldova, Republic of"
                    elif country_name == "Venezuela":
                        country_name = "Venezuela, Bolivarian Republic of"
                    elif country_name == "Macedonia":
                        country_name = "North Macedonia"
                    elif country_name == "Palestine":
                        country_name = "Palestine, State of"
                    elif country_name == "Turkey":
                        country_name = "Türkiye"
                    elif country_name == "Vatican City State":
                        country_name = "Holy See"
                    elif country_name == "Tanzania":
                        country_name = "Tanzania, United Republic of"
                    elif country_name == "Bolivia":
                        country_name = "Bolivia, Plurinational State of"

                    country = iso3166.countries_by_name.get(
                        country_name.upper()).alpha2
                except Exception as e:
                    print(e, self._get_value(sjr, "Country"))
                if country:
                    entry["publisher"] = {
                        "country_code": country, "name": self._get_value(sjr, "Publisher")}
                entry["ranking"].append({
                    "to_date": self.scimago_end_ts,
                    "from_date": self.scimago_start_ts,
                    "rank": self._get_value(sjr, "SJR Best Quartile"),
                    "order": int(self._get_value(sjr, "Rank")) if self._get_value(sjr, "Rank") else None,
                    "source": "scimago Best Quartile"
                })
                entry["ranking"].append({
                    "to_date": self.scimago_end_ts,
                    "from_date": self.scimago_start_ts,
                    "rank": int(self._get_value(sjr, "H index")),
                    "order": int(self._get_value(sjr, "Rank")) if self._get_value(sjr, "Rank") else None,
                    "source": "scimago hindex"
                })
                if self._get_value(sjr, "Open Access") and self._get_value(sjr, "Open Access Diamond"):
                    entry["open_access"].append({
                        "provenance": "scimago",
                        "is_open_access": True if self._get_value(sjr, "Open Access") == "Yes" else False,
                        "open_access_diamond": True if self._get_value(sjr, "Open Access Diamond") == "Yes" else False
                    })
                if self._get_value(sjr, "SJR"):
                    rank = ""
                    if isinstance(self._get_value(sjr, "SJR"), str):
                        rank = float(self._get_value(sjr, "SJR").replace(",", "."))
                    else:
                        rank = self._get_value(sjr, "SJR")
                    entry["ranking"].append({
                        "to_date": self.scimago_end_ts,
                        "from_date": self.scimago_start_ts,
                        "rank": rank,
                        "order": int(self._get_value(sjr, "Rank")) if self._get_value(sjr, "Rank") else None,
                        "source": "scimago"
                    })

                # Upsert category rankings
                self.upsert_scimago_category_rankings(sjr, entry)

                scimago_subjects = []
                for cat in self._get_value(sjr, "Categories", "").split(";"):
                    cat_clean = self._normalize_spaces(cat)
                    m = self._CAT_RE.match(cat_clean)
                    if m:
                        cat_name = self._normalize_spaces(m.group("name"))
                    else:
                        cat_name = cat_clean.split(" (")[0].strip()
                    scimago_subjects.append({
                        "id": "",
                        "name": cat_name,
                        "level": None,
                        "external_ids": []
                    })
                entry["subjects"].append({
                    "source": "scimago",
                    "subjects": scimago_subjects
                })
                self.collection.insert_one(entry)

    def run(self):
        if self.scimago_file_paths:
            for filename in self.scimago_file_paths:
                self.scimago_year = int(
                    filename.replace(".csv", "").split(" ")[-1])
                self.scimago_start_ts = dt.strptime(
                    "01 01 " + str(self.scimago_year), "%d %m %Y").timestamp()
                self.scimago_end_ts = dt.strptime(
                    "31 12 " + str(self.scimago_year), "%d %m %Y").timestamp()
                self.scimago = read_csv(filename,
                                        sep=";", dtype={"Sourceid": str}).to_dict("records")
                self.process_scimago()
            return 0

        years = self.scimago_collection.distinct("year")
        for year in sorted(years):
            self.scimago_year = int(year)
            self.scimago_start_ts = dt.strptime(
                "01 01 " + str(self.scimago_year), "%d %m %Y").timestamp()
            self.scimago_end_ts = dt.strptime(
                "31 12 " + str(self.scimago_year), "%d %m %Y").timestamp()
            self.scimago = self.scimago_collection.find(
                {"year": self.scimago_year}, sort=[("Sourceid", 1)]
            )
            self.process_scimago()
        return 0
