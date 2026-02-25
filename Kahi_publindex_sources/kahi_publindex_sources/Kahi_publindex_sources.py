from datetime import datetime as dt
from time import time
import re

from kahi.KahiBase import KahiBase
from pymongo import MongoClient


class Kahi_publindex_sources(KahiBase):
    config = {}

    def __init__(self, config):
        self.config = config
        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]
        self.collection = self.db["sources"]
        self.collection.create_index("external_ids.id")

        source_cfg = config["publindex_sources"]
        self.publindex_client = MongoClient(source_cfg["database_url"])

        if source_cfg["database_name"] not in self.publindex_client.list_database_names():
            raise RuntimeError(
                f'Database {source_cfg["database_name"]} was not found'
            )

        self.publindex_db = self.publindex_client[source_cfg["database_name"]]
        if source_cfg["collection_name"] not in self.publindex_db.list_collection_names():
            raise RuntimeError(
                f'Collection {source_cfg["collection_name"]} was not found on database {source_cfg["database_name"]}'
            )

        self.publindex_collection = self.publindex_db[source_cfg["collection_name"]]
        self.verbose = source_cfg.get("verbose", 0)

        self.inserted = 0
        self.updated = 0
        self.skipped = 0

    def _normalize_text(self, value):
        if value is None:
            return ""
        if not isinstance(value, str):
            value = str(value)
        value = " ".join(value.strip().split())
        if value.lower() == "no disponible":
            return ""
        return value

    def _normalize_numeric_id(self, value):
        text = self._normalize_text(value)
        if not text:
            return ""
        match = re.match(r"^(\d+)\.0+$", text)
        if match:
            return match.group(1)
        return text

    def _normalize_issn(self, value):
        issn = self._normalize_text(value).upper()
        if not issn:
            return ""
        issn = issn.replace("-", "").replace(" ", "")
        if len(issn) != 8:
            return ""
        return f"{issn[:4]}-{issn[4:]}"

    def _year_bounds(self, value):
        year_txt = self._normalize_numeric_id(value)
        if not year_txt or not year_txt.isdigit():
            return (None, None)

        year = int(year_txt)
        if year < 1800 or year > 2200:
            return (None, None)

        start = int(dt(year, 1, 1, 0, 0, 0).timestamp())
        end = int(dt(year, 12, 31, 23, 59, 59).timestamp())
        return (start, end)

    def _ensure_required_fields(self, entry):
        list_fields = [
            "updated",
            "names",
            "types",
            "external_ids",
            "subjects",
            "ranking",
            "open_access",
            "external_urls",
            "keywords",
        ]
        for field in list_fields:
            if field not in entry or not isinstance(entry[field], list):
                entry[field] = []
        if "publisher" not in entry or not isinstance(entry["publisher"], dict):
            entry["publisher"] = {}
        return entry

    def _upsert_updated(self, entry, source_name):
        found = False
        for upd in entry["updated"]:
            if upd.get("source") == source_name:
                upd["time"] = int(time())
                found = True
                break
        if not found:
            entry["updated"].append(
                {"source": source_name, "time": int(time())})

    def _append_name(self, entry, name, lang, source):
        name = self._normalize_text(name)
        if not name:
            return
        for rec in entry["names"]:
            if rec.get("name") == name and rec.get("source") == source:
                return
        entry["names"].append({"lang": lang, "name": name, "source": source})

    def _append_external_id(self, entry, source, identifier):
        identifier = self._normalize_text(identifier)
        if not identifier:
            return
        for rec in entry["external_ids"]:
            if rec.get("source") == source and rec.get("id") == identifier:
                return
        entry["external_ids"].append({"source": source, "id": identifier})

    def _upsert_ranking(self, entry, rank_value, from_date, to_date):
        rank_value = self._normalize_text(rank_value)
        if not rank_value:
            return

        for rec in entry["ranking"]:
            if (
                rec.get("source") == "publindex" and rec.get("from_date") == from_date and rec.get("to_date") == to_date
            ):
                rec["rank"] = rank_value
                rec["order"] = None
                return

        entry["ranking"].append(
            {
                "from_date": from_date,
                "to_date": to_date,
                "rank": rank_value,
                "order": None,
                "source": "publindex",
            }
        )

    def _subject_key(self, subject):
        external_ids = tuple(
            sorted(
                (
                    ext.get("source", ""),
                    ext.get("id", ""),
                )
                for ext in subject.get("external_ids", [])
            )
        )
        return (
            subject.get("name", ""),
            subject.get("level", ""),
            external_ids,
        )

    def _build_subjects(self, reg):
        subjects = []
        gran_area = self._normalize_text(reg.get("nme_gran_area"))
        area = self._normalize_text(reg.get("nme_area"))
        specialty = self._normalize_text(reg.get("nme_especialidad"))
        area_code = self._normalize_text(reg.get("id_area_con"))

        if gran_area and gran_area.lower() != "no registra":
            subjects.append(
                {"id": "", "name": gran_area, "level": "gran_area", "external_ids": []}
            )
        if area and area.lower() != "no registra":
            subjects.append(
                {"id": "", "name": area, "level": "area", "external_ids": []}
            )
        if specialty and specialty.lower() != "no registra":
            subjects.append(
                {
                    "id": "",
                    "name": specialty,
                    "level": "especialidad",
                    "external_ids": [],
                }
            )

        if area_code:
            ext = {"source": "publindex_area", "id": area_code}
            if subjects:
                subjects[-1]["external_ids"].append(ext)
            else:
                subjects.append(
                    {
                        "id": "",
                        "name": area_code,
                        "level": "area_code",
                        "external_ids": [ext],
                    }
                )

        return subjects

    def _merge_subjects(self, entry, subjects):
        if not subjects:
            return

        source_idx = None
        for idx, block in enumerate(entry["subjects"]):
            if block.get("source") == "publindex":
                source_idx = idx
                break

        if source_idx is None:
            entry["subjects"].append(
                {"source": "publindex", "subjects": subjects})
            return

        current = entry["subjects"][source_idx].get("subjects", [])
        seen = {self._subject_key(sub) for sub in current}
        for sub in subjects:
            key = self._subject_key(sub)
            if key not in seen:
                current.append(sub)
                seen.add(key)
        entry["subjects"][source_idx]["subjects"] = current

    def _extract_identity(self, reg):
        return {
            "name": self._normalize_text(reg.get("nme_revista_in")),
            "issn_p": self._normalize_issn(reg.get("txt_issn_p")),
            "issn_l": self._normalize_issn(reg.get("txt_issn_l")),
            "publindex_id": self._normalize_numeric_id(reg.get("id_revista_p")),
        }

    def _find_existing_source(self, reg):
        identity = self._extract_identity(reg)
        ids = [
            identity["issn_p"],
            identity["issn_l"],
            identity["publindex_id"],
        ]
        ids = [identifier for identifier in ids if identifier]
        if ids:
            doc = self.collection.find_one({"external_ids.id": {"$in": ids}})
            if doc:
                return doc

        if identity["name"]:
            return self.collection.find_one({"names.name": identity["name"]})
        return None

    def insert_source(self, reg):
        entry = self.empty_source()
        entry = self._ensure_required_fields(entry)
        identity = self._extract_identity(reg)

        self._upsert_updated(entry, "publindex")
        self._append_name(entry, identity["name"], "es", "publindex")
        self._append_external_id(entry, "issn", identity["issn_p"])
        self._append_external_id(entry, "issn_l", identity["issn_l"])
        self._append_external_id(entry, "publindex", identity["publindex_id"])

        from_date, to_date = self._year_bounds(reg.get("nro_ano"))
        self._upsert_ranking(entry, reg.get("id_clas_rev"), from_date, to_date)

        publisher_name = self._normalize_text(reg.get("nme_inst_edit_1"))
        if publisher_name:
            country = self._normalize_text(reg.get("pais_rev_in"))
            country_code = "CO" if country and country.lower() == "colombia" else ""
            entry["publisher"] = {
                "country_code": country_code,
                "name": publisher_name,
                "id": "",
            }

        self._merge_subjects(entry, self._build_subjects(reg))

        self.collection.insert_one(entry)
        self.inserted += 1

    def update_source(self, reg, entry):
        entry = self._ensure_required_fields(entry)
        identity = self._extract_identity(reg)

        self._upsert_updated(entry, "publindex")
        self._append_name(entry, identity["name"], "es", "publindex")
        self._append_external_id(entry, "issn", identity["issn_p"])
        self._append_external_id(entry, "issn_l", identity["issn_l"])
        self._append_external_id(entry, "publindex", identity["publindex_id"])

        from_date, to_date = self._year_bounds(reg.get("nro_ano"))
        self._upsert_ranking(entry, reg.get("id_clas_rev"), from_date, to_date)

        publisher_name = self._normalize_text(reg.get("nme_inst_edit_1"))
        if publisher_name:
            country = self._normalize_text(reg.get("pais_rev_in"))
            country_code = "CO" if country and country.lower() == "colombia" else ""
            if not entry["publisher"]:
                entry["publisher"] = {
                    "country_code": country_code,
                    "name": publisher_name,
                    "id": "",
                }
            else:
                if not entry["publisher"].get("name"):
                    entry["publisher"]["name"] = publisher_name
                if "country_code" not in entry["publisher"]:
                    entry["publisher"]["country_code"] = country_code

        self._merge_subjects(entry, self._build_subjects(reg))

        doc_id = entry["_id"]
        del entry["_id"]
        self.collection.update_one({"_id": doc_id}, {"$set": entry})
        self.updated += 1

    def process_publindex(self):
        cursor = self.publindex_collection.find(no_cursor_timeout=True)
        try:
            for idx, reg in enumerate(cursor, start=1):
                identity = self._extract_identity(reg)
                has_identity = any(
                    [
                        identity["name"],
                        identity["issn_p"],
                        identity["issn_l"],
                        identity["publindex_id"],
                    ]
                )
                if not has_identity:
                    self.skipped += 1
                    continue

                source_db = self._find_existing_source(reg)
                if source_db:
                    self.update_source(reg, source_db)
                else:
                    self.insert_source(reg)

                if self.verbose >= 5 and idx % 1000 == 0:
                    print(f"Processed {idx} records")
        finally:
            cursor.close()

        if self.verbose >= 4:
            print(
                f"Publindex finished: inserted={self.inserted}, updated={self.updated}, skipped={self.skipped}"
            )

    def run(self):
        start_time = time()
        self.process_publindex()
        print("Execution time: {} minutes".format(
            round((time() - start_time) / 60, 2)))
        self.client.close()
        self.publindex_client.close()
        return 0
