from kahi.KahiBase import KahiBase
from elasticsearch import Elasticsearch, VERSION as ES_VERSION
from pymongo import MongoClient
from math import isnan

from mohan.Similarity import Similarity


class Kahi_elasticsearch_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        if "es_index" not in config["elasticsearch_works"].keys():
            raise Exception(
                "[Kahi_elasticsearch_works] ERROR: Please specify an es_index")

        self.index = config["elasticsearch_works"]["es_index"]

        self.debug = config["elasticsearch_works"].get("debug", False)

        self.task = config["elasticsearch_works"].get("task")

        self.es_url = config["elasticsearch_works"]["es_url"] if "es_url" in config["elasticsearch_works"].keys(
        ) else "http://localhost:9200"

        es_auth = (
            config["elasticsearch_works"]["es_user"],
            config["elasticsearch_works"]["es_password"]
        )
        if self.task == "delete":
            auth_parameter = (
                {"basic_auth": es_auth}
                if ES_VERSION[0] >= 8
                else {"http_auth": es_auth}
            )
            self.es_client = Elasticsearch(self.es_url, **auth_parameter)
        else:
            self.es_client = Similarity(
                es_index=self.index,
                es_uri=self.es_url,
                es_auth=es_auth,
            )

        self.verbose = config["elasticsearch_works"]["verbose"] if "verbose" in config["elasticsearch_works"].keys(
        ) else 0
        self.bulk_size = config["elasticsearch_works"]["bulk_size"] if "bulk_size" in config["elasticsearch_works"].keys(
        ) else 100

        self.inserted_ids = []

    def bulk_insert(self):
        es_entries = []
        paper_list = self.collection.find(
            {}, {"titles": 1, "source": 1, "year_published": 1, "bibliographic_info": 1, "authors.full_name": 1})
        paper_list_count = self.collection.count_documents({})
        for i, reg in enumerate(paper_list):
            work = {
                "title": "",
                "source": "",
                "year": "",
                "volume": "",
                "issue": "",
                "page_start": "",
                "page_end": "",
                "authors": [],
                "provenance": "elasticsearch",

            }
            if "titles" not in reg.keys():
                continue
            if not reg["titles"]:
                continue
            work["title"] = reg["titles"][0]["title"]
            source = reg.get("source") or {}
            if "name" in source:
                work["source"] = source["name"] if source["name"] else ""
            if "year_published" in reg.keys():
                work["year"] = reg["year_published"] if reg["year_published"] else ""
            bibliographic_info = reg.get("bibliographic_info") or {}
            if "volume" in bibliographic_info:
                work["volume"] = bibliographic_info["volume"] or ""
            if "issue" in bibliographic_info:
                work["issue"] = bibliographic_info["issue"] or ""
            if "start_page" in bibliographic_info:
                work["page_start"] = bibliographic_info["start_page"] or ""
            if "end_page" in bibliographic_info:
                work["page_end"] = bibliographic_info["end_page"] or ""
            authors = []
            for author in reg.get("authors", []):
                if author.get("full_name"):
                    authors.append(author["full_name"])
                if len(authors) == 5:
                    break
            work["authors"] = authors
            # double checking for nan
            for key, val in work.items():
                if isinstance(val, float) and isnan(val):
                    work[key] = ""
            entry = {
                "_index": self.index,
                "_id": str(reg["_id"]),
                "_source": work
            }
            es_entries.append(entry)
            if len(es_entries) == self.bulk_size or paper_list_count <= self.bulk_size or i + 1 == paper_list_count:
                try:
                    self.es_client.insert_bulk(es_entries)
                except Exception as e:
                    print(e)
                    print(es_entries)
                    raise
                es_entries = []
                if self.verbose > 4:
                    print(f"""{i + 1} entries inserted""")

    def delete(self):
        self.es_client.indices.delete(
            index=self.index,
            ignore_unavailable=True
        )

    def run(self):
        if self.task == "bulk_insert":
            if self.verbose > 0:
                print(f"""Bulk inserting index {self.index}""")
            self.bulk_insert()
        elif self.task == "delete":
            if self.verbose > 0:
                print(f"""Deleting index {self.index}""")
            self.delete()
        else:
            raise Exception("Please specify a task to execute")
        if self.debug:
            return 1
        return 0
