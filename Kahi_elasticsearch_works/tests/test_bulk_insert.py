from bson import ObjectId

import mongomock

from kahi_elasticsearch_works.Kahi_elasticsearch_works import Kahi_elasticsearch_works


class BulkRecorder:
    def __init__(self):
        self.entries = []

    def insert_bulk(self, entries):
        self.entries.extend(entries)


def test_bulk_insert_accepts_sparse_work_and_uses_mohan_page_field_names():
    collection = mongomock.MongoClient().db.works
    work_id = ObjectId()
    collection.insert_one(
        {
            "_id": work_id,
            "titles": [{"title": "Trabajo mínimo"}],
            "authors": [{"full_name": "Ada Uno"}],
            "bibliographic_info": {"start_page": "10", "end_page": "20"},
        }
    )
    plugin = object.__new__(Kahi_elasticsearch_works)
    plugin.collection = collection
    plugin.index = "test"
    plugin.bulk_size = 100
    plugin.verbose = 0
    plugin.es_client = BulkRecorder()

    plugin.bulk_insert()

    source = plugin.es_client.entries[0]["_source"]
    assert source["page_start"] == "10"
    assert source["page_end"] == "20"
    assert "start_page" not in source
    assert source["source"] == ""
