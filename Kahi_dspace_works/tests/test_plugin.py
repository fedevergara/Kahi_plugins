from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import BoundedSemaphore, Lock
from time import sleep
from unittest.mock import MagicMock, patch

import mongomock
from kahi.KahiBase import KahiBase

from kahi_dspace_works.Kahi_dspace_works import (
    DIM_FIELDS,
    Kahi_dspace_works,
    doi_query,
    normalize_repository_url,
    title_query,
)
import kahi_dspace_works.Kahi_dspace_works as plugin_module
from kahi_dspace_works.process_one import process_one, run_es_operation


class FakeCollection:
    def __init__(self, document=None):
        self.document = document

    def find_one(self, *_args, **_kwargs):
        return self.document


class FakeDatabase:
    def __init__(self):
        self.collections = {
            "dspace_demo_records": FakeCollection(),
            "dspace_demo_identity": FakeCollection(
                {"base_url": "https://example.edu/server/oai/request"}
            ),
            "dspace_demo_errors": FakeCollection(),
        }

    def list_collection_names(self):
        return list(self.collections)

    def __getitem__(self, name):
        return self.collections.get(name, FakeCollection())


def test_queries_bind_conditions_to_one_dim_field():
    assert "$elemMatch" in doi_query()["$or"][0][DIM_FIELDS]
    assert "$elemMatch" in title_query()["$or"][0][DIM_FIELDS]


def test_repository_discovery_uses_identity_and_affiliation_map():
    plugin = Kahi_dspace_works.__new__(Kahi_dspace_works)
    plugin.config = {
        "dspace_works": {
            "repository_affiliations": {
                "dspace_demo_records": "https://ror.org/012345678"
            }
        }
    }

    repositories = list(plugin._repositories(FakeDatabase()))

    assert repositories == [
        {
            "collection_name": "dspace_demo_records",
            "institution_id": "https://ror.org/012345678",
            "repository_url": "https://example.edu",
        }
    ]


def test_oai_endpoint_normalization():
    assert normalize_repository_url("https://example.edu/oai/request/") == "https://example.edu"


def test_initialization_creates_name_affiliation_collation_index(monkeypatch):
    client = MagicMock()
    db = MagicMock()
    person = MagicMock()
    works = MagicMock()
    client.__getitem__.return_value = db
    db.__getitem__.side_effect = lambda name: {
        "person": person,
        "works": works,
    }[name]
    monkeypatch.setattr(plugin_module, "MongoClient", lambda *_args, **_kwargs: client)

    Kahi_dspace_works({
        "database_url": "mongodb://localhost:27017",
        "database_name": "kahi",
        "dspace_works": {
            "database_url": "mongodb://localhost:27017",
            "database_name": "dspace",
        },
    })

    person.create_index.assert_any_call(
        [("full_name", 1), ("affiliations.id", 1)],
        name="full_name_affiliation_es",
        collation={"locale": "es", "strength": 1},
    )


def empty_work():
    return deepcopy(object.__new__(KahiBase).empty_work())


def dspace_record(identifier="oai:example.edu:123/456"):
    return {
        "_id": identifier,
        "OAI-PMH": {
            "request": {
                "#text": "https://example.edu/oai/request",
                "@verb": "GetRecord",
                "@metadataPrefix": "dim",
                "@identifier": identifier,
            },
            "GetRecord": {"record": {"metadata": {"dim:dim": {"dim:field": [
                {"@element": "title", "#text": "Trabajo DSpace controlado"},
                {"@element": "type", "#text": "Artículo"},
            ]}}}},
        },
    }


class FakeES:
    es_index = "works"

    def __init__(self, responses=None):
        self.responses = responses
        self.documents = set()
        self.inserted = []
        self.es = self

    def search_work(self, **_kwargs):
        return deepcopy(self.responses)

    def exists(self, index, id):
        return id in self.documents

    def insert_work(self, _id, work):
        self.documents.add(_id)
        self.inserted.append((_id, deepcopy(work)))


def test_no_doi_retry_is_idempotent_and_reconciles_elasticsearch():
    db = mongomock.MongoClient().db
    es = FakeES()
    record = dspace_record()

    for _ in range(2):
        process_one(
            record, None, "https://example.edu", db, db.works,
            empty_work(), es, True,
            {"author_thd": 65, "paper_thd_low": 90, "paper_thd_high": 95},
        )

    assert db.works.count_documents({}) == 1
    assert len(es.inserted) == 1
    assert db.works.find_one()["external_ids"][-1]["id"] == record["_id"]


def test_stale_elasticsearch_hit_does_not_drop_the_dspace_record():
    db = mongomock.MongoClient().db
    es = FakeES(responses=[{
        "_id": "not-a-mongo-object-id",
        "_source": {"title": "Trabajo DSpace controlado", "authors": []},
    }])

    process_one(
        dspace_record(), None, "https://example.edu", db, db.works,
        empty_work(), es, True,
        {"author_thd": 65, "paper_thd_low": 90, "paper_thd_high": 95},
    )

    assert db.works.count_documents({}) == 1


def test_es_semaphore_limits_concurrency():
    state = {"active": 0, "maximum": 0}
    state_lock = Lock()

    def operation():
        with state_lock:
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
        sleep(0.01)
        with state_lock:
            state["active"] -= 1

    semaphore = BoundedSemaphore(2)
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(run_es_operation, semaphore, operation)
            for _ in range(16)
        ]
        for future in futures:
            future.result()

    assert state["maximum"] == 2


def test_repository_cursor_is_streamed_and_closed():
    class Cursor:
        def __init__(self):
            self.iterations = 0
            self.closed = False
            self.batch = None

        def batch_size(self, value):
            self.batch = value
            return self

        def __iter__(self):
            self.iterations += 1
            yield dspace_record()

        def close(self):
            self.closed = True

    cursor = Cursor()
    collection = MagicMock()
    collection.find.return_value = cursor
    plugin = Kahi_dspace_works.__new__(Kahi_dspace_works)
    plugin.task = None
    plugin.batch_size = 500
    plugin.n_jobs = 1
    plugin.verbose = 0
    plugin.db = MagicMock()
    plugin.collection = MagicMock()
    plugin.es_handler = None
    plugin.es_semaphore = None
    plugin.thresholds = {"author_thd": 65, "paper_thd_low": 90, "paper_thd_high": 95}
    plugin.empty_work = empty_work

    class RecordingParallel:
        def __init__(self, **_kwargs):
            pass

        def __call__(self, tasks):
            assert cursor.iterations == 0
            list(tasks)
            return []

    with patch.object(plugin_module, "Parallel", RecordingParallel):
        plugin.process_repository(None, "https://example.edu", collection)

    assert cursor.batch == 500
    assert cursor.iterations == 1
    assert cursor.closed
