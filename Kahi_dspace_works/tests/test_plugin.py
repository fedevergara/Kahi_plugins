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
from kahi_dspace_works.process_one import (
    get_units_affiliations,
    process_one,
    process_one_insert,
    process_one_update,
    run_es_operation,
)


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


def seed_internal_units(db):
    parent = {
        "id": "02yr3f298",
        "name": "FUCS",
        "types": [{"source": "ror", "type": "education"}],
    }
    faculty = {
        "id": "02yr3f298_U05",
        "name": "Facultad de Enfermería",
        "types": [{"source": "staff", "type": "faculty"}],
    }
    department = {
        "id": "02yr3f298_U05_U01",
        "name": "Enfermería",
        "types": [{"source": "staff", "type": "department"}],
    }
    group = {
        "id": "COL0001",
        "name": "Grupo de Enfermería",
        "types": [{"source": "scienti", "type": "group"}],
    }
    unrelated = {
        "id": "other_U01",
        "name": "Otra facultad",
        "types": [{"source": "staff", "type": "faculty"}],
    }
    db.affiliations.insert_many([
        {"_id": parent["id"], "types": parent["types"]},
        {"_id": faculty["id"], "relations": [{"id": parent["id"]}]},
        {"_id": department["id"], "relations": [{"id": parent["id"]}]},
        {"_id": group["id"], "relations": [{"id": parent["id"]}]},
        {"_id": unrelated["id"], "relations": [{"id": "other"}]},
    ])
    person = {
        "_id": "person-1",
        "full_name": "adriana panader torres",
        "first_names": ["adriana"],
        "last_names": ["panader", "torres"],
        "initials": "apt",
        "updated": [{"source": "staff"}],
        "affiliations": [parent, faculty, department, group, unrelated],
    }
    db.person.insert_one(deepcopy(person))
    return parent, faculty, department, group, unrelated, person


def test_get_units_affiliations_returns_faculty_and_department_only():
    db = mongomock.MongoClient().db
    parent, faculty, department, _group, _unrelated, person = seed_internal_units(db)

    units = get_units_affiliations(db, person, [parent])

    assert [unit["id"] for unit in units] == [faculty["id"], department["id"]]


def test_get_units_affiliations_requires_parent_in_product():
    db = mongomock.MongoClient().db
    _parent, _faculty, _department, _group, _unrelated, person = seed_internal_units(db)

    assert get_units_affiliations(db, person, []) == []


def test_get_units_affiliations_requires_parent_in_person():
    db = mongomock.MongoClient().db
    parent, _faculty, _department, _group, _unrelated, person = seed_internal_units(db)
    person["affiliations"] = person["affiliations"][1:]
    db.person.replace_one({"_id": person["_id"]}, person)

    assert get_units_affiliations(db, person, [parent]) == []


def test_get_units_affiliations_resolves_parent_external_id():
    db = mongomock.MongoClient().db
    parent, faculty, department, _group, _unrelated, person = seed_internal_units(db)
    db.affiliations.update_one(
        {"_id": parent["id"]},
        {"$set": {"external_ids": [{"id": "https://ror.org/02yr3f298"}]}})

    units = get_units_affiliations(db, person, [{
        "external_ids": [{"id": "https://ror.org/02yr3f298"}]
    }])

    assert [unit["id"] for unit in units] == [faculty["id"], department["id"]]


def test_dspace_insert_propagates_units_without_duplicates():
    db = mongomock.MongoClient().db
    parent, faculty, department, _group, _unrelated, _person = seed_internal_units(db)
    entry = empty_work()
    entry.update({
        "titles": [{"title": "Trabajo de enfermería", "lang": "es"}],
        "external_ids": [{"source": "dspace", "id": "oai:test:1"}],
        "authors": [{
            "id": "",
            "full_name": "Adriana Panader Torres",
            "first_names": ["Adriana"],
            "last_names": ["Panader", "Torres"],
            "initials": "APT",
            "affiliations": [deepcopy(parent)],
        }],
    })

    process_one_insert(entry, parent, db, db.works, None, 0)

    author = db.works.find_one()["authors"][0]
    assert author["id"] == "person-1"
    assert [item["id"] for item in author["affiliations"]] == [
        parent["id"], faculty["id"], department["id"]]


def test_dspace_update_propagates_units_to_matched_author():
    db = mongomock.MongoClient().db
    parent, faculty, department, _group, _unrelated, _person = seed_internal_units(db)
    existing = empty_work()
    existing.update({
        "_id": "work-1",
        "authors": [{
            "id": "person-1",
            "full_name": "adriana panader torres",
            "affiliations": [deepcopy(parent)],
        }],
        "author_count": 1,
    })
    db.works.insert_one(deepcopy(existing))
    incoming = empty_work()
    incoming["authors"] = [{
        "full_name": "Adriana Panader Torres",
        "type": "author",
        "affiliations": [],
    }]

    with patch("kahi_dspace_works.process_one.compare_author", return_value=True):
        process_one_update(incoming, existing, parent, db, db.works, 0)

    author = db.works.find_one({"_id": "work-1"})["authors"][0]
    assert [item["id"] for item in author["affiliations"]] == [
        parent["id"], faculty["id"], department["id"]]
