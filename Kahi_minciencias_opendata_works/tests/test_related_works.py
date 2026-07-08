from copy import deepcopy
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Lock
from time import sleep
from unittest.mock import MagicMock, patch

import mongomock

from kahi_minciencias_opendata_works.related_works import (
    collect_related_works,
    process_person_related_works,
)
from kahi_minciencias_opendata_works.process_one import process_one
from kahi_minciencias_opendata_works.process_one import run_es_operation
from kahi_minciencias_opendata_works.parser import parse_minciencias_opendata
import kahi_minciencias_opendata_works.Kahi_minciencias_opendata_works as plugin_module


def empty_work():
    return {
        "titles": [],
        "updated": [],
        "doi": "",
        "abstracts": [],
        "keywords": [],
        "types": [],
        "external_ids": [],
        "external_urls": [],
        "date_published": None,
        "year_published": None,
        "bibliographic_info": {},
        "open_access": {},
        "apc": {"paid": {}},
        "references_count": None,
        "references": [],
        "citations_count": [],
        "citations": [],
        "author_count": None,
        "authors": [],
        "source": {},
        "ranking": [],
        "subjects": [],
        "citations_by_year": [],
        "groups": [],
        "rights": [],
        "primary_topic": {},
        "topics": [],
    }


def related(title="Trabajo controlado", doi=None):
    return {
        "provenance": "minciencias",
        "source": "doi" if doi else "cvlac_stage_raw",
        "id": doi or {"title": "trabajo controlado", "year": 2024},
        "title": title,
        "year": 2024,
        "product_type": "Artículo",
        "type_impactu": "Artículo de revista",
        "authors": [],
        "keywords": ["ciencia abierta"],
        "areas": ["Ciencias Sociales"],
        "advisor_role": "",
        "oriented_people": [],
        "doi": [doi] if doi else [],
        "issn": ["1234-5678"],
        "isbn": [],
    }


class FakeES:
    def __init__(self, responses=None):
        self.responses = responses
        self.inserted = []

    def search_work(self, **kwargs):
        return deepcopy(self.responses)

    def insert_work(self, _id, work):
        self.inserted.append((_id, deepcopy(work)))


def test_collect_groups_same_work_and_keeps_all_known_authors():
    db = mongomock.MongoClient().db
    item = related(doi="https://doi.org/10.1000/test")
    db.person.insert_many([{"full_name": "Ada Uno",
                            "affiliations": [],
                            "related_works": [item]},
                           {"full_name": "Beto Dos",
                            "affiliations": [],
                            "related_works": [item]},
                           ])

    candidates = collect_related_works(db.person)

    assert len(candidates) == 1
    assert {author["full_name"] for author in candidates[0]["authors"]} == {
        "Ada Uno",
        "Beto Dos",
    }
    assert candidates[0]["dois"] == ["https://doi.org/10.1000/test"]


def test_exact_doi_enriches_existing_work_instead_of_duplicating_it():
    db = mongomock.MongoClient().db
    db.person.insert_one(
        {
            "full_name": "Ada Uno",
            "affiliations": [],
            "related_works": [related(doi="10.1000/TEST")],
        }
    )
    db.works.insert_one({**empty_work(),
                         "titles": [{"title": "Existing title",
                                     "lang": "en",
                                     "source": "openalex"}],
                         "external_ids": [{"source": "doi",
                                           "id": "https://doi.org/10.1000/test"}],
                         })

    counters = process_person_related_works(
        db.person, db.works, empty_work, insert_all=True
    )

    assert counters["doi"] == 1
    assert counters["inserted"] == 0
    assert db.works.count_documents({}) == 1
    work = db.works.find_one()
    assert work["titles"][0]["title"] == "Existing title"
    assert "ciencia abierta" in work["keywords"]
    assert work["authors"][0]["full_name"] == "Ada Uno"


def test_exact_top_level_doi_is_also_resolved():
    db = mongomock.MongoClient().db
    db.person.insert_one(
        {
            "full_name": "Ada Uno",
            "affiliations": [],
            "related_works": [related(doi="10.1000/TEST")],
        }
    )
    db.works.insert_one(
        {**empty_work(), "doi": "https://doi.org/10.1000/test"})

    counters = process_person_related_works(
        db.person, db.works, empty_work, insert_all=True
    )

    assert counters["doi"] == 1
    assert db.works.count_documents({}) == 1


def test_insert_all_false_reports_unmatched_candidate_without_writing():
    db = mongomock.MongoClient().db
    db.person.insert_one(
        {"full_name": "Ada Uno", "affiliations": [], "related_works": [related()]}
    )

    counters = process_person_related_works(
        db.person, db.works, empty_work, es_handler=FakeES(), insert_all=False
    )

    assert counters["skipped"] == 1
    assert db.works.count_documents({}) == 0


def test_no_doi_and_empty_elasticsearch_is_inserted_once_and_is_idempotent():
    db = mongomock.MongoClient().db
    db.person.insert_one(
        {"full_name": "Ada Uno", "affiliations": [], "related_works": [related()]}
    )
    es = FakeES(responses=None)

    first = process_person_related_works(
        db.person, db.works, empty_work, es_handler=es, insert_all=True
    )
    second = process_person_related_works(
        db.person, db.works, empty_work, es_handler=es, insert_all=True
    )

    assert first["inserted"] == 1
    assert second["fingerprint"] == 1
    assert db.works.count_documents({}) == 1
    assert len(es.inserted) == 1
    assert es.inserted[0][1]["first_page"] == ""
    assert db.works.find_one(
    )["external_ids"][-1]["source"] == "minciencias_title_fingerprint"


def test_similarity_updates_the_mongo_work_referenced_by_elasticsearch():
    db = mongomock.MongoClient().db
    db.person.insert_one(
        {"full_name": "Ada Uno", "affiliations": [], "related_works": [related()]}
    )
    work_id = db.works.insert_one(
        {
            **empty_work(),
            "titles": [{"title": "Trabajo controlado", "lang": "es", "source": "other"}],
            "authors": [{"full_name": "Ada Uno", "affiliations": []}],
        }
    ).inserted_id
    es = FakeES(responses=[{"_id": str(work_id), "_source": {
        "title": "Trabajo controlado", "authors": ["Ada Uno"]}, }])

    counters = process_person_related_works(
        db.person, db.works, empty_work, es_handler=es, insert_all=True
    )

    assert counters["similarity"] == 1
    assert counters["inserted"] == 0
    assert db.works.count_documents({}) == 1


def test_gruplac_path_inserts_when_elasticsearch_has_zero_hits():
    db = mongomock.MongoClient().db
    db.person.insert_one(
        {
            "full_name": "Ada Uno",
            "affiliations": [],
            "external_ids": [{"source": "scienti", "id": {"COD_RH": "1234567890"}}],
        }
    )
    source = {
        "id_producto_pd": "1234567890-1",
        "id_persona_pd": "1234567890",
        "nme_producto_pd": "Trabajo sin coincidencias",
        "nme_tipologia_pd": "Artículo",
        "nme_clase_pd": "Producción bibliográfica",
        "cod_grupo_gr": "G-1",
    }
    es = FakeES(responses=None)

    process_one(
        source,
        db,
        db.works,
        empty_work(),
        es,
        insert_all=True,
        thresholds=[65, 90, 95],
    )

    assert db.works.count_documents({}) == 1
    assert len(es.inserted) == 1


def test_gruplac_retry_is_idempotent_by_product_id():
    db = mongomock.MongoClient().db
    db.person.insert_one({
        "full_name": "Ada Uno",
        "affiliations": [],
        "external_ids": [{"source": "scienti", "id": {"COD_RH": "1234567890"}}],
    })
    source = {
        "id_producto_pd": "1234567890-1",
        "id_persona_pd": "1234567890",
        "nme_producto_pd": "Trabajo idempotente",
        "nme_tipologia_pd": "Artículo",
        "nme_clase_pd": "Producción bibliográfica",
        "cod_grupo_gr": "G-1",
    }
    es = FakeES(responses=None)

    process_one(source, db, db.works, empty_work(), es, True, [65, 90, 95])
    process_one(source, db, db.works, empty_work(), es, True, [65, 90, 95])

    assert db.works.count_documents({}) == 1
    work = db.works.find_one()
    assert any(
        item.get("source") == "minciencias"
        and item.get("id") == "1234567890-1"
        for item in work["external_ids"]
    )


def test_parser_rejects_blank_title_without_unbound_variables():
    assert parse_minciencias_opendata(
        {"nme_producto_pd": "", "id_persona_pd": None}, empty_work()
    ) is None


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


def test_main_aggregation_cursor_is_streamed_and_closed():
    class Cursor:
        def __init__(self):
            self.iterations = 0
            self.closed = False

        def __iter__(self):
            self.iterations += 1
            yield {
                "id_producto_pd": "1234567890-1",
                "nme_producto_pd": "Trabajo",
            }

        def close(self):
            self.closed = True

    cursor = Cursor()
    source_collection = MagicMock()
    source_collection.aggregate.return_value = cursor
    source_client = MagicMock()
    source_client.__getitem__.return_value.__getitem__.return_value = source_collection
    instance = object.__new__(plugin_module.Kahi_minciencias_opendata_works)
    instance.config = {"minciencias_opendata_works": {
        "database_url": "localhost:27017",
        "database_name": "dam",
        "collection_name": "gruplac_production_data",
    }}
    instance.db = MagicMock()
    instance.collection = MagicMock()
    instance.n_jobs = 1
    instance.verbose = 0
    instance.es_handler = None
    instance.es_semaphore = None
    instance.insert_all = True
    instance.thresholds = [65, 90, 95]
    instance.include_person_related_works = False
    instance.empty_work = empty_work

    class RecordingParallel:
        def __init__(self, **_kwargs):
            pass

        def __call__(self, tasks):
            assert cursor.iterations == 0
            list(tasks)
            return []

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(plugin_module, "MongoClient", return_value=source_client)
        )
        stack.enter_context(
            patch.object(plugin_module, "Parallel", RecordingParallel)
        )
        instance.process_opendata()

    assert source_collection.aggregate.call_args.kwargs["batchSize"] == 1000
    assert cursor.iterations == 1
    assert cursor.closed
