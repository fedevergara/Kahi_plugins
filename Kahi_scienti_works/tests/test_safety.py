from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Lock
from time import sleep
from unittest.mock import MagicMock

import kahi_scienti_works.process_one as process_module


def parsed_entry():
    return {
        "titles": [{"title": "Controlled title"}],
        "source": {"name": "Journal"},
        "year_published": None,
        "bibliographic_info": {"start_page": "10", "end_page": "20"},
        "authors": [{"full_name": "Ana"}],
        "external_ids": [{
            "source": "scienti",
            "id": {"COD_RH": "RH1", "COD_PRODUCTO": "P1"},
        }],
        "types": [],
    }


def test_es_operations_respect_concurrency_limit():
    semaphore = BoundedSemaphore(2)
    lock = Lock()
    active = 0
    maximum = 0

    def operation():
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        sleep(0.01)
        with lock:
            active -= 1

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(
                process_module.run_es_operation,
                semaphore,
                operation,
            )
            for _ in range(16)
        ]
        for future in futures:
            future.result()

    assert maximum == 2


def test_scienti_identity_uses_cod_rh_and_product():
    identity = process_module.scienti_identity(parsed_entry())
    assert identity == {
        "source": "scienti",
        "id": {"COD_RH": "RH1", "COD_PRODUCTO": "P1"},
    }
    assert process_module.scienti_insert_lock(
        identity, "Controlled title") is process_module.scienti_insert_lock(
            identity, "Controlled title")
    assert process_module.scienti_retry_query(identity, "Controlled title") == {
        "external_ids": {"$elemMatch": {
            "source": "scienti",
            "id.COD_RH": "RH1",
            "id.COD_PRODUCTO": "P1",
        }},
        "titles": {"$elemMatch": {
            "source": "scienti",
            "title": "Controlled title",
        }},
    }


def test_existing_scienti_identity_skips_search_and_insert(monkeypatch):
    collection = MagicMock()
    collection.find_one.return_value = {"_id": "already-inserted"}
    es_handler = MagicMock()
    es_handler.es_index = "works"
    es_handler.es.exists.return_value = True
    insert = MagicMock()
    monkeypatch.setattr(process_module, "parse_scienti", lambda *_args, **_kwargs: parsed_entry())
    monkeypatch.setattr(process_module, "process_one_insert", insert)

    process_module.process_one(
        {"TXT_NME_PROD": "Work"}, MagicMock(), collection, {}, es_handler, similarity=True,
        es_semaphore=BoundedSemaphore(1),
    )

    es_handler.search_work.assert_not_called()
    insert.assert_not_called()


def test_existing_mongo_work_is_reindexed_when_missing_from_es(monkeypatch):
    collection = MagicMock()
    collection.find_one.return_value = {"_id": "mongo-id"}
    es_handler = MagicMock()
    es_handler.es_index = "works"
    es_handler.es.exists.return_value = False
    monkeypatch.setattr(process_module, "parse_scienti", lambda *_args, **_kwargs: parsed_entry())

    process_module.process_one(
        {"TXT_NME_PROD": "Work"}, MagicMock(), collection, {}, es_handler, similarity=True,
        es_semaphore=BoundedSemaphore(1),
    )

    es_handler.insert_work.assert_called_once()
    assert es_handler.insert_work.call_args.kwargs["_id"] == "mongo-id"
    es_handler.search_work.assert_not_called()


def test_search_normalizes_missing_year_and_uses_parsed_pages(monkeypatch):
    collection = MagicMock()
    collection.find_one.return_value = None
    es_handler = MagicMock()
    es_handler.search_work.return_value = None
    insert = MagicMock()
    monkeypatch.setattr(process_module, "parse_scienti", lambda *_args, **_kwargs: parsed_entry())
    monkeypatch.setattr(process_module, "process_one_insert", insert)

    process_module.process_one(
        {"TXT_NME_PROD": "Work"}, MagicMock(), collection, {}, es_handler, similarity=True,
        es_semaphore=BoundedSemaphore(1),
    )

    kwargs = es_handler.search_work.call_args.kwargs
    assert kwargs["year"] == "0"
    assert kwargs["page_start"] == "10"
    assert kwargs["page_end"] == "20"
    assert insert.call_args.kwargs["es_semaphore"] is not None


def test_empty_parsed_title_skips_search(monkeypatch):
    entry = parsed_entry()
    entry["titles"] = []
    es_handler = MagicMock()
    monkeypatch.setattr(process_module, "parse_scienti", lambda *_args, **_kwargs: entry)

    process_module.process_one(
        {"TXT_NME_PROD": "Work"}, MagicMock(), MagicMock(), {}, es_handler, similarity=True,
        es_semaphore=BoundedSemaphore(1),
    )

    es_handler.search_work.assert_not_called()


def test_blank_raw_title_skips_before_parser(monkeypatch):
    parser = MagicMock()
    monkeypatch.setattr(process_module, "parse_scienti", parser)

    process_module.process_one(
        {"TXT_NME_PROD": "   "}, MagicMock(), MagicMock(), {},
        MagicMock(), similarity=True, es_semaphore=BoundedSemaphore(1),
    )

    parser.assert_not_called()
