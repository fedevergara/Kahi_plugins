import importlib
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Lock
from time import sleep
from unittest.mock import MagicMock, patch

from pandas import DataFrame
from bson import ObjectId
from kahi.KahiBase import KahiBase
from kahi_impactu_utils.Mapping import ciarp_mapping

import kahi_ciarp_works.process_one as process_module
from kahi_ciarp_works.parser import parse_ciarp


ciarp_module = importlib.import_module("kahi_ciarp_works.Kahi_ciarp_works")


def test_doi_task_skips_institution_without_valid_doi_before_mapping():
    instance = object.__new__(ciarp_module.Kahi_ciarp_works)
    instance.task = "doi"
    instance.verbose = 1
    instance.mongodb_url = "localhost:27017"
    instance.config = {
        "database_name": "target",
        "ciarp_works": {},
    }
    affiliation = {
        "_id": "04cmc9894",
        "names": [{"name": "Universidad Pedagógica Nacional"}],
        "types": [],
    }
    affiliations = MagicMock()
    affiliations.find_one.return_value = affiliation
    instance.db = {"affiliations": affiliations}
    instance._institution_ids = lambda: ["04cmc9894"]
    instance._config_for_institution = lambda _institution_id: {}
    instance._load_ciarp_data = lambda _database, _institution_id: DataFrame(
        [{"doi": "0", "ranking": "Artículo", "identificación": "1"}]
    )

    with patch.object(ciarp_module, "ciarp_mapping") as mapping:
        instance.process_ciarp()

    mapping.assert_not_called()


def ciarp_record():
    return {
        "título": "Controlled work",
        "idioma": "es",
        "doi": "",
        "issn": "",
        "isbn": "",
        "revista": "",
        "año": "2024",
        "volumen": "",
        "issue": "",
        "primera_página": "10",
        "última_página": "20",
        "ranking": "Artículo",
        "identificación": "123",
        "código_unidad_académica": "",
        "código_subunidad_académica": "",
        "index": "timestamp-id",
        "source_record_id": "mongo-source-id",
    }


def affiliation():
    return {
        "_id": ObjectId(),
        "names": [{"name": "University", "lang": "en", "source": "ror"}],
        "types": [],
    }


def test_parser_preserves_timestamp_adds_stable_id_and_correct_pages():
    entry = parse_ciarp(ciarp_record(), affiliation(), KahiBase.empty_work(None))

    assert {ext["source"] for ext in entry["external_ids"]} == {
        "ciarp", "ciarp_record"}
    assert process_module.ciarp_record_id(entry) == "mongo-source-id"
    assert entry["bibliographic_info"] == {
        "start_page": "10", "end_page": "20"}


def test_es_document_uses_numeric_year_and_correct_pages():
    record = ciarp_record()
    record["año"] = ""
    entry = parse_ciarp(record, affiliation(), KahiBase.empty_work(None))

    document = process_module.build_es_work(entry)

    assert document["year"] == 0
    assert document["first_page"] == "10"
    assert document["last_page"] == "20"


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
        futures = [executor.submit(
            process_module.run_es_operation, semaphore, operation)
            for _ in range(16)]
        for future in futures:
            future.result()

    assert maximum == 2


def test_existing_source_record_reconciles_es_without_search(monkeypatch):
    collection = MagicMock()
    collection.find_one.return_value = {"_id": "mongo-id"}
    es_handler = MagicMock()
    es_handler.es_index = "works"
    es_handler.es.exists.return_value = False
    monkeypatch.setattr(
        process_module, "parse_ciarp",
        lambda *_args, **_kwargs: parse_ciarp(
            ciarp_record(), affiliation(), KahiBase.empty_work(None)))

    process_module.process_one(
        ciarp_record(), MagicMock(), collection, affiliation(),
        KahiBase.empty_work(None), True, es_handler,
        es_semaphore=BoundedSemaphore(1))

    es_handler.insert_work.assert_called_once()
    es_handler.search_work.assert_not_called()


def test_blank_title_is_skipped_before_parser(monkeypatch):
    parser = MagicMock()
    monkeypatch.setattr(process_module, "parse_ciarp", parser)
    record = ciarp_record()
    record["título"] = "   "

    process_module.process_one(
        record, MagicMock(), MagicMock(), affiliation(),
        KahiBase.empty_work(None), True, MagicMock())

    parser.assert_not_called()


def test_shared_ciarp_catalogue_includes_04cmc9894():
    assert "Articulo en revista Tipo A1" in ciarp_mapping(
        "https://ror.org/04cmc9894", "works")
