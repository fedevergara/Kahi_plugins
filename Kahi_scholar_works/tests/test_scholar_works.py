from copy import deepcopy
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Lock
from time import sleep
from unittest.mock import MagicMock, patch

from bson import ObjectId
from kahi.KahiBase import KahiBase
from pymongo import TEXT

import kahi_scholar_works.Kahi_scholar_works as scholar_module
import kahi_scholar_works.parser as parser_module
import kahi_scholar_works.process_one as process_module


def empty_work():
    return deepcopy(object.__new__(KahiBase).empty_work())


def scholar_record(**overrides):
    record = {
        "doi": "10.1000/test",
        "cid": "scholar:123",
        "title": "A Scholar Title",
        "abstract": "A useful abstract",
        "year": "2024",
        "volume": "1",
        "issue": "2",
        "pages": "10--20",
        "bibtex": "@article{test}",
        "cites": "5",
        "cites_link": "https://scholar.example/cites",
        "pdf": "https://scholar.example/paper.pdf",
        "journal": "Example Journal",
        "author": "Doe, Jane",
        "profiles": {"Jane Doe": "profile:1"},
    }
    record.update(overrides)
    return record


def target_work(**overrides):
    work = {
        "_id": ObjectId(),
        "updated": [],
        "titles": [],
        "abstracts": [],
        "external_ids": [
            {
                "provenance": "openalex",
                "source": "doi",
                "id": "https://doi.org/10.1000/test",
            }
        ],
        "types": [],
        "bibliographic_info": {},
        "external_urls": [],
        "citations_count": [],
    }
    work.update(overrides)
    return work


def updated_fields(collection):
    return collection.update_one.call_args.args[1]["$set"]


def test_update_skips_existing_normalized_title_and_abstract_processing():
    title = {
        "title": "  A  SCHOLAR title ",
        "lang": "en",
        "source": "openalex"}
    abstract = {
        "abstract": {
            "Existing": [0]},
        "lang": "en",
        "source": "openalex"}
    work = target_work(titles=[title],
                       abstracts=[abstract],
                       types=[{"provenance": "scholar",
                               "source": "scholar",
                               "type": "article",
                               "level": None,
                               }],
                       external_urls=[{"provenance": "scholar",
                                       "source": "scholar citations",
                                       "url": "https://scholar.example/cites",
                                       }],
                       citations_count=[{"provenance": "scholar",
                                         "source": "scholar",
                                         "count": 5}],
                       )
    collection = MagicMock()

    with patch.object(parser_module, "lang_poll") as lang_poll:
        process_module.process_one_update(
            scholar_record(), work, collection, empty_work()
        )

    lang_poll.assert_not_called()
    fields = updated_fields(collection)
    assert fields["titles"] == [title]
    assert fields["abstracts"] == [abstract]
    assert len(fields["types"]) == 1
    assert len(fields["external_urls"]) == 2
    assert len({url["url"] for url in fields["external_urls"]}) == 2
    assert len(fields["citations_count"]) == 1


def test_update_adds_abstract_only_when_missing_and_adds_new_title():
    work = target_work(
        titles=[{"title": "Different title", "lang": "en", "source": "openalex"}]
    )
    collection = MagicMock()

    with ExitStack() as stack:
        lang_poll = stack.enter_context(
            patch.object(parser_module, "lang_poll", return_value="en")
        )
        stack.enter_context(patch.object(
            parser_module,
            "text_to_inverted_index",
            return_value={"A": [0], "useful": [1], "abstract": [2]},
        ))
        process_module.process_one_update(
            scholar_record(), work, collection, empty_work()
        )

    assert lang_poll.call_count == 2
    fields = updated_fields(collection)
    assert len(fields["titles"]) == 2
    assert fields["titles"][-1]["source"] == "scholar"
    assert fields["abstracts"] == [
        {
            "abstract": {"A": [0], "useful": [1], "abstract": [2]},
            "lang": "en",
            "source": "scholar",
            "provenance": "scholar",
        }
    ]


def test_already_updated_work_backfills_only_missing_abstract():
    work = target_work(updated=[{"source": "scholar", "time": 1}])
    collection = MagicMock()

    with ExitStack() as stack:
        stack.enter_context(patch.object(parser_module, "lang_poll", return_value="en"))
        stack.enter_context(patch.object(
            parser_module, "text_to_inverted_index", return_value={"text": [0]}
        ))
        process_module.process_one_update(
            scholar_record(), work, collection, empty_work()
        )

    assert set(updated_fields(collection)) == {"abstracts"}
    assert updated_fields(collection)["abstracts"][0]["source"] == "scholar"


def test_process_one_uses_minimal_work_projection():
    collection = MagicMock()
    collection.find_one.return_value = target_work()

    with patch.object(process_module, "process_one_update") as update:
        process_module.process_one(
            scholar_record(),
            MagicMock(),
            collection,
            empty_work(),
            similarity=False,
            es_handler=None,
        )

    assert (
        collection.find_one.call_args.args[1] == process_module.WORK_UPDATE_PROJECTION
    )
    update.assert_called_once()


def test_text_index_is_removed_for_doi_and_created_for_similarity():
    doi_collection = MagicMock()
    doi_collection.index_information.return_value = {"titles.title_text": {}}
    scholar_module.configure_work_indexes(doi_collection, "doi")
    doi_collection.drop_index.assert_called_once_with("titles.title_text")

    similarity_collection = MagicMock()
    similarity_collection.index_information.return_value = {}
    scholar_module.configure_work_indexes(similarity_collection, None)
    assert (([("titles.title", TEXT)],), {}) in [
        (call.args, call.kwargs)
        for call in similarity_collection.create_index.call_args_list
    ]


def test_process_scholar_streams_cursor_with_projection():
    class Cursor:
        def __init__(self):
            self.yielded = 0
            self.batch = None
            self.closed = False

        def batch_size(self, size):
            self.batch = size
            return self

        def __iter__(self):
            self.yielded += 1
            yield scholar_record()

        def close(self):
            self.closed = True

    cursor = Cursor()
    source_collection = MagicMock()
    source_collection.find.return_value = cursor
    client = MagicMock()
    instance = object.__new__(scholar_module.Kahi_scholar_works)
    instance.task = "doi"
    instance.scholar_collection = source_collection
    instance.mongodb_url = "localhost:27017"
    instance.config = {"database_name": "test"}
    instance.n_jobs = 1
    instance.verbose = 0
    instance.es_handler = None

    class RecordingParallel:
        def __init__(self, **_kwargs):
            pass

        def __call__(self, tasks):
            assert cursor.yielded == 0
            list(tasks)
            return []

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(scholar_module, "MongoClient", return_value=client)
        )
        stack.enter_context(
            patch.object(scholar_module, "Parallel", RecordingParallel)
        )
        instance.process_scholar()

    assert (
        source_collection.find.call_args.args[1]
        == scholar_module.SCHOLAR_SOURCE_PROJECTION
    )
    assert cursor.batch == 1000
    assert cursor.closed


def test_parser_tolerates_optional_null_and_malformed_values():
    record = scholar_record(
        cid=None,
        journal=None,
        author=None,
        profiles=None,
        year=2024,
        cites="not-a-number",
        volume=1,
        issue=2,
        pages=10,
    )

    with patch.object(parser_module, "lang_poll", return_value="en"):
        entry = parser_module.parse_scholar(record, empty_work())

    assert entry["year_published"] == 2024
    assert entry["bibliographic_info"]["volume"] == "1"
    assert entry["bibliographic_info"]["issue"] == "2"
    assert entry["source"] == {"name": "", "external_ids": []}
    assert entry["citations_count"] == []
    assert entry["authors"] == []


def test_parser_preserves_doi_when_scholar_cid_is_present():
    with patch.object(parser_module, "lang_poll", return_value="en"):
        entry = parser_module.parse_scholar(scholar_record(), empty_work())

    identifiers = {item["source"]: item["id"]
                   for item in entry["external_ids"]}
    assert identifiers == {
        "doi": "https://doi.org/10.1000/test",
        "scholar": "scholar:123",
    }


def test_es_document_normalizes_empty_year_and_uses_parsed_pages():
    entry = empty_work()
    entry.update({
        "titles": [{"title": "Valid title"}],
        "source": {"name": "Journal"},
        "year_published": "",
        "bibliographic_info": {"start_page": "10", "end_page": "20"},
        "authors": [{"full_name": "Jane Doe"}],
    })

    document = process_module.build_es_work(entry)

    assert document["year"] == 0
    assert document["first_page"] == "10"
    assert document["last_page"] == "20"


def test_existing_scholar_cid_is_reconciled_without_duplicate_insert():
    work_id = ObjectId()
    collection = MagicMock()
    collection.find_one.return_value = {"_id": work_id}
    es_handler = MagicMock()
    es_handler.es_index = "works"
    es_handler.es.exists.return_value = True

    process_module.process_one(
        scholar_record(doi=""), MagicMock(), collection, empty_work(),
        similarity=True, es_handler=es_handler,
    )

    collection.find_one.assert_called_once_with(
        process_module.scholar_cid_query("scholar:123"), {"_id": 1}
    )
    es_handler.search_work.assert_not_called()
    es_handler.insert_work.assert_not_called()


def test_es_semaphore_limits_concurrent_operations():
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
            executor.submit(process_module.run_es_operation, semaphore, operation)
            for _ in range(16)
        ]
        for future in futures:
            future.result()

    assert state["maximum"] == 2


def test_blank_title_is_skipped_before_database_or_es_queries():
    collection = MagicMock()
    es_handler = MagicMock()

    process_module.process_one(
        scholar_record(doi="", title="  "), MagicMock(), collection,
        empty_work(), similarity=True, es_handler=es_handler,
    )

    collection.find_one.assert_not_called()
    es_handler.search_work.assert_not_called()
