from kahi_impactu_postcalculations import topics
from kahi_impactu_postcalculations import process_one
from kahi_impactu_postcalculations.process_one import (
    split_network_edges,
    top_words_from_works_scan,
)
from kahi_impactu_postcalculations.Kahi_impactu_postcalculations import (
    Kahi_impactu_postcalculations,
)


class _Response:
    status_code = 200

    def json(self):
        return [[{"topic_id": 123, "topic_score": 0.9}]]


class _Works:
    def __init__(self):
        self.operations = []

    def bulk_write(self, operations, ordered=False):
        self.operations.extend(operations)
        return type("Result", (), {"modified_count": len(operations)})()


class _OpenAlexTopics:
    def find(self, query, projection):
        return [{"id": "https://openalex.org/T123", "display_name": "Topic"}]


class _Token:
    def __init__(self, lemma):
        self.lemma_ = lemma


class _Model:
    def __call__(self, text):
        return [_Token(word) for word in text.split()]


def test_topic_inference_updates_and_deduplicates(monkeypatch):
    monkeypatch.setattr(
        topics, "request_topic_inference_batch", lambda *a, **k: _Response()
    )
    works = _Works()
    work = {
        "_id": "w1",
        "titles": [{"title": "A title"}],
        "abstracts": [{"abstract": {"word": [0]}}],
        "source": {"name": "Journal"},
        "primary_topic": {},
        "topics": [],
    }
    assert topics.process_topic(works, _OpenAlexTopics(), work)
    assert len(works.operations) == 1
    operation = works.operations[0]
    assert operation._filter == {"_id": "w1", "primary_topic": {}}
    saved = operation._doc["$set"]
    assert saved["primary_topic"]["id"] == "https://openalex.org/T123"
    assert len(saved["topics"]) == 1


def test_topic_batch_uses_one_request_and_one_bulk_write(monkeypatch):
    captured = []

    class _BatchResponse:
        status_code = 200

        def json(self):
            return [
                [{"topic_id": 123, "topic_score": 0.9}],
                [{"topic_id": 123, "topic_score": 0.8}],
            ]

    def fake_request(payload, *args):
        captured.append(payload)
        return _BatchResponse()

    monkeypatch.setattr(topics, "request_topic_inference_batch", fake_request)
    works = _Works()
    batch = [
        {
            "_id": f"w{index}",
            "titles": [{"title": f"Title {index}"}],
            "abstracts": [{"abstract": {"word": [0]}}],
            "primary_topic": {},
            "topics": [],
        }
        for index in (1, 2)
    ]
    cache = {
        "https://openalex.org/T123": {
            "id": "https://openalex.org/T123",
            "display_name": "Topic",
        }
    }

    assert topics.process_topic_batch(works, cache, batch) == 2
    assert len(captured) == 1
    assert len(captured[0]) == 2
    assert len(works.operations) == 2


def test_topic_batch_retries_before_writing(monkeypatch):
    responses = [
        type("Failure", (), {"status_code": 500, "text": "temporary"})(),
        _Response(),
    ]
    monkeypatch.setattr(
        topics, "request_topic_inference_batch", lambda *a: responses.pop(0)
    )
    monkeypatch.setattr(topics, "sleep", lambda *_: None)
    works = _Works()
    work = {
        "_id": "w1",
        "titles": [{"title": "A title"}],
        "abstracts": [{"abstract": {"word": [0]}}],
        "primary_topic": {},
        "topics": [],
    }
    cache = {
        "https://openalex.org/T123": {
            "id": "https://openalex.org/T123",
            "display_name": "Topic",
        }
    }

    assert topics.process_topic_batch(works, cache, [work], retries=2) == 1
    assert not responses


def test_network_split_preserves_every_edge():
    edges = list(range(11))
    first, second = split_network_edges(edges)
    assert first + second == edges
    assert len(first) == 6
    assert len(second) == 5


def test_network_construction_can_be_disabled():
    plugin = object.__new__(Kahi_impactu_postcalculations)
    plugin.networks_enabled = False
    plugin.process_networks = lambda *args: (_ for _ in ()).throw(
        AssertionError("network construction must not run")
    )

    assert plugin.maybe_process_networks(object(), object(), object()) is False


def test_network_construction_runs_when_enabled():
    plugin = object.__new__(Kahi_impactu_postcalculations)
    plugin.networks_enabled = True
    calls = []
    plugin.process_networks = lambda *args: calls.append(args)
    arguments = (object(), object(), object())

    assert plugin.maybe_process_networks(*arguments) is True
    assert calls == [arguments]


class _Client(dict):
    pass


def test_threading_backend_initializes_databases(monkeypatch):
    called = {}

    def fake_network(db_in, db_out, idx, author_count, force_recalculate):
        called.update(
            db_in=db_in,
            db_out=db_out,
            idx=idx,
            force=force_recalculate,
        )

    monkeypatch.setattr(process_one, "network_creation_affiliations", fake_network)
    input_db = object()
    output_db = object()
    process_one.network_creation_process_one(
        {
            "database_name": "input",
            "impactu_postcalculations": {"database_name": "output"},
        },
        _Client(input=input_db),
        _Client(output=output_db),
        "aff1",
        10,
        "affiliations",
        "threading",
        True,
    )
    assert called == {
        "db_in": input_db,
        "db_out": output_db,
        "idx": "aff1",
        "force": True,
    }


def test_works_scan_top_words_keeps_output_schema():
    import mongomock

    client = mongomock.MongoClient()
    db_in = client["input"]
    db_out = client["output"]
    db_in["works"].insert_many(
        [
            {
                "_id": "w1",
                "titles": [{"title": "salud publica salud", "lang": "es"}],
                "authors": [
                    {"id": "p1", "affiliations": [{"id": "a1"}]},
                    {"id": "p2", "affiliations": [{"id": "a1"}, {"id": "a2"}]},
                ],
            },
            {
                "_id": "w2",
                "titles": [{"title": "datos salud", "lang": "es"}],
                "authors": [{"id": "p1", "affiliations": [{"id": "a2"}]}],
            },
        ]
    )

    top_words_from_works_scan(
        db_in,
        db_out,
        _Model(),
        _Model(),
        set(),
        batch_size=1,
        force_recalculate=True,
    )

    assert db_out["person"].find_one({"_id": "p1"})["top_words"] == [
        {"name": "salud", "value": 3},
        {"name": "datos", "value": 1},
        {"name": "publica", "value": 1},
    ]
    assert db_out["person"].find_one({"_id": "p2"})["top_words"] == [
        {"name": "salud", "value": 2},
        {"name": "publica", "value": 1},
    ]
    assert db_out["affiliations"].find_one({"_id": "a1"})["top_words"][0] == {
        "name": "salud",
        "value": 2,
    }
    assert db_out["top_words_person_tmp"].count_documents({}) == 0
    assert db_out["top_words_affiliations_tmp"].count_documents({}) == 0
