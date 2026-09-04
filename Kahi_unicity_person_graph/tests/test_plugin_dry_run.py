import copy
import importlib
from unittest.mock import patch

import mongomock
from bson import ObjectId


plugin_module = importlib.import_module(
    "kahi_unicity_person_graph.Kahi_unicity_person_graph"
)
KahiUnicityPersonGraph = plugin_module.Kahi_unicity_person_graph


def person(person_id, source, external_ids):
    return {
        "_id": person_id,
        "updated": [{"source": source, "time": 1}],
        "full_name": "Ada Lovelace",
        "first_names": ["Ada"],
        "last_names": ["Lovelace"],
        "initials": "AL",
        "aliases": [],
        "affiliations": [],
        "keywords": [],
        "external_ids": external_ids,
        "sex": "",
        "marital_status": None,
        "ranking": [],
        "birthplace": {},
        "birthdate": -1,
        "degrees": [],
        "subjects": [],
        "citations_count": [],
        "products_count": 0,
        "related_works": [],
    }


def test_dry_run_preserves_person_documents_and_stores_one_plan():
    client = mongomock.MongoClient()
    database = client["kahi"]
    person_ids = [ObjectId() for _ in range(3)]
    documents = [
        person(
            person_ids[0],
            "staff",
            [
                {
                    "source": "orcid",
                    "id": "https://orcid.org/0000-0003-2895-3084",
                    "provenance": "staff",
                }
            ],
        ),
        person(
            person_ids[1],
            "scopus",
            [
                {
                    "source": "orcid",
                    "id": "https://orcid.org/0000-0003-2895-3084",
                    "provenance": "scopus",
                },
                {
                    "source": "scopus",
                    "id": "https://www.scopus.com/authid/detail.uri?authorId=12345678",
                    "provenance": "scopus",
                },
            ],
        ),
        person(
            person_ids[2],
            "scholar",
            [
                {
                    "source": "scopus",
                    "id": "https://www.scopus.com/authid/detail.uri?authorId=12345678",
                    "provenance": "scholar",
                }
            ],
        ),
    ]
    database.person.insert_many(copy.deepcopy(documents))
    original_documents = list(database.person.find().sort("_id"))
    config = {
        "database_url": "mongodb://unused",
        "database_name": "kahi",
        "unicity_person_graph": {
            "collection_name": "person",
            "task": ["orcid", "scopus"],
            "num_jobs": 2,
            "dry_run": True,
            "component_batch_size": 1,
            "audit_batch_size": 1,
        },
    }

    with patch.object(plugin_module, "MongoClient", return_value=client):
        plugin = KahiUnicityPersonGraph(config)
        assert plugin.run() == 0

    assert list(database.person.find().sort("_id")) == original_documents
    plans = list(database.person_graph_merged_sets.find())
    assert len(plans) == 1
    assert plans[0]["status"] == "planned"
    assert plans[0]["target_author"]["_id"] == person_ids[0]
    assert set(plans[0]["absorbed"]) == {person_ids[1], person_ids[2]}
    assert plans[0]["proposed_document"]["_id"] == person_ids[0]
    assert set(plans[0]["proposed_document"]) == set(original_documents[0])
    assert len(plans[0]["match_details"]) == 2
    assert all(match["compare_author"] for match in plans[0]["match_details"])
    assert plans[0]["confidence"] == "high"

    run = database.person_graph_runs.find_one()
    assert run["status"] == "planned"
    assert run["candidate_groups"] == 2
    assert run["candidate_edges"] == 2
    assert run["components"] == 1
    assert run["processed_components"] == 1
    assert run["component_batch_size"] == 1
    assert run["audit_batch_size"] == 1
    assert run["absorbed_documents"] == 2
    assert run["high_confidence_plans"] == 1
    assert run["medium_confidence_plans"] == 0
    assert run["review_plans"] == 0
    assert run["rejected_orcid_conflicts"] == 0
    assert run["rejected_national_id_conflicts"] == 0
    assert run["rejected_identity_conflicts"] == 0
    assert run["rejected_name_mismatches"] == 0
    assert run["rejected_given_name_conflicts"] == 0
    assert run["rejected_weak_bridges"] == 0
    assert run["max_authors_threshold"] == 10
    assert run["single_doi_exact_name_max_authors"] == 50
    assert run["max_profiles_per_doi"] == 100
