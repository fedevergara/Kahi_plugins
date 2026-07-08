from copy import deepcopy
import mongomock
from kahi.KahiBase import KahiBase
from kahi_siiu_projects.parser import parse_siiu
from kahi_siiu_projects.process_one import process_one


def empty():
    return deepcopy(object.__new__(KahiBase).empty_project())


def record():
    return {
        "CODIGO": "P-1", "NOMBRE_COMPLETO": "Proyecto SIIU",
        "project_participant": [{
            "PERSONA_NATURAL": None,
            "project_participant_role": [{"IDENTIFICADOR": 307}],
            "INSTITUCION": "890980040",
        }],
    }


def test_null_principal_is_ignored():
    entry = parse_siiu(record(), empty())
    assert entry["authors"] == []


def test_retry_is_idempotent_by_code():
    db = mongomock.MongoClient().db
    for _ in range(2):
        process_one(record(), db, db.projects, empty(), None)
    assert db.projects.count_documents({}) == 1
