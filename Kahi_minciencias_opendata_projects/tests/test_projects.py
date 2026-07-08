from copy import deepcopy
import mongomock
from kahi.KahiBase import KahiBase
from kahi_minciencias_opendata_projects.parser import parse_minciencias_opendata
from kahi_minciencias_opendata_projects.process_one import process_one


def empty():
    return deepcopy(object.__new__(KahiBase).empty_project())


def record(title="Proyecto controlado"):
    return {
        "id_producto_pd": "PID-0000123456-1",
        "id_persona_pd": "0000123456",
        "nme_producto_pd": title,
        "nme_tipologia_pd": "Proyecto de Investigacion y Desarrollo",
        "nme_clase_pd": "Proyecto",
        "cod_grupo_gr": "G1"}


def test_blank_title_is_skipped():
    assert parse_minciencias_opendata(record(""), empty()) is None


def test_retry_is_idempotent():
    db = mongomock.MongoClient().db
    for _ in range(2):
        process_one(record(), db, db.projects, empty(), None, False, None)
    assert db.projects.count_documents({}) == 1
