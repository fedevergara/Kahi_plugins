import mongomock
from bson import ObjectId
from kahi_post_person_work_cleaning.Kahi_post_person_work_cleaning import Kahi_post_person_work_cleaning


def plugin(db, dry_run):
    value = object.__new__(Kahi_post_person_work_cleaning)
    value.works = db.works
    value.person = db.person
    value.affiliations = db.affiliations
    value.dry_run = dry_run
    return value


def test_dry_run_reports_without_unlinking():
    db = mongomock.MongoClient().db
    person_id = ObjectId()
    author = {"_id": person_id, "full_name": "Ana Pérez", "external_ids": [{"source": "scienti", "id": {"COD_RH": "1"}}], "affiliations": [{"id": "A"}]}
    db.person.insert_one({"_id": ObjectId(), "full_name": "Ana Pérez", "external_ids": [{"source": "scienti", "id": {"COD_RH": "2"}}], "affiliations": [{"id": "B"}]})
    db.works.insert_one({"authors": [{"id": person_id, "full_name": "Ana Pérez", "affiliations": [{"id": "B"}]}], "external_ids": []})

    assert plugin(db, True).process_one(author) == 1
    assert db.works.find_one()["authors"][0]["id"] == person_id


def test_apply_relinks_only_target_author():
    db = mongomock.MongoClient().db
    person_id = ObjectId()
    other_id = ObjectId()
    author = {"_id": person_id, "full_name": "Ana Pérez", "external_ids": [{"source": "scienti", "id": {"COD_RH": "1"}}], "affiliations": [{"id": "A"}]}
    candidate_id = ObjectId()
    db.person.insert_one({"_id": candidate_id, "full_name": "Ana Pérez correcta", "external_ids": [{"source": "scienti", "id": {"COD_RH": "2"}}], "affiliations": [{"id": "B"}]})
    db.works.insert_one({"authors": [
        {"id": person_id, "full_name": "Ana Pérez correcta", "affiliations": [{"id": "B"}]},
        {"id": other_id, "affiliations": []},
    ], "external_ids": []})

    assert plugin(db, False).process_one(author) == 1
    authors = db.works.find_one()["authors"]
    assert authors[0]["id"] == candidate_id
    assert authors[0]["full_name"] == "Ana Pérez correcta"
    assert authors[1]["id"] == other_id


def test_does_not_unlink_without_a_better_homonym():
    db = mongomock.MongoClient().db
    person_id = ObjectId()
    author = {"_id": person_id, "full_name": "Ana Pérez", "external_ids": [], "affiliations": [{"id": "A"}]}
    db.works.insert_one({"authors": [{"id": person_id, "full_name": "Ana Pérez", "affiliations": [{"id": "B"}]}], "external_ids": []})

    assert plugin(db, False).process_one(author) == 0
    assert db.works.find_one()["authors"][0]["id"] == person_id


def test_openalex_only_duplicate_is_not_distinct_identity():
    db = mongomock.MongoClient().db
    person_id = ObjectId()
    author = {"_id": person_id, "full_name": "Ana Pérez", "external_ids": [{"source": "scienti", "id": {"COD_RH": "1"}}], "affiliations": [{"id": "A"}]}
    db.person.insert_one({"_id": ObjectId(), "full_name": "Ana Pérez", "external_ids": [{"source": "openalex", "id": "https://openalex.org/A1"}], "affiliations": [{"id": "B"}]})
    db.works.insert_one({"authors": [{"id": person_id, "full_name": "Ana Pérez", "affiliations": [{"id": "B"}]}], "external_ids": []})

    assert plugin(db, False).process_one(author) == 0
    assert db.works.find_one()["authors"][0]["id"] == person_id


def test_shared_national_id_overrides_conflicting_cod_rh():
    db = mongomock.MongoClient().db
    person_id = ObjectId()
    author = {"_id": person_id, "full_name": "Ana Pérez", "external_ids": [
        {"source": "scienti", "id": {"COD_RH": "1"}},
        {"source": "Cédula de Ciudadanía", "id": "001234"},
    ], "affiliations": [{"id": "A"}]}
    db.person.insert_one({"_id": ObjectId(), "full_name": "Ana Pérez", "external_ids": [
        {"source": "scienti", "id": {"COD_RH": "2"}},
        {"source": "Cédula de Ciudadanía", "id": "1234"},
    ], "affiliations": [{"id": "B"}]})
    db.works.insert_one({"authors": [{"id": person_id, "full_name": "Ana Pérez", "affiliations": [{"id": "B"}]}], "external_ids": []})

    assert plugin(db, False).process_one(author) == 0
    assert db.works.find_one()["authors"][0]["id"] == person_id


def test_hierarchical_affiliation_keeps_link():
    db = mongomock.MongoClient().db
    person_id = ObjectId()
    author = {"_id": person_id, "full_name": "Ana Pérez", "external_ids": [], "affiliations": [{"id": "A"}]}
    db.affiliations.insert_one({"_id": "B", "relations": [{"id": "A"}]})
    db.person.insert_one({"_id": ObjectId(), "full_name": "Ana Pérez", "affiliations": [{"id": "B"}]})
    db.works.insert_one({"authors": [{"id": person_id, "full_name": "Ana Pérez", "affiliations": [{"id": "B"}]}], "external_ids": []})

    assert plugin(db, False).process_one(author) == 0
    assert db.works.find_one()["authors"][0]["id"] == person_id


def test_cod_rh_match_keeps_link():
    db = mongomock.MongoClient().db
    person_id = ObjectId()
    author = {
        "_id": person_id,
        "full_name": "Ana Pérez",
        "external_ids": [{"source": "scienti", "id": {"COD_RH": "123"}}],
        "affiliations": [{"id": "A"}],
    }
    db.person.insert_one({"_id": ObjectId(), "full_name": "Ana Pérez", "affiliations": [{"id": "B"}]})
    db.works.insert_one({
        "authors": [{"id": person_id, "full_name": "Ana Pérez", "affiliations": [{"id": "B"}]}],
        "external_ids": [{"source": "scienti", "id": {"COD_RH": "123"}}],
    })

    assert plugin(db, False).process_one(author) == 0
    assert db.works.find_one()["authors"][0]["id"] == person_id
