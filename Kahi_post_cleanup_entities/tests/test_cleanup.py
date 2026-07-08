import mongomock
from bson import ObjectId
from kahi_post_cleanup_entities.Kahi_post_cleanup_entities import Kahi_post_cleanup_entities


def plugin(db, dry_run=True):
    value = object.__new__(Kahi_post_cleanup_entities)
    value.person = db.person
    value.affiliations = db.affiliations
    value.works = db.works
    value.entity_collections = [db.works, db.events, db.projects, db.patents]
    value.dry_run = dry_run
    return value


def test_person_referenced_only_by_project_is_preserved():
    db = mongomock.MongoClient().db
    person_id = ObjectId()
    db.person.insert_one({"_id": person_id})
    db.projects.insert_one({"authors": [{"id": person_id}]})
    assert plugin(db).cleanup_author({"_id": person_id}) == 0


def test_affiliation_relation_is_preserved():
    db = mongomock.MongoClient().db
    db.affiliations.insert_many([
        {"_id": "A"}, {"_id": "B", "relations": [{"id": "A"}]},
    ])
    assert plugin(db).cleanup_affiliation({"_id": "A"}) == 0


def test_dry_run_does_not_delete_orphan():
    db = mongomock.MongoClient().db
    person_id = ObjectId()
    db.person.insert_one({"_id": person_id})
    assert plugin(db, True).cleanup_author({"_id": person_id}) == 1
    assert db.person.count_documents({"_id": person_id}) == 1
