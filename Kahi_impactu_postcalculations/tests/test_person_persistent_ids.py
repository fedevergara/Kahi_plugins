from bson import ObjectId

from kahi_impactu_postcalculations.person_persistent_ids import (
    persistent_person_id,
    process_person_id,
    recover_person_id_migration,
)


def test_persistent_ids_are_normalized():
    assert persistent_person_id(
        {"_id": ObjectId(), "external_ids": [{
            "source": "orcid", "id": "https://orcid.org/0000-0002-1234-5678"
        }]},
        "orcid",
    ) == "0000000212345678"
    assert persistent_person_id(
        {"external_ids": [{"source": "scienti", "id": {"COD_RH": "123"}}]},
        "scienti",
    ) == "123"


def test_malformed_persistent_id_is_skipped():
    assert persistent_person_id(
        {"external_ids": [{"source": "scienti", "id": "bad"}]},
        "scienti",
    ) is None


class _Collection:
    def __init__(self):
        self.updates = []

    def update_many(self, query, update, array_filters=None):
        self.updates.append((query, update, array_filters))

    def update_one(self, query, update):
        self.updates.append((query, update, None))


class _PersonCollection(_Collection):
    def __init__(self):
        super().__init__()
        self.insert_session = None
        self.delete_session = None

    def insert_one(self, document, session=None):
        self.insert_session = session

    def delete_one(self, query, session=None):
        self.delete_session = session


class _Session:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def start_transaction(self):
        return self


class _Client:
    def __init__(self):
        self.session = _Session()

    def start_session(self):
        return self.session


def test_recovery_repeats_reference_updates_and_marks_complete():
    person = _Collection()
    works = _Collection()
    old_id = ObjectId()
    assert recover_person_id_migration(
        person,
        [works],
        {"_id": "persistent", "_id_old": old_id},
    )
    assert works.updates[0][0] == {"authors.id": old_id}
    assert person.updates[-1][1] == {
        "$set": {"_id_migration_complete": True}
    }


def test_person_move_uses_transaction_session_and_completion_marker():
    person = _PersonCollection()
    products = _Collection()
    client = _Client()
    old_id = ObjectId()
    result = process_person_id(
        client,
        person,
        [products],
        {
            "_id": old_id,
            "external_ids": [{"source": "orcid", "id": "0000-0001"}],
        },
        "orcid",
    )
    assert result == "00000001"
    assert person.insert_session is client.session
    assert person.delete_session is client.session
    assert products.updates[0][0] == {"authors.id": old_id}
    assert person.updates[-1][1] == {
        "$set": {"_id_migration_complete": True}
    }
