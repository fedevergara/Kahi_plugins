from pymongo.errors import DuplicateKeyError


def persistent_person_id(person, source):
    """Return a normalized persistent id, or ``None`` for malformed data."""
    original_id = person.get("_id")
    if source == "mongodb_id":
        raw_id = str(original_id) if original_id is not None else None
    else:
        raw_id = next(
            (
                external_id.get("id")
                for external_id in person.get("external_ids", [])
                if external_id.get("source") == source
            ),
            None,
        )

    if source == "scienti":
        if not isinstance(raw_id, dict):
            return None
        raw_id = raw_id.get("COD_RH")

    if raw_id is None:
        return None
    if not isinstance(raw_id, str):
        raw_id = str(raw_id)
    persistent_id = raw_id.split("/")[-1].replace("-", "").split("=")[-1].strip()
    return persistent_id or None


def update_product_references(product_cols, original_id, persistent_id):
    """Idempotently replace an old person id in every product collection."""
    for product_col in product_cols:
        product_col.update_many(
            {"authors.id": original_id},
            {"$set": {"authors.$[author].id": persistent_id}},
            array_filters=[{"author.id": original_id}],
        )


def recover_person_id_migration(person_col, product_cols, person):
    """Finish references left pending by an interrupted id migration."""
    original_id = person.get("_id_old")
    persistent_id = person.get("_id")
    if original_id is None or persistent_id is None:
        return False
    update_product_references(product_cols, original_id, persistent_id)
    person_col.update_one(
        {"_id": persistent_id},
        {"$set": {"_id_migration_complete": True}},
    )
    return True


def process_person_id(client, person_col, product_cols, person, source):
    """Move a person to a persistent id and update product references safely."""
    original_id = person["_id"]
    persistent_id = persistent_person_id(person, source)
    if not persistent_id:
        print(
            "WARNING: No usable persistent id for person",
            original_id,
            "source:",
            source,
        )
        return None

    if persistent_id == original_id:
        person_col.update_one(
            {"_id": original_id},
            {"$set": {
                "_id_old": original_id,
                "_id_migration_complete": True,
            }},
        )
        return persistent_id

    new_doc = person.copy()
    new_doc["_id"] = persistent_id
    new_doc["_id_old"] = original_id
    new_doc["_id_migration_complete"] = False

    try:
        with client.start_session() as session:
            with session.start_transaction():
                person_col.insert_one(new_doc, session=session)
                person_col.delete_one({"_id": original_id}, session=session)
    except DuplicateKeyError:
        # A different person already owns this persistent id. Leave the
        # original document untouched so a lower-priority id can be tried.
        print(
            "WARNING: Persistent person id already exists:",
            source,
            persistent_id,
            str(original_id),
        )
        return None
    except Exception as error:
        print(
            "ERROR: Could not migrate persistent person id:",
            source,
            persistent_id,
            str(original_id),
            error,
        )
        return None

    # Product updates are intentionally recoverable. If execution stops here,
    # the next run finds the marker set to false and repeats these idempotent
    # updates before starting new migrations.
    update_product_references(product_cols, original_id, persistent_id)
    person_col.update_one(
        {"_id": persistent_id},
        {"$set": {"_id_migration_complete": True}},
    )
    return persistent_id
