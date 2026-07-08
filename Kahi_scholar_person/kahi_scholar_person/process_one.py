from time import time


def external_scholar_id(author):
    return next(
        (record.get("id") for record in author.get("external_ids", []) if record.get("source") == "scholar"),
        None,
    )


def freeze_identity(value):
    if isinstance(value, dict):
        return tuple(sorted((key, freeze_identity(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(freeze_identity(item) for item in value)
    return value


def related_work_key(work):
    identifier = work.get("id")
    if isinstance(identifier, str):
        identifier = identifier.lower()
    return work.get("source"), freeze_identity(identifier)


def name_score(name):
    tokens = str(name or "").split()
    expanded = sum(len(token) > 2 for token in tokens)
    reasonable_length = 1 if 2 <= len(tokens) <= 6 else 0
    return reasonable_length, expanded, min(len(tokens), 6), len(str(name or ""))


def merge_author_entries(current, incoming):
    if name_score(incoming.get("full_name")) > name_score(current.get("full_name")):
        old_name = current.get("full_name")
        current["full_name"] = incoming["full_name"]
        current["first_names"] = incoming.get("first_names", [])
        current["last_names"] = incoming.get("last_names", [])
        current["initials"] = incoming.get("initials", "")
        if old_name:
            current.setdefault("aliases", []).append(old_name)

    aliases = current.setdefault("aliases", [])
    aliases.extend(incoming.get("aliases", []))
    aliases.append(incoming.get("full_name", ""))
    current["aliases"] = list(dict.fromkeys(alias for alias in aliases if alias and alias != current.get("full_name")))

    existing_work_keys = {related_work_key(work) for work in current.get("related_works", [])}
    for work in incoming.get("related_works", []):
        key = related_work_key(work)
        if key not in existing_work_keys:
            current.setdefault("related_works", []).append(work)
            existing_work_keys.add(key)
    return current


def process_one(author, collection, verbose=0):
    scholar_id = external_scholar_id(author)
    if not scholar_id:
        return "skipped_without_id"

    author_db = collection.find_one({"external_ids.id": scholar_id})
    if not author_db:
        collection.insert_one(author)
        return "inserted"

    trusted_sources = {
        update.get("source")
        for update in author_db.get("updated", [])
        if update.get("source") != "scholar"
    }
    set_fields = {}
    incoming_name = author.get("full_name", "")
    if not author_db.get("full_name") or not trusted_sources:
        if name_score(incoming_name) > name_score(author_db.get("full_name")):
            set_fields.update({
                "full_name": incoming_name,
                "first_names": author.get("first_names", []),
                "last_names": author.get("last_names", []),
                "initials": author.get("initials", ""),
            })

    aliases = list(author.get("aliases", []))
    if incoming_name and incoming_name != author_db.get("full_name"):
        aliases.append(incoming_name)
    aliases = list(dict.fromkeys(alias for alias in aliases if alias))

    existing_work_keys = {related_work_key(work) for work in author_db.get("related_works", [])}
    new_works = [
        work for work in author.get("related_works", [])
        if related_work_key(work) not in existing_work_keys
    ]

    add_to_set = {}
    if not any(update.get("source") == "scholar" for update in author_db.get("updated", [])):
        add_to_set["updated"] = {"source": "scholar", "time": int(time())}
    if aliases:
        add_to_set["aliases"] = {"$each": aliases}
    if new_works:
        add_to_set["related_works"] = {"$each": new_works}

    operation = {}
    if set_fields:
        operation["$set"] = set_fields
    if add_to_set:
        operation["$addToSet"] = add_to_set
    if operation:
        collection.update_one({"_id": author_db["_id"]}, operation)
    return "updated"
