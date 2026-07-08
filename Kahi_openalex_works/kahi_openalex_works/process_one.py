
from collections import OrderedDict, defaultdict
from copy import deepcopy
from os import getpid
from threading import Lock
import atexit
from kahi_openalex_works.parser import parse_openalex
from time import time
from bson import ObjectId
from pymongo import MongoClient
from mohan.Similarity import Similarity
from kahi_impactu_utils.Utils import normalize_name


_AUTHOR_NAME_CACHE = OrderedDict()
_AUTHOR_NAME_CACHE_LOCK = Lock()
_AUTHOR_NAME_CACHE_MAX_SIZE = 10000
_PROCESS_MONGO_CLIENT = None
_PROCESS_MONGO_CLIENT_PID = None


def close_process_mongo_client():
    """Close the worker-local MongoDB client when its process exits."""
    global _PROCESS_MONGO_CLIENT, _PROCESS_MONGO_CLIENT_PID
    if _PROCESS_MONGO_CLIENT is not None:
        _PROCESS_MONGO_CLIENT.close()
    _PROCESS_MONGO_CLIENT = None
    _PROCESS_MONGO_CLIENT_PID = None


def get_process_mongo_client(database_url):
    """Return one persistent MongoClient per multiprocessing worker."""
    global _PROCESS_MONGO_CLIENT, _PROCESS_MONGO_CLIENT_PID
    pid = getpid()
    if _PROCESS_MONGO_CLIENT is None or _PROCESS_MONGO_CLIENT_PID != pid:
        close_process_mongo_client()
        _PROCESS_MONGO_CLIENT = MongoClient(database_url)
        _PROCESS_MONGO_CLIENT_PID = pid
        atexit.register(close_process_mongo_client)
    return _PROCESS_MONGO_CLIENT


def get_author_name_candidates(person_collection, names):
    """Load exact-name candidates in one query and cache hits and misses."""
    unique_names = set(filter(None, names))
    namespace = person_collection.full_name
    result = {}
    missing = []

    with _AUTHOR_NAME_CACHE_LOCK:
        for name in unique_names:
            key = (namespace, name)
            if key in _AUTHOR_NAME_CACHE:
                _AUTHOR_NAME_CACHE.move_to_end(key)
                result[name] = deepcopy(_AUTHOR_NAME_CACHE[key])
            else:
                missing.append(name)

    if missing:
        loaded = defaultdict(list)
        projection = {
            "_id": 1,
            "full_name": 1,
            "external_ids": 1,
            "affiliations": 1,
        }
        for person in person_collection.find(
                {"full_name": {"$in": missing}}, projection):
            loaded[person["full_name"]].append(person)

        with _AUTHOR_NAME_CACHE_LOCK:
            for name in missing:
                key = (namespace, name)
                candidates = loaded[name]
                _AUTHOR_NAME_CACHE[key] = candidates
                _AUTHOR_NAME_CACHE.move_to_end(key)
                result[name] = deepcopy(candidates)
            while len(_AUTHOR_NAME_CACHE) > _AUTHOR_NAME_CACHE_MAX_SIZE:
                _AUTHOR_NAME_CACHE.popitem(last=False)

    return result


def get_affiliation_ids_by_external_id(db, authors):
    """Resolve all incoming affiliation identifiers with one MongoDB query."""
    external_ids = {
        ext["id"]
        for author in authors
        for affiliation in author.get("affiliations", [])
        for ext in affiliation.get("external_ids", [])
        if ext.get("id")
    }
    if not external_ids:
        return {}

    result = {}
    projection = {"_id": 1, "external_ids.id": 1}
    for affiliation in db["affiliations"].find(
            {"external_ids.id": {"$in": list(external_ids)}}, projection):
        for ext in affiliation.get("external_ids", []):
            if ext.get("id") in external_ids:
                result[ext["id"]] = affiliation["_id"]
    return result


def select_name_candidate(author, candidates, affiliation_ids):
    """Select a unique name match, using affiliations to disambiguate."""
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        return None

    incoming_affiliations = {
        affiliation_ids[ext["id"]]
        for affiliation in author.get("affiliations", [])
        for ext in affiliation.get("external_ids", [])
        if ext.get("id") in affiliation_ids
    }
    if not incoming_affiliations:
        return None

    scored = []
    for candidate in candidates:
        candidate_affiliations = {
            affiliation.get("id")
            for affiliation in candidate.get("affiliations", [])
        }
        scored.append((
            len(incoming_affiliations & candidate_affiliations),
            candidate,
        ))
    best_score = max(score for score, _ in scored)
    best = [candidate for score, candidate in scored if score == best_score]
    return best[0] if best_score > 0 and len(best) == 1 else None


def normalized_author_name(name):
    """Normalize an author name for comparison, never for persistence."""
    return " ".join(normalize_name(name or "").split())


def resolve_person_by_external_ids(person_collection, external_ids):
    """Resolve all available source-qualified IDs to one unambiguous person."""
    pairs = [
        (ext.get("source"), ext.get("id"))
        for ext in external_ids
        if ext.get("id") is not None
    ]
    if not pairs:
        return None

    clauses = []
    for source, identifier in pairs:
        match = {"id": identifier}
        if source:
            match["source"] = source
        clauses.append({"external_ids": {"$elemMatch": match}})

    candidates = list(person_collection.find(
        {"$or": clauses},
        {"_id": 1, "full_name": 1, "external_ids": 1, "affiliations": 1},
    ))
    scored = []
    for candidate in candidates:
        score = sum(
            1
            for source, identifier in pairs
            if any(
                ext.get("id") == identifier
                and (not source or ext.get("source") == source)
                for ext in candidate.get("external_ids", [])
            )
        )
        scored.append((score, candidate))
    if not scored:
        return None
    best_score = max(score for score, _ in scored)
    best = [candidate for score, candidate in scored if score == best_score]
    return best[0] if best_score > 0 and len(best) == 1 else None


def match_existing_work_author(db, incoming_author, existing_authors,
                               person=None):
    """Match by person ID, unique normalized name, then name+affiliation."""
    if person is not None:
        by_id = [
            author for author in existing_authors
            if author.get("id") == person.get("_id")
        ]
        if len(by_id) == 1:
            return by_id[0]

    incoming_name = normalized_author_name(incoming_author.get("full_name"))
    if not incoming_name:
        return None
    by_name = [
        author for author in existing_authors
        if normalized_author_name(author.get("full_name")) == incoming_name
    ]
    if len(by_name) == 1:
        return by_name[0]
    if not by_name:
        return None

    affiliation_ids = set(
        get_affiliation_ids_by_external_id(db, [incoming_author]).values())
    if not affiliation_ids:
        return None
    scored = []
    for candidate in by_name:
        candidate_ids = {
            affiliation.get("id")
            for affiliation in candidate.get("affiliations", [])
            if affiliation.get("id") is not None
        }
        scored.append((len(affiliation_ids & candidate_ids), candidate))
    best_score = max(score for score, _ in scored)
    best = [candidate for score, candidate in scored if score == best_score]
    return best[0] if best_score > 0 and len(best) == 1 else None


def get_units_affiations(db, author_db, affiliations):
    """
    Method to get the units of an author in a register. ex: faculty, department and group.

    Parameters:
    ----------
    db : pymongo.database.Database
        Database connection to colav database.
    author_db : dict
        record from person
    affiliations : list
        list of affiliations from the parse_openalex method

    Returns:
    -------
    list
        list of units of an author (entries from using affiliations)
    """
    institution_id = None
    # verifiying univeristy
    for j, aff in enumerate(affiliations):
        aff_db = None
        if "external_ids" in aff.keys():
            for ext in aff["external_ids"]:
                aff_db = db["affiliations"].find_one(
                    {"external_ids.id": ext["id"]}, {"_id": 1, "types": 1})
                if aff_db:
                    types = [i["type"] for i in aff_db["types"]]
                    if "group" in types or "department" in types or "faculty" in types:
                        aff_db = None
                        continue
                    else:
                        break
        if aff_db:
            count = db["person"].count_documents(
                {"_id": author_db["_id"], "affiliations.id": aff_db["_id"]})
            if count > 0:
                institution_id = aff_db["_id"]
                break
    units = []
    for aff in author_db["affiliations"]:
        if aff["id"] == institution_id:
            continue
        count = db["affiliations"].count_documents(
            {"_id": aff["id"], "relations.id": institution_id})
        if count > 0:
            types = [i["type"] for i in aff["types"]]
            if "department" in types or "faculty" in types:
                units.append(aff)
    return units


def process_one_update(
        oa_reg,
        colav_reg,
        db,
        collection,
        empty_work,
        verbose=0):
    """
    Method to update a register in the database if it is found in the openalex database.
    This means that the register is already on the database and it is being updated with new information.

    Parameters
    ----------
    oa_reg : dict
        A record from openalex
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    db : pymongo.database.Database
        Database connection to colav database.
    collection : pymongo.collection.Collection
        Collection to insert the register. Colav database collection for works.
    empty_work : dict
        A template for a work entry, with empty fields.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    # updated
    for upd in colav_reg["updated"]:
        if upd["source"] == "openalex":
            return None  # Register already on db
            # Could be updated with new information when openalex database
            # changes
    entry = parse_openalex(oa_reg, empty_work.copy(), verbose=verbose)
    colav_reg["updated"].append(
        {"source": "openalex", "time": int(time())})
    # titles
    colav_reg["titles"].extend(entry["titles"])
    # external_ids
    ext_ids = [ext["id"] for ext in colav_reg["external_ids"]]
    for ext in entry["external_ids"]:
        if ext["id"] not in ext_ids:
            colav_reg["external_ids"].append(ext)
            ext_ids.append(ext["id"])
    # types
    colav_reg["types"].extend(entry["types"])
    # open access info
    colav_reg["open_access"] = entry["open_access"]
    # external urls
    urls_sources = [url["source"]
                    for url in colav_reg["external_urls"]]
    if "open_access" not in urls_sources:
        oa_url = None
        for ext in entry["external_urls"]:
            if ext["source"] == "open_access":
                oa_url = ext["url"]
                break
        if oa_url:
            colav_reg["external_urls"].append(
                {"provenance": "openalex", "source": "open_access", "url": oa_url})
    # citations by year
    if "counts_by_year" in entry.keys():
        colav_reg["citations_by_year"] = entry["counts_by_year"]
    # citations count
    if entry["citations_count"]:
        colav_reg["citations_count"].extend(entry["citations_count"])
    # subjects
    subject_list = []
    for subjects in entry["subjects"]:
        for i, subj in enumerate(subjects["subjects"]):
            for ext in subj["external_ids"]:
                sub_db = db["subjects"].find_one(
                    {"external_ids.id": ext["id"]})
                if sub_db:
                    name = sub_db["names"][0]["name"]
                    for n in sub_db["names"]:
                        if n["lang"] == "en":
                            name = n["name"]
                            break
                        elif n["lang"] == "es":
                            name = n["name"]
                    subject_list.append({
                        "id": sub_db["_id"],
                        "name": name,
                        "level": sub_db["level"]
                    })
                    break
    colav_reg["subjects"].append(
        {"source": "openalex", "subjects": subject_list})

    # authors
    for author in entry["authors"]:
        author_db = resolve_person_by_external_ids(
            db["person"], author.get("external_ids", []))
        target_author = match_existing_work_author(
            db, author, colav_reg["authors"], person=author_db)
        if target_author is None:
            continue
        if author_db is None and target_author.get("id"):
            author_db = db["person"].find_one({"_id": target_author["id"]})
        if author_db is None:
            continue

        aff_units = get_units_affiations(
            db, author_db, author["affiliations"])
        existing_affiliation_ids = {
            affiliation.get("id")
            for affiliation in target_author.get("affiliations", [])
        }
        for aff_unit in aff_units:
            if aff_unit.get("id") not in existing_affiliation_ids:
                target_author.setdefault("affiliations", []).append(aff_unit)
                existing_affiliation_ids.add(aff_unit.get("id"))
    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "types": colav_reg["types"],
            "open_access": colav_reg["open_access"],
            "bibliographic_info": colav_reg["bibliographic_info"],
            "external_urls": colav_reg["external_urls"],
            "subjects": colav_reg["subjects"],
            "citations_count": colav_reg["citations_count"],
            "citations_by_year": colav_reg["citations_by_year"],
            "authors": colav_reg["authors"]
        }}
    )


def run_es_operation(semaphore, operation, *args, **kwargs):
    """Run one Elasticsearch operation within the shared pool limit."""
    if semaphore is None:
        return operation(*args, **kwargs)
    with semaphore:
        return operation(*args, **kwargs)


def index_work_in_elasticsearch(entry, inserted_id, es_handler,
                                es_semaphore=None):
    """Index a newly inserted work when it has a usable title."""
    if not es_handler or not inserted_id or not entry.get("titles"):
        return

    work = {}
    work["title"] = entry["titles"][0]["title"]
    work["source"] = entry["source"]["name"] if "name" in entry["source"].keys() else ""
    work["year"] = (entry["year_published"]
                    if entry.get("year_published") is not None else 0)
    work["volume"] = entry["bibliographic_info"]["volume"] if "volume" in entry["bibliographic_info"].keys() else ""
    work["issue"] = entry["bibliographic_info"]["issue"] if "issue" in entry["bibliographic_info"].keys() else ""
    work["first_page"] = entry["bibliographic_info"]["first_page"] if "first_page" in entry["bibliographic_info"].keys() else ""
    work["last_page"] = entry["bibliographic_info"]["last_page"] if "last_page" in entry["bibliographic_info"].keys() else ""
    authors = []
    for author in entry['authors']:
        if len(authors) >= 5:
            break
        if "full_name" in author.keys():
            authors.append(author["full_name"])
    work["authors"] = authors
    work["provenance"] = "openalex"

    run_es_operation(
        es_semaphore,
        es_handler.insert_work,
        _id=str(inserted_id),
        work=work,
    )


def process_one_insert(oa_reg, db, collection, empty_work, es_handler,
                       verbose=0, es_semaphore=None):
    """
    ""
    Function to insert a new register in the database if it is not found in the colav(kahi works) database.
    This means that the register is not on the database and it is being inserted.

    For similarity purposes, the register is also inserted in the elasticsearch index,
    all the elastic search fields are filled with the information from the register and it is
    handled by Mohan's Similarity class.

    The register is also linked to the source of the register, and the authors and affiliations are searched in the database.

    Parameters
    ----------
    scholar_reg : dict
        Register from the openalex database
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    es_handler : Similarity
        Elasticsearch handler to insert the register in the elasticsearch index, Mohan's Similarity class.
    verbose : int, optional
        Verbosity level. The default is 0
    """

    # parse
    entry = parse_openalex(oa_reg, empty_work.copy(), verbose=verbose)
    # link
    source_db = None
    if entry["source"]:
        if "external_ids" in entry["source"].keys():
            for ext in entry["source"]["external_ids"]:
                source_db = db["sources"].find_one(
                    {"external_ids.id": ext["id"]})
                if source_db:
                    break
    if source_db:
        name = source_db["names"][0]["name"]
        for n in source_db["names"]:
            if n["lang"] == "es":
                name = n["name"]
                break
            if n["lang"] == "en":
                name = n["name"]
        entry["source"] = {
            "id": source_db["_id"],
            "name": name
        }
    else:
        if entry["source"]:
            if len(entry["source"]["external_ids"]) == 0:
                print(
                    f'Register with doi: {
                        oa_reg["doi"]} does not provide a source')
            else:
                print("No source found for\n\t",
                      entry["source"]["external_ids"])
            entry["source"] = {
                "id": "",
                "name": entry["source"]["name"]
            }
    for subjects in entry["subjects"]:
        for i, subj in enumerate(subjects["subjects"]):
            for ext in subj["external_ids"]:
                sub_db = db["subjects"].find_one(
                    {"external_ids.id": ext["id"]})
                if sub_db:
                    name = sub_db["names"][0]["name"]
                    for n in sub_db["names"]:
                        if n["lang"] == "en":
                            name = n["name"]
                            break
                        elif n["lang"] == "es":
                            name = n["name"]
                    entry["subjects"][0]["subjects"][i] = {
                        "id": sub_db["_id"],
                        "name": name,
                        "level": sub_db["level"]
                    }
                    break

    # Batch the expensive exact-name fallback for authors without identifiers.
    fallback_names = [
        author["full_name"]
        for author in entry["authors"]
        if not author["external_ids"] and author["full_name"]
    ]
    name_candidates = get_author_name_candidates(
        db["person"], fallback_names)
    ambiguous_authors = [
        author
        for author in entry["authors"]
        if len(name_candidates.get(author["full_name"], [])) > 1
    ]
    affiliation_ids = get_affiliation_ids_by_external_id(
        db, ambiguous_authors)

    # search authors and affiliations in db
    for i, author in enumerate(entry["authors"]):
        author_db = None
        for ext in author["external_ids"]:  # given priority to scienti person
            author_db = db["person"].find_one(
                {"external_ids.id": ext["id"], "updated.source": "scienti"})
            if author_db:
                break
        if not author_db:  # if not found ids with scienti, let search it with openalex
            for ext in author["external_ids"]:
                author_db = db["person"].find_one(
                    {"external_ids.id": ext["id"], "updated.source": "openalex"})
                if author_db:
                    break
        if not author_db:  # if not found ids with scienti/openalex, let search it with other sources
            for ext in author["external_ids"]:
                author_db = db["person"].find_one(
                    {"external_ids.id": ext["id"]})
                if author_db:
                    break
        if author_db:
            sources = [ext["source"]
                       for ext in author_db["external_ids"]]
            ids = [ext["id"] for ext in author_db["external_ids"]]
            for ext in author["external_ids"]:
                if ext["id"] not in ids:
                    author_db["external_ids"].append(ext)
                    sources.append(ext["source"])
                    ids.append(ext["id"])
            entry["authors"][i] = {
                "id": author_db["_id"],
                "full_name": author_db["full_name"],
                "affiliations": author["affiliations"]
            }
            aff_units = get_units_affiations(
                db, author_db, author["affiliations"])
            for aff_unit in aff_units:
                if aff_unit not in author["affiliations"]:
                    author["affiliations"].append(aff_unit)

            if "external_ids" in author.keys():
                del (author["external_ids"])
        else:
            if verbose > 1:
                print(
                    f"WARNING: author found not in db {author} maybe deleted author in openalex, trying to find by name")
            candidates = name_candidates.get(author["full_name"])
            if candidates is None:
                candidates = get_author_name_candidates(
                    db["person"], [author["full_name"]]
                ).get(author["full_name"], [])
                if len(candidates) > 1:
                    affiliation_ids.update(
                        get_affiliation_ids_by_external_id(db, [author]))
            author_db = select_name_candidate(
                author, candidates, affiliation_ids)
            if author_db:
                sources = [ext["source"]
                           for ext in author_db["external_ids"]]
                ids = [ext["id"] for ext in author_db["external_ids"]]
                for ext in author["external_ids"]:
                    if ext["id"] not in ids:
                        author_db["external_ids"].append(ext)
                        sources.append(ext["source"])
                        ids.append(ext["id"])
                entry["authors"][i] = {
                    "id": author_db["_id"],
                    "full_name": author_db["full_name"],
                    "affiliations": author["affiliations"]
                }
                aff_units = get_units_affiations(
                    db, author_db, author["affiliations"])
                for aff_unit in aff_units:
                    if aff_unit not in author["affiliations"]:
                        author["affiliations"].append(aff_unit)

            else:
                entry["authors"][i] = {
                    "id": "",
                    "full_name": author["full_name"],
                    "affiliations": author["affiliations"]
                }
        for j, aff in enumerate(author["affiliations"]):
            aff_db = None
            if "types" in aff.keys():  # if not types it not group, department or faculty
                types = [i["type"] for i in aff["types"]]
                if "group" in types or "department" in types or "faculty" in types:
                    continue
            if "external_ids" in aff.keys():
                for ext in aff["external_ids"]:
                    aff_db = db["affiliations"].find_one(
                        {"external_ids.id": ext["id"]})
                    if aff_db:
                        break
            if aff_db:
                name = aff_db["names"][0]["name"]
                for n in aff_db["names"]:
                    if n["source"] == "ror":
                        name = n["name"]
                        break
                    if n["lang"] == "en":
                        name = n["name"]
                    if n["lang"] == "es":
                        name = n["name"]
                entry["authors"][i]["affiliations"][j] = {
                    "id": aff_db["_id"],
                    "name": name,
                    "types": aff_db["types"]
                }
            else:
                aff_db = db["affiliations"].find_one(
                    {"names.name": aff["name"]})
                if aff_db:
                    name = aff_db["names"][0]["name"]
                    for n in aff_db["names"]:
                        if n["source"] == "ror":
                            name = n["name"]
                            break
                        if n["lang"] == "en":
                            name = n["name"]
                        if n["lang"] == "es":
                            name = n["name"]
                    entry["authors"][i]["affiliations"][j] = {
                        "id": aff_db["_id"],
                        "name": name,
                        "types": aff_db["types"]
                    }
                else:
                    entry["authors"][i]["affiliations"][j] = {
                        "id": "",
                        "name": aff["name"],
                        "types": []
                    }

    entry["author_count"] = len(entry["authors"])
    # insert in mongo
    inserted_id = None
    openalex_id = oa_reg["id"] if "id" in oa_reg.keys() else None
    if openalex_id:
        response = collection.update_one(
            {"external_ids": {"$elemMatch": {"source": "openalex", "id": openalex_id}}},
            {"$setOnInsert": entry},
            upsert=True
        )
        inserted_id = response.upserted_id
    else:
        response = collection.insert_one(entry)
        inserted_id = response.inserted_id
    index_work_in_elasticsearch(
        entry, inserted_id, es_handler, es_semaphore)


def process_one(oa_reg, config, empty_work, client, es_handler, backend,
                verbose=0, es_semaphore=None):
    """
    Function to process a single register from the scholar database.
    This function is used to insert or update a register in the colav(kahi works) database.

    Parameters
    ----------
    oa_reg : dict
        Register from the openalex database
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    es_handler : Similarity
        Elasticsearch handler to insert the register in the elasticsearch index, Mohan's Similarity class.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    if backend != "threading":
        client = get_process_mongo_client(config["database_url"])
    db = client[config["database_name"]]
    collection = db["works"]

    if backend != "threading":
        es_handler = None
        if "es_index" in config["openalex_works"].keys() and "es_url" in config["openalex_works"].keys(
        ) and "es_user" in config["openalex_works"].keys() and "es_password" in config["openalex_works"].keys():
            es_index = config["openalex_works"]["es_index"]
            es_url = config["openalex_works"]["es_url"]
            if config["openalex_works"]["es_user"] and config["openalex_works"]["es_password"]:
                es_auth = (config["openalex_works"]["es_user"],
                           config["openalex_works"]["es_password"])
            else:
                es_auth = None
            es_handler = Similarity(
                es_index,
                es_uri=es_url,
                es_auth=es_auth,
                es_req_timeout=300,
                es_max_retries=5,
                es_retry_on_timeout=True)
        else:
            es_handler = None

    colav_reg = None
    openalex_id = oa_reg["id"] if "id" in oa_reg.keys() else None
    if openalex_id:
        colav_reg = collection.find_one(
            {"external_ids": {"$elemMatch": {"source": "openalex", "id": openalex_id}}}
        )

    doi = oa_reg["doi"]
    if colav_reg:
        process_one_update(
            oa_reg, colav_reg, db, collection, empty_work, verbose=verbose)
    elif doi:
        # fallback: is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})
        if colav_reg:  # update the register
            process_one_update(
                oa_reg, colav_reg, db, collection, empty_work, verbose=verbose)
        else:  # insert a new register
            process_one_insert(
                oa_reg, db, collection, empty_work, es_handler,
                verbose=verbose, es_semaphore=es_semaphore)
    else:  # does not have a doi identifier
        # elasticsearch section
        if es_handler:
            # Search in elasticsearch
            authors = []
            for author in oa_reg['authorships']:
                if "display_name" in author["author"].keys():
                    authors.append(author["author"]["display_name"])
            source = ""
            if oa_reg["primary_location"]:
                if "source" in oa_reg["primary_location"].keys():
                    if oa_reg["primary_location"]["source"]:
                        if "display_name" in oa_reg["primary_location"]["source"].keys(
                        ):
                            source = oa_reg["primary_location"]["source"]["display_name"]
            response = run_es_operation(
                es_semaphore,
                es_handler.search_work,
                title=oa_reg["title"],
                source=source,
                year=(str(oa_reg["publication_year"])
                      if oa_reg.get("publication_year") is not None else "0"),
                authors=authors,
                volume=oa_reg["biblio"]["volume"],
                issue=oa_reg["biblio"]["issue"],
                page_start=oa_reg["biblio"]["first_page"],
                page_end=oa_reg["biblio"]["last_page"],
            )

            if response:  # register already on db... update accordingly
                colav_reg = collection.find_one(
                    {"_id": ObjectId(response["_id"])})
                if colav_reg:
                    process_one_update(oa_reg, colav_reg, db,
                                       collection, empty_work, verbose=verbose)
                else:
                    if verbose > 4:
                        print("Register with {} not found in mongodb".format(
                            response["_id"]))
                        print(response)
                    process_one_insert(oa_reg, db, collection,
                                       empty_work, es_handler, verbose=0,
                                       es_semaphore=es_semaphore)

            else:  # insert new register
                if verbose > 4:
                    print("INFO: found no register in elasticsearch")
                process_one_insert(oa_reg, db, collection,
                                   empty_work, es_handler, verbose=0,
                                   es_semaphore=es_semaphore)
        else:
            if verbose > 4:
                print("No elasticsearch index provided")
    if backend != "threading" and es_handler:
        es_handler.close()
