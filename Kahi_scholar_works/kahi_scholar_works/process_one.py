from kahi_impactu_utils.Utils import doi_processor
from time import time
from kahi_scholar_works.parser import parse_scholar
from bson import ObjectId
import unicodedata
from threading import Lock


WORK_UPDATE_PROJECTION = {
    "updated": 1,
    "titles": 1,
    "abstracts": 1,
    "external_ids": 1,
    "types": 1,
    "bibliographic_info": 1,
    "external_urls": 1,
    "citations_count": 1,
}

_DOI_LOCKS = tuple(Lock() for _ in range(1024))
_CID_LOCKS = tuple(Lock() for _ in range(1024))


def _normalized_title(title):
    if not isinstance(title, str):
        return ""
    return " ".join(unicodedata.normalize("NFKC", title).casefold().split())


def _append_missing(target, values):
    for value in values:
        if value not in target:
            target.append(value)


def _doi_lock(doi):
    return _DOI_LOCKS[hash(doi) % len(_DOI_LOCKS)]


def _cid_lock(cid):
    return _CID_LOCKS[hash(cid) % len(_CID_LOCKS)]


def run_es_operation(semaphore, operation, *args, **kwargs):
    if semaphore is None:
        return operation(*args, **kwargs)
    with semaphore:
        return operation(*args, **kwargs)


def scholar_cid(entry):
    return next((
        ext.get("id") for ext in entry.get("external_ids", [])
        if ext.get("source") == "scholar" and ext.get("id")
    ), None)


def scholar_cid_query(cid):
    return {"external_ids": {"$elemMatch": {
        "source": "scholar", "id": cid}}}


def build_es_work(entry):
    if not entry.get("titles") or not entry["titles"][0].get("title", "").strip():
        return None
    bibliographic_info = entry.get("bibliographic_info", {})
    return {
        "title": entry["titles"][0]["title"],
        "source": entry.get("source", {}).get("name", ""),
        "year": entry.get("year_published") or 0,
        "volume": bibliographic_info.get("volume", ""),
        "issue": bibliographic_info.get("issue", ""),
        "first_page": bibliographic_info.get("start_page", ""),
        "last_page": bibliographic_info.get("end_page", ""),
        "authors": [
            author["full_name"]
            for author in entry.get("authors", [])[:5]
            if author.get("full_name")
        ],
        "provenance": "scholar",
    }


def ensure_es_work(entry, work_id, es_handler, es_semaphore=None):
    if not es_handler or work_id is None:
        return
    document = build_es_work(entry)
    if document is None:
        return
    exists = run_es_operation(
        es_semaphore, es_handler.es.exists,
        index=es_handler.es_index, id=str(work_id))
    if not exists:
        run_es_operation(
            es_semaphore, es_handler.insert_work,
            _id=str(work_id), work=document)


def process_one_update(scholar_reg, colav_reg, collection, empty_work, verbose=0):
    """
    Method to update a register in the database if it is found in the scholar database.
    This means that the register is already on the database and it is being updated with new information.


    Parameters
    ----------
    scholar_reg : dict
        Register from the scholar database
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    updated = colav_reg.setdefault("updated", [])
    abstracts = colav_reg.setdefault("abstracts", [])
    already_updated = any(upd.get("source") == "scholar" for upd in updated)
    needs_abstract = not abstracts and bool(scholar_reg.get("abstract"))

    if already_updated:
        if not needs_abstract:
            return None
        entry = parse_scholar(
            scholar_reg,
            empty_work.copy(),
            verbose=verbose,
            include_title=False,
            include_abstract=True,
            include_authors=False,
        )
        _append_missing(abstracts, entry["abstracts"])
        collection.update_one(
            {"_id": colav_reg["_id"]}, {"$set": {"abstracts": abstracts}}
        )
        return None

    existing_titles = {
        _normalized_title(title.get("title"))
        for title in colav_reg.setdefault("titles", [])
    }
    include_title = _normalized_title(scholar_reg.get("title")) not in existing_titles
    entry = parse_scholar(
        scholar_reg,
        empty_work.copy(),
        verbose=verbose,
        include_title=include_title,
        include_abstract=needs_abstract,
        include_authors=False,
    )
    updated.append({"source": "scholar", "time": int(time())})
    # titles
    _append_missing(colav_reg["titles"], entry["titles"])
    # abstracts are added only when the work does not have one
    _append_missing(abstracts, entry["abstracts"])
    # external_ids
    external_ids = colav_reg.setdefault("external_ids", [])
    ext_ids = [ext.get("id") for ext in external_ids]
    for ext in entry["external_ids"]:
        if ext.get("id") not in ext_ids:
            external_ids.append(ext)
            ext_ids.append(ext.get("id"))
    # types
    types = colav_reg.setdefault("types", [])
    _append_missing(types, entry["types"])
    # bibliographic info
    bibliographic_info = colav_reg.setdefault("bibliographic_info", {})
    if "start_page" not in colav_reg["bibliographic_info"].keys():
        if "start_page" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["start_page"] = entry["bibliographic_info"][
                "start_page"
            ]
    if "end_page" not in colav_reg["bibliographic_info"].keys():
        if "end_page" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["end_page"] = entry["bibliographic_info"][
                "end_page"
            ]
    if "volume" not in colav_reg["bibliographic_info"].keys():
        if "volume" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["volume"] = entry["bibliographic_info"][
                "volume"
            ]
    if "issue" not in colav_reg["bibliographic_info"].keys():
        if "issue" in entry["bibliographic_info"].keys():
            colav_reg["bibliographic_info"]["issue"] = entry["bibliographic_info"][
                "issue"
            ]
    # bibtex
    if "bibtex" in entry["bibliographic_info"].keys():
        colav_reg["bibliographic_info"]["bibtex"] = entry["bibliographic_info"][
            "bibtex"
        ]

    # external urls
    external_urls = colav_reg.setdefault("external_urls", [])
    existing_urls = {url.get("url") for url in external_urls}
    for ext in entry["external_urls"]:
        if ext.get("url") not in existing_urls:
            external_urls.append(ext)
            existing_urls.add(ext.get("url"))

    # citations count
    citations_count = colav_reg.setdefault("citations_count", [])
    _append_missing(citations_count, entry["citations_count"])

    collection.update_one(
        {"_id": colav_reg["_id"]},
        {
            "$set": {
                "updated": updated,
                "titles": colav_reg["titles"],
                "abstracts": abstracts,
                "external_ids": external_ids,
                "types": types,
                "bibliographic_info": bibliographic_info,
                "external_urls": external_urls,
                "citations_count": citations_count,
            }
        },
    )


def process_one_insert(scholar_reg, db, collection, empty_work, es_handler,
                       verbose=0, es_semaphore=None):
    """
    Function to insert a new register in the database if it is not found in the colav(kahi works) database.
    This means that the register is not on the database and it is being inserted.

    For similarity purposes, the register is also inserted in the elasticsearch index,
    all the elastic search fields are filled with the information from the register and it is
    handled by Mohan's Similarity class.

    The register is also linked to the source of the register, and the authors and affiliations are searched in the database.

    Parameters
    ----------
    scholar_reg : dict
        Register from the scholar database
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
    entry = parse_scholar(scholar_reg, empty_work.copy(), verbose=verbose)
    if not entry.get("titles") or not entry["titles"][0].get("title", "").strip():
        return
    # link
    source_db = None
    if "external_ids" in entry["source"].keys():
        for ext in entry["source"]["external_ids"]:
            source_db = db["sources"].find_one({"external_ids.id": ext["id"]})
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
        entry["source"] = {"id": source_db["_id"], "name": name}
    else:
        if len(entry["source"]["external_ids"]) == 0:
            if verbose > 4:
                print(
                    f"Register with doi: {scholar_reg['doi']} does not provide a source"
                )
        else:
            if verbose > 4:
                print("No source found for\n\t", entry["source"]["external_ids"])
        entry["source"] = {"id": "", "name": entry["source"]["name"]}

    # search authors and affiliations in db
    for i, author in enumerate(entry["authors"]):
        author_db = None
        for ext in author["external_ids"]:
            author_db = db["person"].find_one({"external_ids.id": ext["id"]})
            if author_db:
                break
        if author_db:
            sources = [ext["source"] for ext in author_db["external_ids"]]
            ids = [ext["id"] for ext in author_db["external_ids"]]
            for ext in author["external_ids"]:
                if ext["id"] not in ids:
                    author_db["external_ids"].append(ext)
                    sources.append(ext["source"])
                    ids.append(ext["id"])
            entry["authors"][i] = {
                "id": author_db["_id"],
                "full_name": author_db["full_name"],
                "affiliations": author["affiliations"],
            }
            if "external_ids" in author.keys():
                del author["external_ids"]
        else:
            author_db = db["person"].find_one({"full_name": author["full_name"]})
            if author_db:
                sources = [ext["source"] for ext in author_db["external_ids"]]
                ids = [ext["id"] for ext in author_db["external_ids"]]
                for ext in author["external_ids"]:
                    if ext["id"] not in ids:
                        author_db["external_ids"].append(ext)
                        sources.append(ext["source"])
                        ids.append(ext["id"])
                entry["authors"][i] = {
                    "id": author_db["_id"],
                    "full_name": author_db["full_name"],
                    "affiliations": author["affiliations"],
                }
            else:
                entry["authors"][i] = {
                    "id": "",
                    "full_name": author["full_name"],
                    "affiliations": author["affiliations"],
                }
        for j, aff in enumerate(author["affiliations"]):
            aff_db = None
            if "external_ids" in aff.keys():
                for ext in aff["external_ids"]:
                    aff_db = db["affiliations"].find_one({"external_ids.id": ext["id"]})
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
                    "types": aff_db["types"],
                }
            else:
                aff_db = db["affiliations"].find_one({"names.name": aff["name"]})
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
                        "types": aff_db["types"],
                    }
                else:
                    entry["authors"][i]["affiliations"][j] = {
                        "id": "",
                        "name": aff["name"],
                        "types": [],
                    }

    entry["author_count"] = len(entry["authors"])
    cid = scholar_cid(entry)
    if cid:
        with _cid_lock(cid):
            response = collection.update_one(
                scholar_cid_query(cid),
                {"$setOnInsert": entry},
                upsert=True)
            inserted_id = response.upserted_id
        if inserted_id is None:
            existing = collection.find_one(scholar_cid_query(cid), {"_id": 1})
            inserted_id = existing.get("_id") if existing else None
    else:
        response = collection.insert_one(entry)
        inserted_id = response.inserted_id
    ensure_es_work(entry, inserted_id, es_handler, es_semaphore)


def process_one(
    scholar_reg, db, collection, empty_work, similarity, es_handler,
    verbose=0, es_semaphore=None
):
    """
    Function to process a single register from the scholar database.
    This function is used to insert or update a register in the colav(kahi works) database.

    Parameters
    ----------
    scholar_reg : dict
        Register from the scholar database
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
    title = scholar_reg.get("title")
    if not isinstance(title, str) or not title.strip():
        return

    doi = None
    # register has doi
    if scholar_reg.get("doi"):
        if isinstance(scholar_reg["doi"], str):
            doi = doi_processor(scholar_reg["doi"])
    if doi:
        # Records with the same DOI can be scheduled concurrently. Serialize
        # each DOI so that the lookup/insert pair cannot create duplicates.
        with _doi_lock(doi):
            colav_reg = collection.find_one(
                {"external_ids.id": doi}, WORK_UPDATE_PROJECTION
            )
            if colav_reg:  # update
                process_one_update(
                    scholar_reg, colav_reg, collection, empty_work, verbose=verbose
                )
            else:  # insert a new register
                process_one_insert(
                    scholar_reg,
                    db,
                    collection,
                    empty_work,
                    es_handler,
                    verbose=verbose,
                    es_semaphore=es_semaphore,
                )
    elif similarity:
        if es_handler:
            entry = parse_scholar(
                scholar_reg, empty_work.copy(), verbose=verbose)
            cid = scholar_cid(entry)
            existing = (collection.find_one(
                scholar_cid_query(cid), {"_id": 1}) if cid else None)
            if existing:
                ensure_es_work(entry, existing["_id"], es_handler, es_semaphore)
                return
            work = build_es_work(entry)
            response = run_es_operation(
                es_semaphore, es_handler.search_work,
                title=work["title"],
                source=work["source"],
                year=str(work["year"]),
                authors=work["authors"],
                volume=work["volume"],
                issue=work["issue"],
                page_start=work["first_page"],
                page_end=work["last_page"],
            )

            if response:  # register already on db... update accordingly
                colav_reg = collection.find_one(
                    {"_id": ObjectId(response["_id"])}, WORK_UPDATE_PROJECTION
                )
                if colav_reg:
                    process_one_update(
                        scholar_reg, colav_reg, collection, empty_work, verbose=0
                    )
                else:
                    if verbose > 4:
                        print(
                            "Register with {} not found in mongodb".format(
                                response["_id"]
                            )
                        )
                        print(response)
            else:  # insert new register
                if verbose > 4:
                    print("INFO: found no register in elasticsearch")
                process_one_insert(
                    scholar_reg, db, collection, empty_work, es_handler,
                    verbose=0, es_semaphore=es_semaphore,
                )
        else:
            if verbose > 4:
                print("No elasticsearch index provided")
