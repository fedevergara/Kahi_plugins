from kahi_scienti_works.parser import parse_scienti
from kahi_impactu_utils.Utils import lang_poll, doi_processor, compare_author, split_names, split_names_fix, check_date_format
import re
from time import time
from bson import ObjectId
from threading import Lock


_INSERT_LOCKS = tuple(Lock() for _ in range(256))


def run_es_operation(semaphore, operation, *args, **kwargs):
    """Run one Elasticsearch operation within the shared pool limit."""
    if semaphore is None:
        return operation(*args, **kwargs)
    with semaphore:
        return operation(*args, **kwargs)


def scienti_identity(entry):
    """Return the stable COD_RH/COD_PRODUCTO identity for one work."""
    return next((
        ext for ext in entry.get("external_ids", [])
        if ext.get("source") == "scienti"
        and isinstance(ext.get("id"), dict)
        and ext["id"].get("COD_RH") is not None
        and ext["id"].get("COD_PRODUCTO") is not None
    ), None)


def scienti_retry_query(identity, title):
    """Identify the same source record without merging reused Scienti IDs."""
    return {
        "external_ids": {"$elemMatch": {
            "source": "scienti",
            "id.COD_RH": identity["id"]["COD_RH"],
            "id.COD_PRODUCTO": identity["id"]["COD_PRODUCTO"],
        }},
        "titles": {"$elemMatch": {
            "source": "scienti",
            "title": title,
        }},
    }


def scienti_insert_lock(identity, title):
    """Serialize only potentially identical upserts within the thread pool."""
    key = (
        str(identity["id"]["COD_RH"]),
        str(identity["id"]["COD_PRODUCTO"]),
        title,
    )
    return _INSERT_LOCKS[hash(key) % len(_INSERT_LOCKS)]


def build_es_work(entry):
    """Build a valid Elasticsearch document from a parsed Scienti work."""
    if not entry.get("titles") or not entry["titles"][0].get(
            "title", "").strip():
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
        "provenance": "scienti",
    }


def ensure_es_work(entry, work_id, es_handler, es_semaphore=None):
    """Insert a work in Elasticsearch only when its MongoDB ID is absent."""
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


def cod_product_mismatch(list1, list2):
    """
    check if the COD_PRODUCTO of the registers are different for the same COD_RH

    Parameters
    ----------
    list1 : list
        List of external_ids from the first register
    list2 : list
        List of external_ids from the second register

    Returns
    -------
    bool
        True if the COD_PRODUCTO are different, False otherwise.
    """
    # Extract COD_RH and their respective COD_PRODUCTO in list1
    rh_to_product1 = {
        entry["id"]["COD_RH"]: entry["id"]["COD_PRODUCTO"] for entry in list1 if isinstance(
            entry["id"],
            dict) and "COD_RH" in entry["id"] and "COD_PRODUCTO" in entry["id"]}

    # Extract COD_RH and their respective COD_PRODUCTO in list2
    rh_to_product2 = {
        entry["id"]["COD_RH"]: entry["id"]["COD_PRODUCTO"] for entry in list2 if isinstance(
            entry["id"],
            dict) and "COD_RH" in entry["id"] and "COD_PRODUCTO" in entry["id"]}

    # Identify the COD_RH that exist in both lists but have different
    # COD_PRODUCTO
    mismatched_cod_rh = {
        rh: (rh_to_product1[rh], rh_to_product2[rh])
        # Only COD_RH that exist in both lists
        for rh in rh_to_product1.keys() & rh_to_product2.keys()
        if rh_to_product1[rh] != rh_to_product2[rh]  # Different COD_PRODUCTO
    }
    return False if len(list(mismatched_cod_rh)) == 0 else True


def has_scienti_source(lst):
    """
    Checks if scienti in field source is available

    Parameters
    ----------
    lst : list
        List of types/external_ids from register

    Returns
    -------
    bool
        True if the scienti type is available, False otherwise.
    """
    return any(item.get("source") == "scienti" for item in lst)


def check_first_level_type(list1, list2):
    """
    Function to check if the first level types of two registers are different.

    Parameters
    ----------
    list1 : list
        List of types from the first register
    list2 : list
        List of types from the second register

    Returns
    -------
    bool
        True if the first level types are equal, False otherwise.
    """
    # Extraer los tipos donde source='scienti' y level=0
    types1 = {item["type"] for item in list1 if item.get(
        "source") == "scienti" and item.get("level") == 0}
    types2 = {item["type"] for item in list2 if item.get(
        "source") == "scienti" and item.get("level") == 0}

    # Comparar conjuntos de tipos
    return types1 == types2


def get_doi(reg):
    """
    Method to get the doi of a register.

    Parameters:
    ----------
    reg : dict
        Register from the scienti database

    Returns:
    -------
    str
        doi of the register, False if not found.
    """
    for i in reg["external_ids"]:
        if i["source"] == 'doi':
            return i["id"]
    return False


def get_units_affiliations(db, author_db, affiliations):
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


def process_author(entry, colav_reg, db, verbose=0):
    """
    Function to compare the authors of a register from the scienti database with the authors of a register from the colav database.
    If the authors match, the author from the colav register is replaced with the author from the scienti register.

    Parameters
    ----------
    entry : dict
        Register from the scienti database
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    db : pymongo.collection.Collection
        Database where ETL result is stored
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    scienti_author = entry['authors'][0]
    if scienti_author:
        author_db = None
        if 'external_ids' in scienti_author.keys():
            author_ids = scienti_author['external_ids']
            author_db = db['person'].find_one({'external_ids': {'$elemMatch': {'$or': author_ids}}}, {
                "_id": 1, "full_name": 1, "affiliations": 1, "first_names": 1, "last_names": 1, "initials": 1, "external_ids": 1})

        if author_db:
            name_match = False
            affiliation_match = False
            for i, author in enumerate(colav_reg['authors']):
                if author['id'] == author_db['_id']:
                    # adding the group for the author
                    groups = []
                    for aff in scienti_author["affiliations"]:
                        if aff["types"]:
                            for t in aff["types"]:
                                if t["type"] == "group":
                                    groups.append(aff)
                    for group in groups:
                        if group not in author["affiliations"]:
                            author["affiliations"].append(group)
                    colav_reg["authors"][i] = {
                        "id": author_db["_id"],
                        "full_name": author_db["full_name"],
                        "affiliations": author["affiliations"]
                    }
                    continue
                author_reg = None
                if author['id'] == "":
                    if verbose >= 4:
                        print(
                            f"WARNING: author with id '' found in colav register: {author} using split_names")
                    author_reg = split_names(author["full_name"])
                else:
                    author_reg = db['person'].find_one(
                        # this is required to get  first_names and last_names
                        {'_id': author['id']}, {"_id": 1, "full_name": 1, "first_names": 1, "last_names": 1, "initials": 1, "external_ids": 1})
                    if author_reg is None:
                        print(
                            f"ERROR: author with id {author['id']} found not in colav database")
                        continue

                # Note: even in openalex names are bad splitted, so we need to fix them
                # ex: 'full_name': 'Claudia Marcela-Vélez', 'first_names': ['Claudia'], 'last_names': ['Marcela', 'Vélez']  where Marcela is the first name and Vélez is the last name
                # then we need to compare the names after fixing them.
                author_reg_fix = split_names_fix(author_reg, author_db)
                if author_reg_fix:
                    author_reg["full_name"] = author_reg_fix["full_name"]
                    author_reg["first_names"] = author_reg_fix["first_names"]
                    author_reg["last_names"] = author_reg_fix["last_names"]
                    author_reg["initials"] = author_reg_fix["initials"]

                name_match = compare_author(
                    author_reg, author_db, len(colav_reg['authors']))

                doi1 = get_doi(entry)
                doi2 = get_doi(colav_reg)
                if doi1 and doi2:
                    if doi1 == doi2:
                        affiliation_match = True
                else:
                    if author['affiliations']:
                        affiliations_person = [str(aff['id'])
                                               for aff in author_db['affiliations']]
                        author_affiliations = [str(aff['id'])
                                               for aff in author['affiliations']]
                        affiliation_match = any(
                            affil in author_affiliations for affil in affiliations_person)
                # if name and affiliation match, we can replace the author
                if name_match and affiliation_match:
                    # replace the author, maybe add the openalex id to the
                    # record in the future
                    for reg in author_db["affiliations"]:
                        reg.pop('start_date', None)
                        reg.pop('end_date', None)
                    # adding the group for the author
                    groups = []
                    for aff in scienti_author["affiliations"]:
                        if aff["types"]:
                            for t in aff["types"]:
                                if t["type"] == "group":
                                    groups.append(aff)
                    for group in groups:
                        if group not in author["affiliations"]:
                            author["affiliations"].append(group)

                    aff_units = get_units_affiliations(
                        db, author_db, author["affiliations"])
                    for aff_unit in aff_units:
                        if aff_unit not in author["affiliations"]:
                            author["affiliations"].append(aff_unit)

                    colav_reg["authors"][i] = {
                        "id": author_db["_id"],
                        "full_name": author_db["full_name"],
                        "affiliations": author["affiliations"]
                    }
                    break

                else:
                    # if name match but not affiliation, add the author
                    if name_match and not affiliation_match:
                        colav_reg["authors"].append({
                            "id": author_db["_id"],
                            "full_name": author_db["full_name"],
                            "affiliations": scienti_author["affiliations"]
                        })
                        break


def process_one_update(
        scienti_reg,
        colav_reg,
        db,
        collection,
        empty_work,
        verbose=0):
    """
    Method to update a register in the kahi database from scholar database if it is found.
    This means that the register is already on the kahi database and it is being updated with new information.


    Parameters
    ----------
    scienti_reg : dict
        Register from the scienti database
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    db: pymongo.collection.Collection
        Database where ETL result is stored
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    entry = parse_scienti(
        scienti_reg, empty_work.copy(), verbose=verbose)
    colav_reg["updated"].append(
        {"source": "scienti", "time": int(time())})

    # updated author affiliations
    author = entry["authors"][0]
    for j, aff in enumerate(author["affiliations"]):
        aff_db = None
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
            author["affiliations"][j] = {
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
                author["affiliations"][j] = {
                    "id": aff_db["_id"],
                    "name": name,
                    "types": aff_db["types"]
                }
            else:
                author["affiliations"][j] = {
                    "id": "",
                    "name": aff["name"],
                    "types": []
                }

    # titles
    if 'scienti' not in [title['source'] for title in colav_reg["titles"]]:
        lang = lang_poll(entry["titles"][0]["title"])
        rec = {"title": entry["titles"][0]["title"],
               "lang": lang, "source": "scienti"}
        if rec not in colav_reg["titles"]:
            colav_reg["titles"].append(rec)
    # external_ids
    for ext in entry["external_ids"]:
        if ext not in colav_reg["external_ids"]:
            colav_reg["external_ids"].append(ext)
    # types
    for rec in entry["types"]:
        if rec not in colav_reg["types"]:
            colav_reg["types"].append(rec)

    # external urls
    url_sources = [url["source"]
                   for url in colav_reg["external_urls"]]
    for ext in entry["external_urls"]:
        if ext["source"] not in url_sources:
            colav_reg["external_urls"].append(ext)
            url_sources.append(ext["source"])

    # scienti author
    process_author(entry, colav_reg, db, verbose)

    # the first author is the original one always (already inserted)
    # there is a pair (author_others, author) see schema (but it is a list)
    if "re_author_others" in scienti_reg.keys():
        for i, author in enumerate(scienti_reg["re_author_others"][1:]):
            if "author_others" not in author.keys():
                continue
            # for every record in re_author_others there is a record for
            # author_others/author see schema
            author = author["author_others"][0]
            if author["COD_RH_REF"]:
                author_db = db["person"].find_one({"external_ids.id.COD_RH": author["COD_RH_REF"]}, {
                    "_id": 1, "full_name": 1, "affiliations": 1, "first_names": 1, "last_names": 1, "initials": 1, "external_ids": 1})
                if author_db:
                    found = False
                    for author_rec in colav_reg["authors"]:
                        if author_db["_id"] == author_rec["id"]:
                            found = True
                        else:
                            if author_rec['id'] == "":
                                continue
                            # only the name can be compared, because we dont
                            # have the affiliation of the author from the paper
                            # in author_others
                            author_reg = db['person'].find_one(
                                # this is required to get  first_names and
                                # last_names
                                {'_id': author_rec['id']}, {"_id": 1, "full_name": 1, "first_names": 1, "last_names": 1, "initials": 1})

                            # author_reg is only needed here
                            name_match = compare_author(
                                author_reg, author_db, len(
                                    scienti_reg["re_author_others"]))
                            if name_match:
                                found = True
                                author_rec["id"] = author_db["_id"]
                                author_rec["full_name"] = author_db['full_name']

                    if not found:
                        rec = {"id": author_db["_id"],
                               "full_name": author_db["full_name"],
                               # we dont have affiliation of the author from
                               # the paper, we can´t assume one.
                               "affiliations": []
                               }
                        colav_reg["authors"].append(rec)

    # scienti groups
    if "group" in scienti_reg.keys():
        for group in scienti_reg["group"]:
            group_reg = db["affiliations"].find_one(
                {"external_ids.id": group["COD_ID_GRUPO"]})
            if group_reg is None:
                group_reg = db["affiliations"].find_one(
                    {"external_ids.id": group["NRO_ID_GRUPO"]})
            if group_reg:
                found = False
                for rgroup in colav_reg["groups"]:
                    if group_reg["_id"] == rgroup["id"]:
                        found = True
                        break
                if not found:
                    colav_reg["groups"].append(
                        {"id": group_reg["_id"], "name": group_reg["names"][0]["name"]})
            if not group_reg:
                print(
                    f'WARNING: group with ids {group.get("COD_ID_GRUPO")} and '
                    f'{group.get("NRO_ID_GRUPO")} found not in affiliation')

    colav_reg["author_count"] = len(colav_reg["authors"])

    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "types": colav_reg["types"],
            # this is not in the scienti register, but it is in the colav register, so it is not updated, should it be updated with scienti register?
            # scienti provides poor quality data, so it is better to keep the data from colav
            "bibliographic_info": colav_reg["bibliographic_info"],
            "external_urls": colav_reg["external_urls"],
            "author_count": colav_reg["author_count"],
            "authors": colav_reg["authors"],
            "subjects": colav_reg["subjects"],
            "groups": colav_reg["groups"]
        }}
    )


def process_one_insert(scienti_reg, db, collection, empty_work, es_handler,
                       doi=None, verbose=0, es_semaphore=None):
    """
    Function to insert a new register in the database if it is not found in the colav(kahi works) database.
    This means that the register is not on the database and it is being inserted.

    For similarity purposes, the register is also inserted in the elasticsearch index,
    all the elastic search fields are filled with the information from the register and it is
    handled by Mohan's Similarity class.

    The register is also linked to the source of the register, and the authors and affiliations are searched in the database.

    Parameters
    ----------
    scienti_reg : dict
        Register from the scienti database
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
    # parse
    entry = parse_scienti(scienti_reg, empty_work.copy(), doi)
    if not entry.get("titles") or not entry["titles"][0].get(
            "title", "").strip():
        return
    # link
    source_db = None
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
        if "external_ids" in entry["source"].keys():
            if len(entry["source"]["external_ids"]) == 0:
                if verbose > 4:
                    if "title" in entry["source"].keys():
                        print(
                            f'Register with COD_RH: {scienti_reg["COD_RH"]} and '
                            f'COD_PRODUCTO: {scienti_reg["COD_PRODUCTO"]} could not be '
                            f'linked to a source with name: {entry["source"]["title"]}')
                    else:
                        print(
                            f'Register with COD_RH: {scienti_reg["COD_RH"]} and '
                            f'COD_PRODUCTO: {scienti_reg["COD_PRODUCTO"]} does not provide a source')
            else:
                if verbose > 4:
                    print(
                        f'Register with COD_RH: {scienti_reg["COD_RH"]} and COD_PRODUCTO: {scienti_reg["COD_PRODUCTO"]} could not be linked to a source with {entry["source"]["external_ids"][0]["source"]}: {entry["source"]["external_ids"][0]["id"]}')  # noqa: E501
        else:
            if "title" in entry["source"].keys():
                if entry["source"]["title"] == "":
                    if verbose > 4:
                        print(
                            f'Register with COD_RH: {scienti_reg["COD_RH"]} and '
                            f'COD_PRODUCTO: {scienti_reg["COD_PRODUCTO"]} does not provide a source')
                else:
                    if verbose > 4:
                        print(
                            f'Register with COD_RH: {scienti_reg["COD_RH"]} and '
                            f'COD_PRODUCTO: {scienti_reg["COD_PRODUCTO"]} could not be '
                            f'linked to a source with name: {entry["source"]["title"]}')
            else:
                if verbose > 4:
                    print(
                        f'Register with COD_RH: {scienti_reg["COD_RH"]} and '
                        f'COD_PRODUCTO: {scienti_reg["COD_PRODUCTO"]} could not be '
                        'linked to a source (no ids and no name)')

        entry["source"] = {"id": "", "name": entry["source"][
            "title"] if "title" in entry["source"].keys() else ""}

    author = entry["authors"][0]
    # search authors and affiliations in db
    author_db = None
    for ext in author["external_ids"]:
        author_db = db["person"].find_one({"external_ids.id": ext["id"]}, {
            "_id": 1, "full_name": 1, "affiliations": 1, "external_ids": 1, "first_names": 1, "last_names": 1})
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
        author = {
            "id": author_db["_id"],
            "full_name": author_db["full_name"],
            "affiliations": author["affiliations"]
        }
        aff_units = get_units_affiliations(db, author_db, author["affiliations"])
        for aff_unit in aff_units:
            if aff_unit not in author["affiliations"]:
                author["affiliations"].append(aff_unit)

    else:
        author_db = db["person"].find_one(
            {"full_name": author["full_name"]})
        if author_db:
            sources = [ext["source"]
                       for ext in author_db["external_ids"]]
            ids = [ext["id"] for ext in author_db["external_ids"]]
            for ext in author["external_ids"]:
                if ext["id"] not in ids:
                    author_db["external_ids"].append(ext)
                    sources.append(ext["source"])
                    ids.append(ext["id"])
            author = {
                "id": author_db["_id"],
                "full_name": author_db["full_name"],
                "affiliations": author["affiliations"]
            }
            aff_units = get_units_affiliations(
                db, author_db, author["affiliations"])
            for aff_unit in aff_units:
                if aff_unit not in author["affiliations"]:
                    author["affiliations"].append(aff_unit)

        else:
            author = {
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
            author["affiliations"][j] = {
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
                author["affiliations"][j] = {
                    "id": aff_db["_id"],
                    "name": name,
                    "types": aff_db["types"]
                }
            else:
                author["affiliations"][j] = {
                    "id": "",
                    "name": aff["name"],
                    "types": []
                }
    entry["authors"][0] = author

    # Inserting other authors
    directed_work = False
    other_authors = scienti_reg.get("re_author_others", [])
    primary_cod_rh = scienti_reg["COD_RH"]

    for other_author in other_authors:
        author_others = other_author.get("author_others")
        if not author_others:
            author_others = other_author.get("author")
            if author_others:
                directed_work = True
            if not author_others:
                continue

        if directed_work:
            cod_rh_ref = author_others[0].get("COD_RH")
        else:
            cod_rh_ref = author_others[0].get("COD_RH_REF")
        if not cod_rh_ref:
            continue

        author_role = other_author.get("TPO_PARTICIPACION", "")

        if author_role and cod_rh_ref == primary_cod_rh:
            entry["authors"][0]["type"] = "advisor" if author_role in [
                "TUT", "ASE", "COT"] else ""
            continue  # first author is already inserted

        author_db = db["person"].find_one(
            {"external_ids.id.COD_RH": cod_rh_ref}, {"_id": 1, "full_name": 1})

        if not author_db:
            continue

        # Add author to the list of authors only if it is not already in the
        # list
        if not any(author["id"] == author_db["_id"]
                   for author in entry["authors"]):
            entry["authors"].append({
                "id": author_db["_id"],
                "full_name": author_db["full_name"],
                "affiliations": []
            })

    entry["author_count"] = len(entry["authors"])

    # Update date_published if oriented_thesis is available
    details = scienti_reg.get("details", [])
    if details and "oriented_thesis" in details[0].keys():
        oriented_tesis = details[0].get("oriented_thesis")[
            0] if details else None
        if oriented_tesis:
            year = None
            month = None
            if "NRO_ANO_FIN" in oriented_tesis.keys():
                year = oriented_tesis["NRO_ANO_FIN"]
            if "NRO_MES_FIN" in oriented_tesis.keys():
                month = oriented_tesis["NRO_MES_FIN"]
                if len(str(month)) == 1:
                    month = f'0{month}'
            if year and month:
                entry["date_published"] = check_date_format(
                    f'{month}-{year}')
                entry["year_published"] = int(year)

    # scienti group
    if "group" in scienti_reg.keys():
        for group in scienti_reg["group"]:
            group_reg = db["affiliations"].find_one(
                {"external_ids.id": group["COD_ID_GRUPO"]})
            if group_reg is None:
                group_reg = db["affiliations"].find_one(
                    {"external_ids.id": group["NRO_ID_GRUPO"]})
            if group_reg:
                entry["groups"].append(
                    {"id": group_reg["_id"], "name": group_reg["names"][0]["name"]})
            if not group_reg:
                print(
                    f'WARNING: group with ids {group.get("COD_ID_GRUPO")} and '
                    f'{group.get("NRO_ID_GRUPO")} found not in affiliation')

    # Insert idempotently so a failed Elasticsearch operation can be retried.
    identity = scienti_identity(entry)
    if identity:
        title = entry["titles"][0]["title"]
        with scienti_insert_lock(identity, title):
            response = collection.update_one(
                scienti_retry_query(identity, title),
                {"$setOnInsert": entry},
                upsert=True,
            )
            inserted_id = response.upserted_id
    else:
        response = collection.insert_one(entry)
        inserted_id = response.inserted_id
    # insert or reconcile in elasticsearch
    if inserted_id is None and identity:
        existing = collection.find_one(
            scienti_retry_query(identity, entry["titles"][0]["title"]),
            {"_id": 1})
        inserted_id = existing.get("_id") if existing else None
    ensure_es_work(entry, inserted_id, es_handler, es_semaphore)


def process_one(scienti_reg, db, collection, empty_work, es_handler,
                similarity, verbose=0, es_semaphore=None):
    """
    Function to process a single register from the scienti database.
    This function is used to insert or update a register in the colav(kahi works) database.

    Parameters
    ----------
    scienti_reg : dict
        Register from the scienti database
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
    title = scienti_reg.get("TXT_NME_PROD")
    if not isinstance(title, str) or not title.strip():
        return

    doi = None
    # register has doi
    if "TXT_DOI" in scienti_reg.keys():
        if scienti_reg["TXT_DOI"]:
            doi = doi_processor(scienti_reg["TXT_DOI"])
    if not doi:
        if "TXT_WEB_PRODUCTO" in scienti_reg.keys(
        ) and scienti_reg["TXT_WEB_PRODUCTO"] and "10." in scienti_reg["TXT_WEB_PRODUCTO"]:
            doi = doi_processor(scienti_reg["TXT_WEB_PRODUCTO"])
            if doi:
                extracted_doi = re.compile(
                    r'10\.\d{4,9}/[-._;()/:A-Z0-9]+', re.IGNORECASE).match(doi)
                if extracted_doi:
                    doi = extracted_doi.group(0)
                    for keyword in [
                        'abstract',
                        'homepage',
                        'tpmd200765',
                            'event_abstract']:
                        doi = doi.split(
                            f'/{keyword}')[0] if keyword in doi else doi
    if doi:
        # is the doi in colavdb?
        colav_reg = collection.find_one({"external_ids.id": doi})

        if colav_reg:  # update the register
            process_one_update(
                scienti_reg,
                colav_reg,
                db,
                collection,
                empty_work,
                verbose=verbose)
        else:  # insert a new register
            process_one_insert(
                scienti_reg, db, collection, empty_work, es_handler, doi,
                verbose=verbose, es_semaphore=es_semaphore)
    elif similarity:  # does not have a doi identifier
        # elasticsearch section
        if es_handler:
            # Search in elasticsearch
            entry = parse_scienti(
                scienti_reg, empty_work.copy(), verbose=verbose)
            if not entry.get("titles") or not entry["titles"][0].get(
                    "title", "").strip():
                return
            identity = scienti_identity(entry)
            existing = (collection.find_one(
                scienti_retry_query(identity, entry["titles"][0]["title"]),
                {"_id": 1}) if identity else None)
            if existing:
                ensure_es_work(
                    entry, existing["_id"], es_handler, es_semaphore)
                return
            work = {}
            work["title"] = entry["titles"][0]["title"]
            work["source"] = entry["source"]["name"] if "name" in entry["source"].keys(
            ) else ""
            work["year"] = entry["year_published"] if entry["year_published"] else "0"
            work["volume"] = entry["bibliographic_info"]["volume"] if "volume" in entry["bibliographic_info"].keys() else ""
            work["issue"] = entry["bibliographic_info"]["issue"] if "issue" in entry["bibliographic_info"].keys() else ""
            work["first_page"] = entry["bibliographic_info"].get(
                "start_page", "")
            work["last_page"] = entry["bibliographic_info"].get("end_page", "")
            authors = []
            for author in entry['authors']:
                if len(authors) >= 5:
                    break
                if "full_name" in author.keys():
                    authors.append(author["full_name"])
            work["authors"] = authors
            work["provenance"] = "scienti"
            response = run_es_operation(
                es_semaphore, es_handler.search_work,
                title=work["title"],
                source=work["source"],
                year=str(work["year"]),
                authors=authors,
                volume=work["volume"],
                issue=work["issue"],
                page_start=work["first_page"],
                page_end=work["last_page"]
            )

            if response:  # register already on db... update accordingly
                colav_reg = collection.find_one(
                    {"_id": ObjectId(response["_id"])})
                if colav_reg:
                    # TODO: add author check here before to do the update
                    if has_scienti_source(
                            entry["external_ids"]) and has_scienti_source(
                            colav_reg["external_ids"]):
                        if cod_product_mismatch(
                                entry["external_ids"], colav_reg["external_ids"]):
                            # if they have the same COD_RH  but different COD_PRODUCTO
                            # then insert the new register
                            process_one_insert(
                                scienti_reg,
                                db,
                                collection,
                                empty_work,
                                es_handler,
                                doi=None,
                                verbose=verbose,
                                es_semaphore=es_semaphore)
                            return
                    if has_scienti_source(
                            entry["types"]) and has_scienti_source(
                            colav_reg["types"]):
                        # if type is equal, then update the register
                        if check_first_level_type(
                                entry["types"], colav_reg["types"]):
                            process_one_update(scienti_reg, colav_reg, db,
                                               collection, empty_work, verbose)
                        else:
                            process_one_insert(
                                scienti_reg,
                                db,
                                collection,
                                empty_work,
                                es_handler,
                                doi=None,
                                verbose=verbose,
                                es_semaphore=es_semaphore)
                    else:  # there is not scienti types to compare, then update them
                        process_one_update(scienti_reg, colav_reg, db,
                                           collection, empty_work, verbose)

                else:
                    if verbose > 4:
                        print("Register with {} not found in mongodb".format(
                            response["_id"]))
                        print(response)
            else:  # insert new register
                process_one_insert(scienti_reg, db, collection,
                                   empty_work, es_handler, doi=None,
                                   verbose=verbose, es_semaphore=es_semaphore)
        else:
            if verbose > 4:
                print("No elasticsearch index provided")
