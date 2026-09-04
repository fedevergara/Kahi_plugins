from kahi_minciencias_opendata_works.parser import parse_minciencias_opendata
from kahi_impactu_utils.Utils import compare_author
from thefuzz import process, fuzz
from unidecode import unidecode
from time import time
from bson import ObjectId
from re import search, sub
from threading import Lock


_PRODUCT_LOCKS = tuple(Lock() for _ in range(1024))


def _product_lock(product_id):
    return _PRODUCT_LOCKS[hash(product_id) % len(_PRODUCT_LOCKS)]


def run_es_operation(semaphore, operation, *args, **kwargs):
    if semaphore is None:
        return operation(*args, **kwargs)
    with semaphore:
        return operation(*args, **kwargs)


def product_query(product_id):
    return {
        "external_ids": {
            "$elemMatch": {"source": "minciencias", "id": product_id}
        }
    }


def build_es_work(entry):
    titles = entry.get("titles") or []
    title = titles[0].get("title", "").strip() if titles else ""
    if not title:
        return None
    bibliography = entry.get("bibliographic_info") or {}
    return {
        "title": title,
        "source": (entry.get("source") or {}).get("name", ""),
        "year": entry.get("year_published") or "0",
        "volume": bibliography.get("volume", ""),
        "issue": bibliography.get("issue", ""),
        "first_page": bibliography.get("start_page", ""),
        "last_page": bibliography.get("end_page", ""),
        "authors": [
            author.get("full_name", "")
            for author in entry.get("authors", [])[:5]
            if author.get("full_name")
        ],
        "provenance": "minciencias",
    }


def ensure_es_work(entry, work_id, es_handler, es_semaphore=None):
    if not es_handler or work_id is None:
        return
    work = build_es_work(entry)
    if work is None:
        return
    es_client = getattr(es_handler, "es", None)
    es_index = getattr(es_handler, "es_index", None)
    exists = False
    if es_client is not None and es_index:
        exists = run_es_operation(
            es_semaphore, es_client.exists, index=es_index, id=str(work_id)
        )
    if not exists:
        run_es_operation(
            es_semaphore, es_handler.insert_work, _id=str(work_id), work=work
        )


def find_work_by_es_id(collection, value):
    if value is None:
        return None
    found = collection.find_one({"_id": value})
    if found:
        return found
    try:
        return collection.find_one({"_id": ObjectId(value)})
    except Exception:
        return None


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
        list of affiliations from the parse_minciencias_opendata method

    Returns:
    -------
    list
        list of units of an author (entries from using affiliations)
    """
    institution_id = None
    # verifiying univeristy
    for j, aff in enumerate(affiliations):
        aff_db = db["affiliations"].find_one(
            {"_id": aff["id"]}, {"_id": 1, "types": 1})
        if aff_db:
            types = [i["type"] for i in aff_db["types"]]
            if "group" in types or "department" in types or "faculty" in types:
                aff_db = None
                continue
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
        openadata_reg,
        colav_reg,
        db,
        collection,
        empty_work,
        verbose=0):
    """
    Method to update a register in the kahi database from minciencias opendata database if it is found.
    This means that the register is already on the kahi database and it is being updated with new information.


    Parameters
    ----------
    openadata_reg : dict
        Register from the minciencias opendata database
    colav_reg : dict
        Register from the colav database (kahi database for impactu)
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    entry = parse_minciencias_opendata(
        openadata_reg, empty_work.copy(), verbose=verbose)
    # updated
    minciencias_update = next(
        (upd for upd in colav_reg["updated"] if upd["source"] == "minciencias"),
        None,
    )
    if minciencias_update:
        minciencias_update["time"] = int(time())
    else:
        colav_reg["updated"].append(
            {"source": "minciencias", "time": int(time())})
    # titles
    if 'minciencias' not in [title['source'] for title in colav_reg["titles"]]:
        lang = entry["titles"][0]["lang"]
        colav_reg["titles"].append(
            {"title": entry["titles"][0]["title"], "lang": lang, "source": "minciencias"})
    # external_ids
    exts = [ext_ for ext_ in colav_reg["external_ids"]]
    for ext in entry["external_ids"]:
        if ext not in exts:
            colav_reg["external_ids"].append(ext)
            exts.append(ext["id"])
    # types
    types = [ext["source"] for ext in colav_reg["types"]]
    for typ in entry["types"]:
        if typ["source"] not in types:
            colav_reg["types"].append(typ)
    # authors
    minciencias_author = ""
    if "authors" in entry.keys():
        if entry["authors"]:
            minciencias_author = entry["authors"][0]
    author_db = None
    if minciencias_author:
        author_found = False
        if "external_ids" in minciencias_author.keys(
        ) and minciencias_author["affiliations"]:
            for ext in minciencias_author["external_ids"]:
                author_db = db["person"].find_one(
                    {"external_ids.id": ext["id"]})
                if author_db:
                    group_id = minciencias_author["affiliations"][0]['external_ids'][0]['id']

                    group_db = db["affiliations"].find_one(
                        {"external_ids.source": "scienti", "external_ids.id": group_id})
                    if not group_db:
                        group_db = db["affiliations"].find_one(
                            {"external_ids.id": group_id})

                    if group_db:
                        for i, author in enumerate(colav_reg["authors"]):
                            if author_db["_id"] != author["id"]:
                                continue
                            # Adding group to existing author in colav register
                            author_affiliations = [
                                str(aff['id']) for aff in author['affiliations']]
                            if str(group_db["_id"]) not in author_affiliations:
                                author["affiliations"].append(
                                    {
                                        "id": group_db["_id"],
                                        "name": group_db["names"][0]["name"],
                                        "types": group_db["types"]})
                                author_found = True
                                if verbose > 4:
                                    print("group added to author: {}".format(
                                        group_db["names"][0]["name"]))
                                break
                            else:
                                author_found = True
                                if verbose > 4:
                                    print("group already in author")
                                break

                        if not author_found:
                            affiliation_match = False
                            for i, author in enumerate(colav_reg["authors"]):
                                if author['id'] == "":
                                    if verbose >= 4:
                                        print(
                                            f"WARNING: author with id '' found in colav register: {author}")
                                    continue
                                # only the name can be compared, because we
                                # dont have the affiliation of the author from
                                # the paper in author_others
                                author_reg = db['person'].find_one(
                                    # this is required to get  first_names and
                                    # last_names
                                    {'_id': author['id']}, {"_id": 1, "full_name": 1, "first_names": 1, "last_names": 1, "initials": 1})

                                name_match = compare_author(
                                    author_reg, author_db, len(colav_reg["authors"]))
                                # If the author matches, replace the id and
                                # full_name of the record in process so as not
                                # to break the aggregation of affiliations.
                                if name_match:
                                    author["id"] = author_db["_id"]
                                    author["full_name"] = author_db["full_name"]

                                if author['affiliations']:
                                    affiliations_person = [
                                        str(aff['id']) for aff in author_db['affiliations']]
                                    author_affiliations = [
                                        str(aff['id']) for aff in author['affiliations']]
                                    affiliation_match = any(
                                        affil in author_affiliations for affil in affiliations_person)
                                if name_match and affiliation_match:
                                    author["affiliations"].append({
                                        "id": group_db["_id"],
                                        "name": group_db["names"][0]["name"].strip(),
                                        "types": group_db["types"]})
                                    author_found = True
                                    break

                        if not author_found:
                            colav_reg["authors"].append(
                                {"id": author_db["_id"], "full_name": author_db["full_name"], "affiliations": [
                                    {
                                        "id": group_db["_id"],
                                        "name": group_db["names"][0]["name"],
                                        "types": group_db["types"]
                                    }
                                ]}
                            )

                else:
                    if verbose > 4:
                        print("No author in db with external id")
        else:
            if verbose > 4:
                print("No author data")
    # Adding advisor type to author if the work is a thesis
    if author_db:
        for type in entry["types"]:
            if type["level"] == 1 and type["type"] in [
                    "Tesis de pregrado", "Tesis de maestria", "Tesis de doctorado"]:
                for author in colav_reg["authors"]:
                    if author["id"] == author_db["_id"]:
                        author["type"] = "advisor"
                        break
    # work ranking
    if "ranking" in entry.keys():
        for rank in entry["ranking"]:
            if rank not in colav_reg["ranking"]:
                colav_reg["ranking"].append(rank)
    # groups
    group_id = openadata_reg["cod_grupo_gr"]
    rgroup = db["affiliations"].find_one({"external_ids.id": group_id})
    if rgroup:
        found = False
        for group in colav_reg["groups"]:
            if group["id"] == rgroup["_id"]:
                found = True
                break
        if not found:
            colav_reg["groups"].append(
                {"id": rgroup["_id"], "name": rgroup["names"][0]["name"]})

        # Adding group relation affiliation to the author affiliations
        relations = rgroup.get("relations", [])
        # Counting education type relations, if there is only one, it will be
        # added to the author
        edu_count = sum(
            1 for rel in relations if any(
                (t.get("type") or "") == "Education" for t in rel.get(
                    "types", [])))

        if author_db and rgroup.get("relations") and edu_count == 1:
            for author in colav_reg["authors"]:
                if author["id"] == author_db["_id"]:
                    affs = [aff["id"] for aff in author["affiliations"]]
                    for relation in rgroup["relations"]:
                        types = []
                        if "types" in relation.keys() and relation["types"]:
                            types = [rel["type"].lower()
                                     for rel in relation["types"]]
                            if "education" in types:
                                if relation["id"] not in affs:
                                    author["affiliations"].append(relation)
                    aff_units = get_units_affiliations(
                        db, author_db, author["affiliations"])
                    for aff_unit in aff_units:
                        if aff_unit not in author["affiliations"]:
                            author["affiliations"].append(aff_unit)

                    break

    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "types": colav_reg["types"],
            "authors": colav_reg["authors"],
            "ranking": colav_reg["ranking"],
            "groups": colav_reg["groups"]
        }}
    )


def process_one_insert(openadata_reg, db, collection, empty_work, es_handler,
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
    openadata_reg : dict
        Register from the minciencias opendata database
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
    entry = parse_minciencias_opendata(openadata_reg, empty_work.copy())
    if entry is None:
        return
    # authors
    minciencias_author = ""
    author_db = None
    if "authors" in entry.keys():
        if entry["authors"]:
            minciencias_author = entry["authors"][0]
    if minciencias_author:
        # and minciencias_author["affiliations"]:
        if "external_ids" in minciencias_author.keys():
            for ext in minciencias_author["external_ids"]:
                author_db = db["person"].find_one(
                    {"external_ids.id": ext["id"]})
                if author_db:
                    entry["authors"][0]["id"] = author_db["_id"]
                    entry["authors"][0]["full_name"] = author_db["full_name"]
                    # Adding advisor type to author if the work is a thesis
                    for type in entry["types"]:
                        if type["level"] == 1 and type["type"] in [
                                "Tesis de pregrado", "Tesis de maestria", "Tesis de doctorado"]:
                            entry["authors"][0]["type"] = "advisor"
                            break
                    # Adding affiliations to author
                    if minciencias_author["affiliations"]:
                        group_id = minciencias_author["affiliations"][0]['external_ids'][0]['id']
                        affiliations_db = db["affiliations"].find_one(
                            {"external_ids.id": group_id})

                        if affiliations_db:
                            if entry['authors'][0]['external_ids'][0]['id'] == ext['id']:
                                entry['authors'][0]["affiliations"].append(
                                    {
                                        "id": affiliations_db["_id"] if affiliations_db else None,
                                        "name": affiliations_db["names"][0]["name"].strip() if affiliations_db else None,
                                        "types": affiliations_db["types"] if affiliations_db else None})
                                if len(
                                        entry['authors'][0]["affiliations"]) > 1:
                                    entry['authors'][0]["affiliations"].pop(0)
                                if verbose > 4:
                                    print(
                                        "group added to author: {}".format(
                                            affiliations_db["names"][0]["name"] if affiliations_db else None))
                                break
                        else:
                            print(
                                f"WARNING: group {group_id} found not in affiliation db")
            del entry["authors"][0]["external_ids"]

        else:
            if verbose > 4:
                print("No author data")
    if entry.get("authors") and entry["authors"][0].get("full_name") == "":
        del entry["authors"][0]  # this is an empty author, so it is removed

    # authors count
    entry["author_count"] = len(entry["authors"])

    # group
    group_id = openadata_reg.get("cod_grupo_gr")
    rgroup = (db["affiliations"].find_one({"external_ids.id": group_id})
              if group_id else None)
    if rgroup:
        found = False
        for group in entry["groups"]:
            if group["id"] == rgroup["_id"]:
                found = True
                break
        if not found:
            entry["groups"].append(
                {"id": rgroup["_id"], "name": rgroup["names"][0]["name"]})

        # Adding group relation affiliation to the author affiliations
        if author_db and rgroup.get("relations"):
            for author in entry["authors"]:
                if author["id"] == author_db["_id"]:
                    affs = [aff["id"] for aff in author["affiliations"]]
                    for relation in rgroup["relations"]:
                        types = []
                        if "types" in relation.keys() and relation["types"]:
                            types = [rel["type"].lower()
                                     for rel in relation["types"]]
                            if "education" in types:
                                if relation["id"] not in affs:
                                    author["affiliations"].append(relation)
                    aff_units = get_units_affiliations(
                        db, author_db, author["affiliations"])
                    for aff_unit in aff_units:
                        if aff_unit not in author["affiliations"]:
                            author["affiliations"].append(aff_unit)
                    break
    product_id = openadata_reg.get("id_producto_pd")
    if product_id:
        with _product_lock(product_id):
            response = collection.update_one(
                product_query(product_id), {"$setOnInsert": entry}, upsert=True
            )
            work_id = response.upserted_id
            if work_id is None:
                existing = collection.find_one(
                    product_query(product_id), {"_id": 1})
                work_id = existing.get("_id") if existing else None
    else:
        response = collection.insert_one(entry)
        work_id = response.inserted_id
    ensure_es_work(entry, work_id, es_handler, es_semaphore)


def str_normilize(word):
    return unidecode(word).lower().strip().replace(".", "")


def check_work(title_work, authors, response, thresholds):
    author_found = False
    if authors:
        if authors[0] != "":
            _authors = []
            for author in response["_source"]["authors"]:
                _authors.append(str_normilize(author))
            scores = process.extract(str_normilize(
                authors[0]), _authors, scorer=fuzz.partial_ratio)
            for score in scores:
                if score[1] >= thresholds["author_thd"]:
                    author_found = True
                    break
    es_title = response["_source"]["title"]
    if es_title:
        score = fuzz.ratio(str_normilize(title_work),
                           str_normilize(es_title))
        if author_found:
            if score >= thresholds["paper_thd_low"]:
                return True
        else:
            if score >= thresholds["paper_thd_high"]:
                return True
    return False


def process_one(openadata_reg, db, collection, empty_work, es_handler,
                insert_all, thresholds, verbose=0, es_semaphore=None):
    """
    Function to process a single register from the minciencias opendata database.
    This function is used to insert or update a register in the colav(kahi works) database.

    Parameters
    ----------
    openadata_reg : dict
        Register from the minciencias opendata database
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of works)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    es_handler : Similarity
        Elasticsearch handler to insert the register in the elasticsearch index, Mohan's Similarity class.
    insert_all : bool
        Flag to insert all the registers in the minciencias opendata database.
    thresholds : list
        List with the thresholds for the similarity functions.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    title = openadata_reg.get("nme_producto_pd")
    if not isinstance(title, str) or not title.strip():
        return
    # type id verification
    if "id_producto_pd" in openadata_reg.keys():
        if openadata_reg["id_producto_pd"]:
            COD_RH = ""
            COD_PROD = ""
            product_id = openadata_reg["id_producto_pd"]
            match = search(r'(\d{9,11})-(\d{1,7})$', product_id)
            if match:
                COD_RH = match.group(1)
                COD_PROD = match.group(2)

                if COD_RH and COD_PROD:
                    colav_reg = collection.find_one({"$or": [
                        product_query(product_id),
                        {"external_ids.id": {"COD_RH": COD_RH, "COD_PRODUCTO": COD_PROD}},
                    ]})
                    if colav_reg:
                        process_one_update(
                            openadata_reg, colav_reg, db, collection, empty_work, verbose)
                        ensure_es_work(colav_reg, colav_reg["_id"], es_handler,
                                       es_semaphore)
                        return

    # elasticsearch section
    if es_handler:
        # Search in elasticsearch
        if thresholds and len(thresholds) == 3:
            thresholds = {
                "author_thd": thresholds[0],
                "paper_thd_low": thresholds[1],
                "paper_thd_high": thresholds[2]}
        else:
            if verbose > 4:
                print("Invalid thresholds values provided, using default values")
            thresholds = {"author_thd": 65,
                          "paper_thd_low": 90, "paper_thd_high": 95}

        authors = []
        title_work = ""
        if 'nme_producto_pd' in openadata_reg.keys():
            if openadata_reg["nme_producto_pd"]:
                title_work = openadata_reg["nme_producto_pd"]

        if 'id_persona_pd' in openadata_reg.keys():
            if openadata_reg["id_persona_pd"]:
                author_db = db["person"].find_one(
                    {"external_ids.id.COD_RH": openadata_reg["id_persona_pd"]}, {"_id": 1, "full_name": 1})
                if author_db:
                    authors.append(author_db["full_name"])

        if authors and title_work != "":
            responses = run_es_operation(es_semaphore, es_handler.search_work,
                                         title=title_work,
                                         source="",
                                         year="0",
                                         authors=authors,
                                         volume="",
                                         issue="",
                                         page_start="",
                                         page_end="",
                                         use_es_thold=True,
                                         es_thold=0,
                                         hits=20
                                         )
            if responses:
                for response in responses:
                    out = check_work(title_work, authors, response, thresholds)
                    if out:
                        colav_reg = find_work_by_es_id(
                            collection, response.get("_id"))
                        if colav_reg:
                            process_one_update(
                                openadata_reg, colav_reg, db, collection, empty_work, verbose)
                            return
                        else:
                            if verbose > 4:
                                print(
                                    "Register with {} not found in mongodb".format(
                                        response["_id"]))
                            return
            # No matching work, including the case where Elasticsearch returned
            # no hits.
            if insert_all:
                process_one_insert(
                    openadata_reg, db, collection, empty_work, es_handler,
                    verbose, es_semaphore)

        elif title_work:
            # No authors
            title = sub('[_|,\\\\]', '', title_work).lower()
            es_results = run_es_operation(es_semaphore, es_handler.search_work,
                                          title=title,
                                          source="",
                                          year="0",
                                          authors=[],
                                          volume="",
                                          issue="",
                                          page_start="",
                                          page_end="",
                                          use_es_thold=True,
                                          es_thold=0,
                                          hits=20
                                          )
            if es_results:
                for es_work in es_results:
                    colav_reg = find_work_by_es_id(
                        collection, es_work.get("_id"))
                    if colav_reg:
                        titles = [titles.get('title')
                                  for titles in colav_reg.get("titles", [])
                                  if titles.get("title")]
                        match = process.extractOne(
                            title_work, titles) if titles else None
                        if match and match[1] > thresholds["paper_thd_high"]:
                            process_one_update(
                                openadata_reg, colav_reg, db, collection, empty_work, verbose)
                            return
                    else:
                        if verbose > 4:
                            print(
                                "Register with {} not found in mongodb".format(
                                    es_work.get("_id")))
                        return

            if insert_all:
                process_one_insert(
                    openadata_reg, db, collection, empty_work, es_handler,
                    verbose, es_semaphore)
    else:
        process_one_insert(
            openadata_reg, db, collection, empty_work, es_handler,
            verbose, es_semaphore)
        if verbose > 4:
            print("No elasticsearch index provided")
