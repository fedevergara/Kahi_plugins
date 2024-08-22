from kahi_minciencias_opendata_events.parser import parse_minciencias_opendata
from kahi_impactu_utils.Utils import compare_author
from thefuzz import process, fuzz
from unidecode import unidecode
from time import time
from bson import ObjectId
from re import search, sub


def process_one_update(openadata_reg, colav_reg, db, collection, empty_event, verbose=0):
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
        Collection in the database where the register is stored (Collection of events)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    entry = parse_minciencias_opendata(
        openadata_reg, empty_event.copy(), verbose=verbose)
    # updated
    for upd in colav_reg["updated"]:
        if upd["source"] == "minciencias":
            # autor y cod prod
            return None  # Register already on db
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
        if "external_ids" in minciencias_author.keys() and minciencias_author["affiliations"]:
            for ext in minciencias_author["external_ids"]:
                author_db = db["person"].find_one(
                    {"external_ids.id.COD_RH": ext["id"]})
                if not author_db:
                    print(
                        f"WARNING: author not found in db with external id {ext['id']}")
                if author_db:
                    group_id = minciencias_author["affiliations"][0]['external_ids'][0]['id']

                    affiliations_db = db["affiliations"].find_one(
                        {"external_ids.source": "scienti", "external_ids.id": group_id})
                    if not affiliations_db:
                        affiliations_db = db["affiliations"].find_one(
                            {"external_ids.id": group_id})

                    if affiliations_db:
                        for i, author in enumerate(colav_reg["authors"]):
                            if author_db["_id"] != author["id"]:
                                continue
                            # Adding group to existing author in colav register
                            author_affiliations = [str(aff['id'])
                                                   for aff in author['affiliations']]
                            if str(affiliations_db["_id"]) not in author_affiliations:
                                author["affiliations"].append(
                                    {
                                        "id": affiliations_db["_id"],
                                        "name": affiliations_db["names"][0]["name"],
                                        "types": affiliations_db["types"]})
                                author_found = True
                                if verbose > 4:
                                    print("group added to author: {}".format(
                                        affiliations_db["names"][0]["name"]))
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
                                # only the name can be compared, because we dont have the affiliation of the author from the paper in author_others
                                author_reg = db['person'].find_one(
                                    # this is required to get  first_names and last_names
                                    {'_id': author['id']}, {"_id": 1, "full_name": 1, "first_names": 1, "last_names": 1, "initials": 1})

                                name_match = compare_author(
                                    author_reg, author_db)
                                # If the author matches, replace the id and full_name of the record in process so as not to break the aggregation of affiliations.
                                if name_match:
                                    author["id"] = author_db["_id"]
                                    author["full_name"] = author_db["full_name"]

                                if author['affiliations']:
                                    affiliations_person = [str(aff['id'])
                                                           for aff in author_db['affiliations']]
                                    author_affiliations = [str(aff['id'])
                                                           for aff in author['affiliations']]
                                    affiliation_match = any(
                                        affil in author_affiliations for affil in affiliations_person)
                                if name_match and affiliation_match:
                                    author["affiliations"].append({
                                        "id": affiliations_db["_id"],
                                        "name": affiliations_db["names"][0]["name"].strip(),
                                        "types": affiliations_db["types"]})
                                    author_found = True
                                    break

                        if not author_found:
                            colav_reg["authors"].append(
                                {"id": author_db["_id"], "full_name": author_db["full_name"], "affiliations": [
                                    {
                                        "id": affiliations_db["_id"],
                                        "name": affiliations_db["names"][0]["name"],
                                        "types": affiliations_db["types"]
                                    }
                                ]}
                            )

                else:
                    if verbose > 4:
                        print("No author in db with external id")
        else:
            if verbose > 4:
                print("No author data")
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
        if author_db and rgroup["relations"]:
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
                    break

    collection.update_one(
        {"_id": colav_reg["_id"]},
        {"$set": {
            "updated": colav_reg["updated"],
            "titles": colav_reg["titles"],
            "external_ids": colav_reg["external_ids"],
            "types": colav_reg["types"],
            "authors": colav_reg["authors"],
            "groups": colav_reg["groups"]
        }}
    )


def process_one_insert(openadata_reg, db, collection, empty_work, es_handler, verbose=0):
    """
    Function to insert a new register in the database if it is not found in the colav(kahi events) database.
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
        Collection in the database where the register is stored (Collection of events)
    empty_work : dict
        Empty dictionary with the structure of a register in the database
    es_handler : Similarity
        Elasticsearch handler to insert the register in the elasticsearch index, Mohan's Similarity class.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    # parse
    entry = parse_minciencias_opendata(openadata_reg, empty_work.copy())
    # search authors and affiliations in db
    entry["author_count"] = len(entry["authors"])
    # authors
    minciencias_author = ""
    if "authors" in entry.keys():
        if entry["authors"]:
            minciencias_author = entry["authors"][0]
    if minciencias_author:
        if "external_ids" in minciencias_author.keys() and minciencias_author["affiliations"]:
            for ext in minciencias_author["external_ids"]:
                author_db = db["person"].find_one(
                    {"external_ids.id": ext["id"]})
                if not author_db:
                    print(
                        f"WARNING: author not found in db with external id {ext['id']}")
                if author_db:
                    group_id = minciencias_author["affiliations"][0]['external_ids'][0]['id']
                    affiliations_db = db["affiliations"].find_one(
                        {"external_ids.id": group_id})
                    if not affiliations_db:
                        affiliations_db = db["affiliations"].find_one(
                            {"external_ids.id": group_id})
                    if author_db:
                        for author in entry['authors']:
                            if author['external_ids'][0]['id'] == ext['id']:
                                author["id"] = author_db["_id"]
                                author["full_name"] = author_db["full_name"]
                                author["affiliations"].append(
                                    {
                                        "id": affiliations_db["_id"] if affiliations_db else None,
                                        "name": affiliations_db["names"][0]["name"].strip() if affiliations_db else None,
                                        "types": affiliations_db["types"] if affiliations_db else None
                                    }
                                )
                                if len(author["affiliations"]) > 1:
                                    author["affiliations"].pop(0)
                                if verbose > 4:
                                    print("group added to author: {}".format(
                                        affiliations_db["names"][0]["name"] if affiliations_db else None))
                                break
                else:
                    if verbose > 4:
                        print(
                            f"No author in db with external id {minciencias_author}")
        else:
            if verbose > 4:
                print("No author data")
                return
    # group
    group_id = openadata_reg["cod_grupo_gr"]
    rgroup = db["affiliations"].find_one({"external_ids.id": group_id})
    if rgroup:
        entry["groups"].append(
            {"id": rgroup["_id"], "name": rgroup["names"][0]["name"]})

    # insert in mongo
    response = collection.insert_one(entry)


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


def process_one(openadata_reg, db, collection, empty_work, es_handler, insert_all, thresholds, verbose=0):
    """
    Function to process a single register from the minciencias opendata database.
    This function is used to insert or update a register in the colav(kahi events) database.

    Parameters
    ----------
    openadata_reg : dict
        Register from the minciencias opendata database
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of events)
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
                    colav_reg = collection.find_one(
                        {"external_ids.id": {"COD_RH": COD_RH, "COD_PRODUCTO": COD_PROD}})
                    if colav_reg:
                        process_one_update(
                            openadata_reg, colav_reg, db, collection, empty_work, verbose)
                        return

    process_one_insert(
        openadata_reg, db, collection, empty_work, es_handler, verbose)
