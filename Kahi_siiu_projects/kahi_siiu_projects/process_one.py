from kahi_siiu_projects.parser import parse_siiu


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
        list of affiliations from the parse_siiu method

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


def process_one_insert(siiu_reg, db, collection, empty_project, es_handler, verbose=0):
    """
    Function to insert a new register in the database if it is not found in the colav(kahi projects) database.
    This means that the register is not on the database and it is being inserted.

    For similarity purposes, the register is also inserted in the elasticsearch index,
    all the elastic search fields are filled with the information from the register and it is
    handled by Mohan's Similarity class.

    The register is also linked to the source of the register, and the authors and affiliations are searched in the database.

    Parameters
    ----------
    siiu_reg : dict
        Register from the SIIU database
    db : pymongo.database.Database
        Database where the colav collections are stored, used to search for authors and affiliations.
    collection : pymongo.collection.Collection
        Collection in the database where the register is stored (Collection of projects)
    empty_project : dict
        Empty dictionary with the structure of a register in the database
    es_handler : Similarity
        Elasticsearch handler to insert the register in the elasticsearch index, Mohan's Similarity class.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    # parse
    entry = parse_siiu(siiu_reg, empty_project.copy())
    if entry is None:
        return
    # search authors and affiliations in db
    # authors
    valid_authors = []
    for author in entry["authors"]:
        # and author["affiliations"]:
        for ext in author["external_ids"]:
            author_db = db["person"].find_one(
                {"external_ids.id": ext["id"]})
            if author_db:
                author["id"] = author_db["_id"]
                author["full_name"] = author_db["full_name"]
                affiliations = []
                for aff in author["affiliations"]:
                    for aff_id in aff["external_ids"]:
                        if aff_id["source"] == "nit":
                            affiliations_db = db["affiliations"].find_one(
                                {"external_ids.id": {"$regex": f"^{aff_id['id']}", "$options": "i"}})
                        else:
                            affiliations_db = db["affiliations"].find_one(
                                {"external_ids.id": aff_id['id']})

                        if affiliations_db:
                            if author['external_ids'][0]['id'] == ext['id']:
                                affiliations.append(
                                    {
                                        "id": affiliations_db["_id"] if affiliations_db else None,
                                        "name": affiliations_db["names"][0]["name"].strip() if affiliations_db else None,
                                        "types": affiliations_db["types"] if affiliations_db else None
                                    }
                                )
                                break
                author["affiliations"] = affiliations
                aff_units = get_units_affiations(
                    db, author_db, author["affiliations"])
                for aff_unit in aff_units:
                    if aff_unit not in author["affiliations"]:
                        author["affiliations"].append(aff_unit)

        author.pop("external_ids", None)

        if author.get('full_name'):
            valid_authors.append(author)
    entry["authors"] = valid_authors
    entry["author_count"] = len(entry["authors"])

    for author in entry["authors"]:
        for aff in author["affiliations"]:
            # if "types" not in aff:
            #     print(author)
            #     import sys
            #     sys.exit(1)
            if "types" in aff:
                for t in aff["types"]:
                    if t["type"] == "group":
                        if aff not in entry["groups"]:
                            entry["groups"].append(aff)
    # insert in mongo
    code = siiu_reg.get("CODIGO")
    collection.update_one(
        {"external_ids": {"$elemMatch": {"source": "codigo", "id": code}}},
        {"$setOnInsert": entry}, upsert=True,
    )


def process_one(siiu_reg, db, collection, empty_project, es_handler, verbose=0):
    # just inserting at the moment
    if not siiu_reg.get("CODIGO"):
        return
    process_one_insert(siiu_reg, db, collection,
                       empty_project, es_handler, verbose)
