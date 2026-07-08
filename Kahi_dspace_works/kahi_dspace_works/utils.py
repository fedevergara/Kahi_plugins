from kahi_impactu_utils.Utils import doi_processor
from thefuzz import process, fuzz
from unidecode import unidecode


def get_dim_fields(reg):
    """Return DIM fields as a list, tolerating singleton or malformed metadata."""
    try:
        fields = reg["OAI-PMH"]["GetRecord"]["record"]["metadata"]["dim:dim"][
            "dim:field"
        ]
    except (KeyError, TypeError):
        return []
    if isinstance(fields, dict):
        return [fields]
    if isinstance(fields, list):
        return [field for field in fields if isinstance(field, dict)]
    return []


def get_doi(reg):
    """
    helps to get the doi from the dspace record and process it with the doi_processor.

    Parameters
    ----------
    reg : dict
        dspace record.

    Returns
    -------
    str | None
        doi processed or None if there is no doi or it is invalid.
    """
    for field in get_dim_fields(reg):
        if "#text" not in field:
            continue
        if field["@element"] == "identifier" and "@qualifier" in field:
            if field["@qualifier"] == "doi":
                doi = doi_processor(field["#text"])
                return doi
    return None


def set_source(reg, db):
    """
    Given the issn, eissn or other external ids of the source in the dspace record, it tries to find the source in the sources collection.

    If the source is found, it creates a dict to put the source id and name in the parsed entry.
    It also deletes the external ids from the source dict in the parsed entry.

    Parameters
    ----------
    reg : dict
        parsed dspace record.
    db : pymongo.database.Database
        database object to kahi(ETL) database.
    """
    if reg["source"] != {}:
        for source in reg["source"]["external_ids"]:
            found = db["sources"].find_one({"external_ids.id": source["id"]})
            if found:
                del reg["source"]["external_ids"]
                reg["source"]["id"] = found["_id"]
                reg["source"]["name"] = found["names"][0]["name"]
                return
        del reg["source"]["external_ids"]


def process_affiliation(ror_id, db):
    """
    helps to get the affiliation from the ror_id and process it.

    Parameters
    ----------
    ror_id : str
        ror id.
    db : pymongo.database.Database
        database object to kahi(ETL) database.

    Returns
    -------
    dict | None
        affiliation processed or None if affiliation is not found.
    """
    aff_rec = db["affiliations"].find_one({"external_ids.id": ror_id})
    if aff_rec is None:
        return None
    aff = {}
    aff["id"] = aff_rec["_id"]
    aff["name"] = next((i["name"]
                       for i in aff_rec["names"] if i["lang"] == "es"), "")
    aff["types"] = aff_rec["types"]
    if aff_rec["addresses"]:
        if "country" in aff_rec["addresses"][0]:
            aff["country"] = aff_rec["addresses"][0]["country"]
        if "country_code" in aff_rec["addresses"][0]:
            aff["country_code"] = aff_rec["addresses"][0]["country_code"]
    return aff


def str_normilize(word):
    """
    Normalize a string to lowercase and remove accents.

    Parameters
    ----------
    word : str
        string to be normalized.

    Returns
    -------
    str
        normalized string.
    """
    return unidecode(word).lower().strip().replace(".", "")


def check_work(title_work, authors, response, thresholds):
    """
    Check if the title and authors of the dspace work are similar to the title and authors of the elasticsearch work.

    Parameters
    ----------
    title_work : str
        title of the dspace work.
    authors : list
        authors of the dspace work.
    response : dict
        elasticsearch work.
    thresholds : dict
        thresholds to consider a work as similar.

    Returns
    -------
    bool
        True if the works are similar, False otherwise.
    """
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


thesis_types = ['http://purl.org/coar/resource_type/c_46ec',
                'http://purl.org/coar/resource_type/c_bdcc',
                'http://purl.org/coar/resource_type/c_db06',
                'http://purl.org/coar/resource_type/c_7a1f',
                'https://purl.org/redcol/resource_type/TP',
                'https://purl.org/redcol/resource_type/TM',
                'http://purl.org/redcol/resource_type/TD_A',
                'https://purl.org/redcol/resource_type/TD',
                'http://purl.org/redcol/resource_type/TP_A',
                'hnfo:eu-repo/semantics/bachelorthesis',
                'info:eu-repo/semantics/ masterthesis',
                'info:eu-repo/semantics/bachelorthesis',
                'info:eu-repo/semantics/bachelorthesis pregrado',
                'info:eu-repo/semantics/bachelorthesisd',
                'info:eu-repo/semantics/bachelorthesist',
                'info:eu-repo/semantics/bachelorthesistab',
                'info:eu-repo/semantics/doctoralthesis',
                'info:eu-repo/semantics/master thesis',
                'info:eu-repo/semantics/masterthesis',
                'info:eu-repo/semantics/masterthesisc',
                'info:eu-repo/semantics/masterthesisd',
                'info:eu-repo/semantics/masterthesist',
                'info:eu-repo/semantics/masterthesistab',
                'info:eu-repo/semantics/mastherthesis']


def is_thesis(work_type):
    """
    Check if the work type is a thesis.

    Parameters
    ----------
    work_type : str
        work type.

    Returns
    -------
    bool
        True if the work type is a thesis, False otherwise.
    """
    if work_type in thesis_types:
        return True
    if "tesis" in work_type.lower():
        return True
    if "thesis" in work_type.lower():
        return True
    if "trabajo de grado" in work_type.lower():
        return True
    if "monografia" in str_normilize(work_type.lower()):
        return True
    return False


def is_thesis_work(reg):
    """
    Check if the dspace record is a valid product to be processed.

    Parameters
    ----------
    reg : dict
        dspace record.

    Returns
    -------
    bool
        True if the dspace record is a valid product to be processed, False otherwise.
    """
    for field in get_dim_fields(reg):
        if field["@element"] == "type":
            if "#text" not in field:
                continue
            if is_thesis(field["#text"]):
                return True
    return False


def get_oai_pmh_url(reg):
    """
    Return the oai-pmh url of the dspace record.

    Parameters
    ----------
    reg : dict
        dspace record.

    Returns
    -------
    str
        oai-pmh url.
    """
    try:
        request = reg["OAI-PMH"]["request"]
        base_url = request["#text"]
        verb = request["@verb"]
        metadataPrefix = request["@metadataPrefix"]
        identifier = request["@identifier"]
    except (KeyError, TypeError):
        return ""

    url = f"{base_url}?verb={verb}&metadataPrefix={metadataPrefix}&identifier={identifier}"
    return url


def is_similarity_reg(reg):
    """
    Check if the dspace record has not doi but if it a thesis with doi it is valid.
    We are processing the thesis with doi with elasticsearch.

    Parameters
    ----------
    reg : dict
        dspace record.

    Returns
    -------
    bool
        True if the dspace record has a doi, False otherwise.
    """
    if get_doi(reg) and is_thesis_work(reg):
        return True
    elif not get_doi(reg):
        return True
    return False


def set_affiliation(entry, affiliation, collection):
    """
    Set the affiliation of the authors in the entry.
    if the entry is a thesis, the affiliation is set to the authors.
    if the entry is not a thesis, the affiliation is set to the authors that are in the database.
    if the number of authors is one, the affiliation is set to the author.

    read https://github.com/colav/impactu/issues/385

    Parameters
    ----------
    entry : dict
        entry to set the affiliation.
    thesis : bool
        True if the entry is a thesis, False otherwise.
    affiliation : dict
        affiliation to set.
    collection : pymongo.collection.Collection
        collection object to person in kahi(ETL) database.
    """
    if entry["thesis"]:  # only thesis have affiliation
        for author in entry["authors"]:
            author["affiliations"].append(affiliation)
        return

    if len(entry["authors"]) == 1:  # with only one author, I can assume the affiliation
        if affiliation not in entry["authors"][0]["affiliations"]:
            entry["authors"][0]["affiliations"].append(affiliation)
        return
    for author in entry["authors"]:
        author_found = collection.find_one({"full_name": author["full_name"]})
        if author_found:
            if len(author_found["affiliations"]) == 0:
                author["affiliations"].append(affiliation)
                continue
            for aff in author_found["affiliations"]:
                if aff["id"] == affiliation["id"]:
                    author["affiliations"].append(affiliation)
                    break


def title_similarity(titles, titles_kahi, threshold=60):
    """
    Check if the titles are similar enough to be considered the same.

    Parameters
    ----------
    titles : dict
        incoming titles from a source db sto be compared with work kahi ex:
        [{'title': 'Jóvenes: cuerpos, calles y movimiento',
           'lang': 'es',
           'source': 'openalex'},
         {'title': 'Jóvenes: cuerpos, calles y movimiento',
           'lang': 'es',
           'source': 'scholar'}]
    titles_kahi : dict
        titles from kahi.
    threshold : int
        threshold to consider the titles as similar.

    Returns
    -------
    bool
        True if the titles are similar, False otherwise.
    """
    for dtitle in titles:
        for ktitle in titles_kahi:
            score = fuzz.ratio(
                str_normilize(dtitle["title"]), str_normilize(ktitle["title"])
            )
            if score >= threshold:
                return True
    return False
