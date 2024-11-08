from kahi_impactu_utils.Utils import doi_processor, lang_poll, split_names
from time import time
from pandas import isna


def parse_ranking_udea(reg, affiliation, empty_work):
    """
    Parse a record from the ranking database into a work entry, using the empty_work as template.

    Parameters:
    ----------
    reg: dict
        A record from the ranking database.
    affiliation: dict
        The affiliation of the author.
    empty_work: dict
        A template for the work entry.
    """
    entry = empty_work.copy()
    entry["updated"] = [{"source": "ranking", "time": int(time())}]
    title = reg["titulo"].strip().replace('""', '')
    if title.count('"') == 1:
        title = title.replace('"', '')
    if title.startswith('"') and title.endswith('"'):
        title = title.replace('"', '')
    lang = lang_poll(reg["titulo"])
    entry["titles"].append(
        {"title": title, "lang": lang, "source": "ranking", "provenance": "ranking"})
    if reg["DOI"]:
        if not isna(reg["DOI"]):
            doi = doi_processor(reg["DOI"])
            if doi:
                entry["doi"] = doi
                entry["external_ids"].append(
                    {"provenance": "ranking", "source": "doi", "id": doi})
    if reg["issn"]:
        if isinstance(reg["issn"], str):
            for issn in reg["issn"].strip().split():
                if "-" not in issn:
                    continue
                issn = issn.strip()
                entry["source"] = {"name": reg["nombre rev o premio"],
                                   "external_ids": [{"provenance": "ranking", "source": "issn", "id": issn}]}
    if not entry["source"]:
        entry["source"] = {
            "name": reg["nombre rev o premio"], "external_ids": []}
    entry["year_published"] = int(reg["año realiz"])
    name = split_names(reg["nombre"])
    aff = {
        "id": affiliation["_id"],
        "name": affiliation["names"][0]["name"],
        "types": affiliation["types"]
    }
    for affname in affiliation["names"]:
        if affname["lang"] == "es":
            aff["name"] = affname["name"]
            break
        elif affname["lang"] == "en":
            aff["name"] = affname["name"]
        elif affname["source"] == "ror":
            aff["name"] = affname["name"]

    author = {
        "external_ids": [{"provenance": "ranking", "source": "Cédula de Ciudadanía", "id": reg["cedula"]}],
        "full_name": name["full_name"],
        "types": [],
        "affiliations": [aff]
    }
    if not author in entry["authors"]:
        entry["authors"].append(author)

    print(len(entry["authors"]))
    entry["external_ids"].append(
        {"provenance": "ranking", "source": "ranking", "id": reg["index"]})
    return entry
