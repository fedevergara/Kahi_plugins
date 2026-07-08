from kahi_impactu_utils.Utils import doi_processor, lang_poll
from time import time
import copy
from iso639 import is_valid639_1


def parse_ciarp(reg, affiliation, empty_work):
    """
    Parse a record from the ciarp database into a work entry, using the empty_work as template.

    Parameters:
    ----------
    reg: dict
        A record from the ciarp database.
    affiliation: dict
        The affiliation of the author.
    empty_work: dict
        A template for the work entry.
    """
    entry = copy.deepcopy(empty_work)

    entry["updated"] = [{"source": "ciarp", "time": int(time())}]
    title = reg["título"].strip().replace('""', '')
    if title.count('"') == 1:
        title = title.replace('"', '')
    if title.startswith('"') and title.endswith('"'):
        title = title.replace('"', '')
    if title.startswith("'") and title.endswith("'"):
        title = title.replace("'", "")
    lang = None
    if reg["idioma"]:
        if is_valid639_1(reg["idioma"]):
            lang = reg["idioma"]
    if not lang:
        lang = lang_poll(reg["título"])
    entry["titles"].append(
        {"title": title, "lang": lang, "source": "ciarp", "provenance": "ciarp"})
    if reg["doi"]:
        doi = doi_processor(reg["doi"])
        if doi:
            entry["doi"] = doi
            entry["external_ids"].append(
                {"provenance": "ciarp", "source": "doi", "id": doi})
    if reg["issn"]:
        for issn in reg["issn"].strip().split():
            if "-" not in issn:
                continue
            issn = issn.strip()
            entry["source"] = {"name": reg["revista"],
                               "external_ids": [{"provenance": "ciarp", "source": "issn", "id": issn}]}
    if reg["isbn"]:
        isbn = {"provenance": "ciarp",
                "source": "isbn", "id": reg["isbn"].strip()}
        if isbn not in entry["external_ids"]:
            entry["external_ids"].append(isbn)
    if not entry["source"]:
        entry["source"] = {
            "name": reg["revista"], "external_ids": []}
    try:
        entry["year_published"] = int(reg["año"])
    except (TypeError, ValueError):
        entry["year_published"] = None
    if reg["volumen"]:
        entry["bibliographic_info"]["volume"] = reg["volumen"]
    if reg["issue"]:
        entry["bibliographic_info"]["issue"] = reg["issue"]
    if reg["primera_página"]:
        entry["bibliographic_info"]["start_page"] = reg["primera_página"]
    if reg["última_página"]:
        entry["bibliographic_info"]["end_page"] = reg["última_página"]
    if reg["ranking"]:
        entry["types"].append(
            {"provenance": "ciarp", "source": "ciarp", "type": reg["ranking"]})
        entry["ranking"].append(
            {"provenance": "ciarp", "source": "ciarp", "date": "", "rank": reg["ranking"]})

    aff = {
        "id": affiliation["_id"],
        "external_ids": [{"provenance": "ciarp", "source": "ciarp", "id": affiliation["_id"]}],
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
        "id": "",
        "external_ids": [{"provenance": "ciarp", "source": "Cédula de Ciudadanía", "id": reg["identificación"]}],
        "full_name": "",
        "affiliations": [aff]
    }
    if reg["código_unidad_académica"]:
        unit = {
            "external_ids": [{"provenance": "ciarp", "source": "ciarp", "id": f'{affiliation["_id"]}_{reg["código_unidad_académica"]}'}],
            "name": "",
            "types": [{"type": "faculty"}]
        }
        if unit not in author["affiliations"]:
            author["affiliations"].append(unit)
    if reg["código_subunidad_académica"]:
        subunit = {
            "external_ids": [{"provenance": "ciarp", "source": "ciarp", "id": f'{affiliation["_id"]}_{reg["código_unidad_académica"]}_{reg["código_subunidad_académica"]}'}],
            "name": "",
            "types": [{"type": "department"}]
        }
        if subunit not in author["affiliations"]:
            author["affiliations"].append(subunit)

    if author not in entry["authors"]:
        entry["authors"].append(author)

    entry["external_ids"].append(
        {"provenance": "ciarp", "source": "ciarp", "id": reg["index"]})
    if reg.get("source_record_id"):
        entry["external_ids"].append({
            "provenance": "ciarp",
            "source": "ciarp_record",
            "id": str(reg["source_record_id"]),
        })
    return entry
