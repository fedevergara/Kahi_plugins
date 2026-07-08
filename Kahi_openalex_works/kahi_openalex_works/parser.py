from kahi_impactu_utils.Utils import lang_poll
from kahi_impactu_utils.String import parse_mathml, parse_html
from time import time
from datetime import datetime as dt
from kahi_impactu_utils.String import inverted_index_to_text, text_to_inverted_index


def parse_openalex(reg, empty_work, verbose=0):
    """
    Parse a record from openalex to a work entry, using the empty_work template

    Parameters
    ----------
    reg : dict
        A record from openalex
    empty_work : dict
        A template for a work entry, with empty fields.
    verbose : int, optional
        Verbosity level. The default is 0.
    """
    entry = empty_work.copy()
    entry["updated"] = [{"source": "openalex", "time": int(time())}]
    if reg["title"]:
        lang = lang_poll(reg["title"], verbose=verbose)
        title = parse_mathml(reg["title"])
        title = parse_html(title)
        title = title.strip()
        entry["titles"].append(
            {"title": title, "lang": lang, "source": "openalex"})
    for source, idx in reg["ids"].items():
        entry["external_ids"].append(
            {"provenance": "openalex", "source": source, "id": idx})
    entry["doi"] = reg["doi"]
    entry["year_published"] = reg["publication_year"]
    publication_date = reg.get("publication_date")
    if publication_date:
        try:
            entry["date_published"] = int(dt.strptime(
                publication_date, "%Y-%m-%d").timestamp())
        except (TypeError, ValueError):
            if verbose > 1:
                print(
                    f"WARNING: invalid publication_date {publication_date!r} "
                    f"for work {reg.get('id')}"
                )
    entry["types"].append({"provenance": "openalex",
                           "source": "openalex",
                           "type": reg["type"],
                           "level": None})
    if "type_crossref" in reg.keys():
        entry["types"].append({"provenance": "openalex",
                               "source": "crossref",
                               "type": reg["type_crossref"],
                               "level": None})

    entry["citations_by_year"] = reg["counts_by_year"]

    if reg["primary_location"] and reg["primary_location"]['source']:
        entry["source"] = {
            "name": reg["primary_location"]['source']["display_name"],
            "external_ids": [{"source": "openalex", "id": reg["primary_location"]['source']["id"]}]
        }

        if "issn_l" in reg["primary_location"]['source'].keys():
            if reg["primary_location"]['source']["issn_l"]:
                entry["source"]["external_ids"].append(
                    {"source": "issn_l", "id": reg["primary_location"]['source']["issn_l"]})

        if "issn" in reg["primary_location"]['source'].keys():
            if reg["primary_location"]['source']["issn"]:
                entry["source"]["external_ids"].append(
                    {"source": "issn", "id": reg["primary_location"]['source']["issn"][0]})

    entry["citations_count"].append(
        {"source": "openalex", "count": reg["cited_by_count"]})

    if "volume" in reg["biblio"]:
        if reg["biblio"]["volume"]:
            entry["bibliographic_info"]["volume"] = reg["biblio"]["volume"]
    if "issue" in reg["biblio"]:
        if reg["biblio"]["issue"]:
            entry["bibliographic_info"]["issue"] = reg["biblio"]["issue"]
    if "first_page" in reg["biblio"]:
        if reg["biblio"]["first_page"]:
            entry["bibliographic_info"]["start_page"] = reg["biblio"]["first_page"]
    if "last_page" in reg["biblio"]:
        if reg["biblio"]["last_page"]:
            entry["bibliographic_info"]["end_page"] = reg["biblio"]["last_page"]
    if "open_access" in reg.keys():
        if "is_oa" in reg["open_access"].keys():
            entry["open_access"]["is_open_access"] = reg["open_access"]["is_oa"]
        if "oa_status" in reg["open_access"].keys():
            entry["open_access"]["open_access_status"] = reg["open_access"]["oa_status"]
        if "oa_url" in reg["open_access"].keys():
            entry["open_access"]["url"] = reg["open_access"]["oa_url"]
        if "any_repository_has_fulltext" in reg["open_access"].keys():
            entry["open_access"]["has_repository_fulltext"] = reg["open_access"]["any_repository_has_fulltext"]
        if "oa_url" in reg["open_access"].keys():
            if reg["open_access"]["oa_url"]:
                entry["external_urls"].append(
                    {"provenance": "openalex", "source": "open_access", "url": reg["open_access"]["oa_url"]})
    if "apc_paid" in reg.keys():
        if reg["apc_paid"]:
            entry["apc"]["paid"] = {
                "value": reg["apc_paid"]["value"],
                "currency": reg["apc_paid"]["currency"],
                "value_usd": reg["apc_paid"]["value_usd"],
                "provenance": "openalex"}
    if "apc_list" in reg.keys():
        if reg["apc_list"]:
            entry["apc"]["list"] = {
                "value": reg["apc_list"]["value"],
                "currency": reg["apc_list"]["currency"],
                "value_usd": reg["apc_list"]["value_usd"],
                "provenance": "openalex"}
    if "abstract_inverted_index" in reg.keys():
        if reg["abstract_inverted_index"]:
            abstract = inverted_index_to_text(reg["abstract_inverted_index"])
            abstract_lang = lang_poll(abstract, verbose=verbose)
            entry["abstracts"].append({"abstract": text_to_inverted_index(
                abstract), "lang": abstract_lang, "source": "openalex", 'provenance': 'openalex'})

    # authors section
    for author in reg["authorships"]:
        if not author["author"]:
            continue
        affs = []
        for inst in author["institutions"]:
            if inst:
                aff_entry = {
                    "external_ids": [{"source": "openalex", "id": inst["id"]}],
                    "name": inst["display_name"]
                }
                if "ror" in inst.keys():
                    aff_entry["external_ids"].append(
                        {"source": "ror", "id": inst["ror"]})
                affs.append(aff_entry)
        author = author["author"]
        author_entry = {
            "external_ids": [],
            "full_name": author["display_name"],
            "types": [],
            "affiliations": affs
        }
        if author["id"]:
            author_entry["external_ids"].append(
                {"source": "openalex", "id": author["id"]})
        if author["orcid"]:
            orcid = author["orcid"]
            if not orcid.startswith("https://orcid.org/"):
                orcid = f"https: //orcid.org/{orcid}"
            author_entry["external_ids"].append(
                {"source": "orcid", "id": orcid})
        entry["authors"].append(author_entry)
    # concepts section
    subjects = []
    for concept in reg["concepts"]:
        sub_entry = {
            "external_ids": [{"source": "openalex", "id": concept["id"]}],
            "name": concept["display_name"],
            "level": concept["level"]
        }
        subjects.append(sub_entry)
    entry["subjects"].append({"source": "openalex", "subjects": subjects})

    # topics section
    entry["primary_topic"] = reg["primary_topic"]
    entry["topics"] = reg["topics"]
    return entry
