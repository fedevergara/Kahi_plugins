from thefuzz import fuzz
from kahi_impactu_utils.Utils import lang_poll, doi_processor
from time import time
from kahi_impactu_utils.String import text_to_inverted_index


def parse_scholar(
    reg,
    empty_work,
    verbose=0,
    include_title=True,
    include_abstract=True,
    include_authors=True,
):
    """
    Parse a record from the scholar database into a work entry, using the empty_work as template.

    Parameters
    ----------
    reg : dict
        The record to be parsed from scholar.
    empty_work : dict
        A template for the work entry. Structure is defined in the schema.
    verbose : int
        The verbosity level. Default is 0.
    """
    entry = empty_work.copy()
    entry["updated"] = [{"source": "scholar", "time": int(time())}]
    if include_title and reg.get("title"):
        lang = lang_poll(reg["title"])
        entry["titles"].append(
            {"title": reg["title"], "lang": lang, "source": "scholar"}
        )
    if reg.get("year") not in (None, ""):
        year = ""
        try:
            year = int(str(reg["year"]).strip())
        except Exception as e:
            if verbose > 4:
                print(f"""Could not convert year to int in {reg["doi"]}""")
                print(e)
        entry["year_published"] = year
    if reg.get("doi"):
        doi = doi_processor(reg["doi"])
        if doi:
            entry["doi"] = doi
            entry["external_ids"].append(
                {"provenance": "scholar", "source": "doi", "id": doi}
            )
    if reg.get("cid"):
        entry["external_ids"].append(
            {"provenance": "scholar", "source": "scholar", "id": reg["cid"]}
        )
    if include_abstract and reg.get("abstract"):
        abstract = reg["abstract"]
        lang = lang_poll(abstract)
        entry["abstracts"].append(
            {
                "abstract": text_to_inverted_index(abstract),
                "lang": lang,
                "source": "scholar",
                "provenance": "scholar",
            }
        )

    if reg.get("volume"):
        entry["bibliographic_info"]["volume"] = str(reg["volume"]).strip()
    if reg.get("issue"):
        entry["bibliographic_info"]["issue"] = str(reg["issue"]).strip()
    if reg.get("pages"):
        page_value = str(reg["pages"]).strip()
        if "--" in page_value:
            pages = page_value.split("--")
            entry["bibliographic_info"]["start_page"] = pages[0]
            entry["bibliographic_info"]["end_page"] = pages[1]
            if (
                entry["bibliographic_info"]["start_page"].isdigit()
                and entry["bibliographic_info"]["end_page"].isdigit()
            ):
                try:
                    entry["bibliographic_info"]["pages"] = str(
                        int(entry["bibliographic_info"]["end_page"])
                        - int(entry["bibliographic_info"]["start_page"])
                    )
                except Exception as e:
                    if verbose > 4:
                        print(
                            f"""Could not cast pages to substract in {reg["doi"]}""")
                        print(e)
            else:
                if verbose > 4:
                    print(
                        f"Malformed start_page or end_page in source database for {reg['doi']}. "
                        "Setting 'pages' to the original value.")
                entry["bibliographic_info"]["pages"] = page_value
        else:
            if verbose > 4:
                print(
                    f"""Malformed pages in source database for {reg["doi"]}. Inserting anyway""")
            entry["bibliographic_info"]["pages"] = page_value
            entry["bibliographic_info"]["start_page"] = page_value
    if reg.get("bibtex"):
        entry["bibliographic_info"]["bibtex"] = reg["bibtex"]
        typ = reg["bibtex"].split("{")[0].replace("@", "")
        entry["types"].append(
            {"provenance": "scholar", "source": "scholar", "type": typ, "level": None}
        )
    if reg.get("cites") not in (None, ""):
        try:
            cites = int(reg["cites"])
        except (TypeError, ValueError):
            cites = None
        if cites is not None:
            entry["citations_count"].append(
                {"provenance": "scholar", "source": "scholar", "count": cites}
            )
    if reg.get("cites_link"):
        entry["external_urls"].append(
            {
                "provenance": "scholar",
                "source": "scholar citations",
                "url": reg["cites_link"],
            }
        )
    if reg.get("pdf"):
        entry["external_urls"].append(
            {"provenance": "scholar", "source": "pdf", "url": reg["pdf"]}
        )

    entry["source"] = {"name": reg.get("journal") or "", "external_ids": []}

    # authors section
    full_name_list = []
    if include_authors and isinstance(reg.get("author"), str):
        for author in reg["author"].strip().split(" and "):
            if "others" in author:
                continue
            author_entry = {}
            names_list = author.split(", ")
            last_names = ""
            first_names = ""
            if len(names_list) > 0:
                last_names = names_list[0].strip()
            if len(names_list) > 1:
                first_names = names_list[1].strip()
            full_name = first_names + " " + last_names
            author_entry["full_name"] = full_name
            author_entry["affiliations"] = []
            author_entry["external_ids"] = []
            entry["authors"].append(author_entry)
            full_name_list.append(full_name)
    if include_authors and isinstance(reg.get("profiles"), dict):
        if reg["profiles"]:
            for name in reg["profiles"].keys():
                for i, author in enumerate(full_name_list):
                    score = fuzz.ratio(name, author)
                    if score >= 80:
                        entry["authors"][i]["external_ids"] = [
                            {
                                "provenance": "scholar",
                                "source": "scholar",
                                "id": reg["profiles"][name],
                            }
                        ]
                        break
                    elif score > 70:
                        score = fuzz.partial_ratio(name, author)
                        if score >= 90:
                            entry["authors"][i]["external_ids"] = [
                                {
                                    "provenance": "scholar",
                                    "source": "scholar",
                                    "id": reg["profiles"][name],
                                }
                            ]
                            break

    return entry
