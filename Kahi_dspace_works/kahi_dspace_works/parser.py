from time import time
from kahi_impactu_utils.Utils import lang_poll, get_name_connector, doi_processor
from kahi_impactu_utils.String import parse_mathml, parse_html
from kahi_impactu_utils.String import text_to_inverted_index
from kahi_dspace_works.utils import get_dim_fields, get_oai_pmh_url, is_thesis
import unicodedata


def split_name_part(s, connectors=get_name_connector()):
    """
    Split a name part into a list of names or last names.

    Parameters:
    ----------
    s: str
        The name part to split.
    connectors: list
        A list of connectors to split the name part.

    Returns:
    -------
    A list of names or last names.
    """
    s = " ".join(s.strip().split()).title()

    words = s.split()
    last_names = []
    buffer = []
    for word in words:
        if word.upper() in connectors:
            buffer.append(word)
        else:
            if buffer:
                last_names.append(" ".join(buffer + [word]))
                buffer = []
            else:
                last_names.append(word)
    return last_names


def split_names_dspace(s, connectors=get_name_connector()):
    """
    Split a name part into a list of names or last names. In dspace well formed full_name has sure names and names separated by comma.

    Parameters:
    ----------
    s: str
        The full_name to split.
    connectors: list
        A list of connectors to split the full_name.

    Returns:
    -------
    A dictionary with the following keys:
    first_names: list
        A list of names.
    last_names: list
        A list of last names.
    full_name: str
        The full name.
    initials: str
        The initials of the names.

    or an empty dictionary if the full_name is not well formed.
    """
    d = {"first_names": [], "last_names": [], "full_name": "", "initials": ""}
    if (
        "," in s
    ):  # si no hay coma lo desecho, normalmente son nombres de entidades, grupos de investigación o unidades académicas
        c = get_name_connector()
        c = set(c)
        values = s.split(",")
        if len(values) != 2:  # son nombres malos, mal ingresados o entidades raras
            return {}
        surname_part, name_part = values
        if c.intersection(s.upper().split()):
            if c.intersection(surname_part.upper().split()):
                sn = split_name_part(surname_part, list(c))
            else:
                sn = surname_part.split()
            if c.intersection(name_part.upper().split()):
                ns = split_name_part(name_part, list(c))
            else:
                ns = name_part.split()
            d["first_names"] = ns
            d["last_names"] = sn
            d["full_name"] = " ".join(d["first_names"] + d["last_names"])
            d["initials"] = "".join([x[0] for x in d["first_names"]])
            return d
        else:
            d["first_names"] = name_part.split()
            d["last_names"] = surname_part.split()
            d["full_name"] = " ".join(d["first_names"] + d["last_names"])
            d["initials"] = "".join([x[0] for x in d["first_names"]])
            nfkd_form = unicodedata.normalize("NFKD", d["initials"])
            d["initials"] = "".join(
                c for c in nfkd_form if not unicodedata.combining(c)
            )
            return d
    else:
        return {}


def get_dspace_url(reg, base_url):
    """
    Construct the URL of a DSpace item from its OAI-PMH identifier.

    Parameters:
    ----------
    reg: dict
        The OAI-PMH record.
    base_url: str
        The base URL of the DSpace instance.

    Returns:
    -------
    The URL of the DSpace item.
    """
    for field in get_dim_fields(reg):
        if (
            field.get("@element") == "identifier"
            and field.get("@qualifier") == "uri"
            and isinstance(field.get("#text"), str)
            and field["#text"].startswith(("http://", "https://"))
        ):
            return field["#text"]

    oai_id = reg.get("_id", "")
    prefix = "oai:"
    if not oai_id.startswith(prefix):
        raise ValueError("The ID does not follow the expected OAI-PMH format.")

    # Split the ID to extract the item identifier part
    parts = oai_id[len(prefix):].split(":")
    if len(parts) != 2:
        raise ValueError("The OAI-PMH ID has an unexpected format.")

    item_identifier = parts[1]  # Extracts "10893/3527"
    # Construct the final URL
    if not base_url:
        return ""
    return f"{base_url.rstrip('/')}/handle/{item_identifier}"


def get_type_dspace(text: str) -> dict:
    """
    Get the type of a dspace record.

    Parameters:
    ----------
    reg: dict
        The dspace record.

    Returns:
    -------
    The type of the dspace record.
    """
    types = ["redcol", "coar", "eu-repo"]
    for t in types:
        if t in text:
            return {
                "provenance": "dspace",
                "source": t,
                "type": text.split("/")[-1].lower().strip(),
                "level": None,
            }
    rec = {
        "provenance": "dspace",
        "source": "dspace",
        "type": text.lower().strip(),
        "level": None,
    }
    return rec


def parse_dspace(
    reg: dict, empty_work: dict, base_url: str, verbose: int = 0
) -> dict:
    """
    Parse a dspace record into a work entry.

    dspace record is a xml DIM (https://github.com/DSpace/DSpace/blob/main/dspace/config/crosswalks/oai/metadataFormats/dim.xsl)
    in json format using xmltodict.

    Parameters:
    ----------
    reg: dict
        The dspace record to parse.
    empty_work: dict
        An empty work entry to populate.
    base_url: str
        The base URL of the DSpace instance (ex: https://bibliotecadigital.univalle.edu.co).
    affiliation: dict
        The affiliation of the repository. (found in the affiliations collection).
    verbose: int
        A flag to print messages.

    Returns:
    -------
    A populated work entry.
    """
    entry = empty_work.copy()
    entry["updated"] = [{"source": "dspace", "time": int(time())}]
    thesis = False
    for field in get_dim_fields(reg):

        # Title
        if field["@element"] == "title" and "#text" in field:
            lang = lang_poll(field["#text"], verbose=verbose)
            title = parse_mathml(field["#text"])
            title = parse_html(title)
            title = title.strip()
            entry["titles"].append(
                {"title": title, "lang": lang, "source": "dspace"})
        if field["@element"] == "title" and "#text" not in field:
            if "#text" not in field:
                print("WARNING: #text not found in title \n",
                      field, reg["_id"])

        # year
        if (field["@element"] ==
                "date" and "@qualifier" in field and field["@qualifier"] == "issued"):
            if "#text" not in field:
                print("WARNING: #text not found in date \n",
                      field, reg["_id"])
            if "#text" in field and field["#text"] and len(
                    field["#text"]) >= 4:
                if field["#text"][:4].isdigit():
                    if entry["year_published"]:
                        if entry["year_published"] >= int(field["#text"][:4]):
                            entry["year_published"] = int(field["#text"][:4])
                    else:
                        entry["year_published"] = int(field["#text"][:4])
        # Abstract
        if (field["@element"] ==
                "description" and "@qualifier" in field and field["@qualifier"] == "abstract"):
            if "#text" in field:
                abstract = field["#text"]
                abstract_lang = lang_poll(abstract, verbose=verbose)
                entry["abstracts"].append(
                    {
                        "abstract": text_to_inverted_index(abstract),
                        "lang": abstract_lang,
                        "source": "dspace",
                        "provenance": "dspace",
                    }
                )
            else:
                print("WARNING: #text not found in abstract \n",
                      field, reg["_id"])

        # Authors
        if (field["@element"] == "contributor" and "@qualifier" in field and field["@qualifier"]
                in ["author", "advisor"] and "#text" in field):
            if len(field["#text"].split(",")) != 2:
                continue
            author = split_names_dspace(
                field["#text"].replace(".", "").replace("-", " "))
            if author:
                author["id"] = ""
                author["affiliations"] = []
                author["type"] = field["@qualifier"]
                entry["authors"].append(author)
        # Type
        if field["@element"] == "type":
            if "#text" not in field:
                print("WARNING: #text not found in type \n",
                      field, reg["_id"])
            else:
                if is_thesis(field["#text"]):
                    thesis = True
                entry["types"].append(get_type_dspace(field["#text"]))
        # Rights
        if field["@element"] == "rights":
            if "#text" in field:
                entry["rights"].append(
                    {
                        "provenance": "dspace",
                        "source": "dspace",
                        "rights": field["#text"],
                    }
                )

        # ids
        if field["@element"] == "identifier" and "@qualifier" in field:

            if field["@qualifier"] == "doi":
                if "#text" not in field:
                    print("WARNING: #text not found in doi \n",
                          field, reg["_id"])
                else:
                    doi = doi_processor(field["#text"])
                    if doi:
                        if entry["doi"] and entry["doi"] != doi:
                            print(
                                "WARNING:dspace_works: doi already assigned and it is different, leaving it as it is."
                            )
                            print(
                                f"WARNING: dspace_works: {reg['_id']} with doi {entry['doi']} and doi {doi}")
                        else:
                            entry["doi"] = doi
                        entry["external_ids"].append(
                            {"provenance": "dspace", "source": "doi", "id": doi}
                        )

            if field["@qualifier"] in ["ismn", "uri", "other"]:
                if "#text" not in field:
                    print("WARNING: #text not found in external_ids \n",
                          field, reg["_id"])
                else:
                    entry["external_ids"].append(
                        {
                            "provenance": "dspace",
                            "source": field["@qualifier"],
                            "id": field["#text"],
                        }
                    )

            if field["@qualifier"] == "url":
                if "#text" not in field:
                    print("WARNING: #text not found in url \n",
                          field, reg["_id"])
                else:
                    entry["external_urls"].append(
                        {"provenance": "dspace", "source": "url",
                            "url": field["#text"]}
                    )
            if field["@qualifier"] in [
                "isbn",
                "issn",
                "eissn",
            ]:  # esto es para los sources
                if "#text" not in field:
                    print("WARNING: #text not found in external_ids \n",
                          field, reg["_id"])
                else:
                    entry["source"].setdefault("external_ids", []).append(
                        {
                            "provenance": "dspace",
                            "source": field["@qualifier"],
                            "id": field["#text"],
                        }
                    )

    # ids
    entry["external_ids"].append(
        {"provenance": "dspace", "source": "dspace", "id": reg["_id"]}
    )
    public_url = get_dspace_url(reg, base_url)
    if public_url:
        entry["external_urls"].append(
            {"provenance": "dspace", "source": "dspace", "url": public_url}
        )
    oai_pmh_url = get_oai_pmh_url(reg)
    if oai_pmh_url:
        entry["external_urls"].append(
            {"provenance": "dspace", "source": "oaipmh", "url": oai_pmh_url}
        )
    entry["thesis"] = thesis
    entry["author_count"] = len(entry["authors"])
    return entry
