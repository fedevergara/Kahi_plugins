from kahi_impactu_utils.Utils import lang_poll, doi_processor, check_date_format
from time import time
import re
from copy import deepcopy
from kahi_impactu_utils.String import text_to_inverted_index


def parse_scienti(reg, empty_work, doi=None, verbose=0):
    """
    Parse a record from the scienti database into a work entry, using the empty_work as template.

    Parameters
    ----------
    reg : dict
        The record to be parsed from scienti.
    empty_work : dict
        A template for the work entry. Structure is defined in the schema.
    verbose : int
        The verbosity level. Default is 0.
    """
    entry = deepcopy(empty_work)
    entry["updated"] = [{"source": "scienti", "time": int(time())}]
    title = reg["TXT_NME_PROD"].strip().replace("\t", "").replace('"', '')
    lang = lang_poll(title, verbose=verbose)
    entry["titles"].append(
        {"title": title, "lang": lang, "source": "scienti"})

    if reg["TXT_RESUMEN_PROD"]:
        abstract = reg["TXT_RESUMEN_PROD"].replace("\x00", "")
        lang = lang_poll(abstract, verbose=verbose)
        entry["abstracts"].append({"abstract": text_to_inverted_index(
            abstract), "lang": lang, "source": "scienti", 'provenance': 'scienti'})
    ext_ids_reg = {"provenance": "scienti", "source": "scienti", "id": {
        "COD_RH": reg["COD_RH"], "COD_PRODUCTO": reg["COD_PRODUCTO"]}}
    if ext_ids_reg not in entry["external_ids"]:
        entry["external_ids"].append(ext_ids_reg)
    if doi:
        entry["doi"] = doi
        entry["external_ids"].append(
            {"provenance": "scienti", "source": "doi", "id": doi})
    else:
        if "TXT_DOI" in reg.keys():
            if reg["TXT_DOI"]:
                doi = doi_processor(reg["TXT_DOI"])
                if doi:
                    entry["doi"] = doi
                    entry["external_ids"].append(
                        {"provenance": "scienti", "source": "doi", "id": doi})
            else:
                if "TXT_WEB_PRODUCTO" in reg.keys(
                ) and reg["TXT_WEB_PRODUCTO"] and "10." in reg["TXT_WEB_PRODUCTO"]:
                    doi = doi_processor(reg["TXT_WEB_PRODUCTO"])
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
                    entry["doi"] = doi
                    entry["external_ids"].append(
                        {"provenance": "scienti", "source": "doi", "id": doi})
    if "TXT_WEB_PRODUCTO" in reg.keys():
        entry["external_urls"].append(
            {"provenance": "scienti", "source": "scienti", "url": reg["TXT_WEB_PRODUCTO"]})
    year = None
    month = None
    if "NRO_ANO_PRESENTA" in reg.keys():
        year = reg["NRO_ANO_PRESENTA"]
    if "NRO_MES_PRESENTA" in reg.keys():
        month = reg["NRO_MES_PRESENTA"]
        if len(str(month)) == 1:
            month = f'0{month}'
    if year and month:
        entry["date_published"] = check_date_format(
            f'{month}-{year}')
        entry["year_published"] = int(year)
    if "SGL_CATEGORIA" in reg.keys() and reg["SGL_CATEGORIA"]:
        ranking_reg = {
            "provenance": "scienti",
            "date": "",
            "rank": reg["SGL_CATEGORIA"],
            "source": "scienti"}
        if ranking_reg not in entry["ranking"]:
            entry["ranking"].append(ranking_reg)
    # types section
    tpo_obj = reg["product_type"][0]
    for _ in range(0, 4):
        tpo_base = {"provenance": "scienti", "source": "scienti"}
        tpo_name = tpo_obj["TXT_NME_TIPO_PRODUCTO"]
        level = tpo_obj["NRO_NIVEL"]
        tpo_class = tpo_obj["TPO_CLASE"]
        code = tpo_obj["COD_TIPO_PRODUCTO"]
        tpo_base["type"] = tpo_name
        tpo_base["level"] = level
        tpo_base["class"] = tpo_class
        tpo_base["code"] = code
        if tpo_base not in entry["types"]:
            entry["types"].append(tpo_base)
        if "product_type" in tpo_obj.keys():
            tpo_obj = tpo_obj["product_type"][0]
        else:
            break

    # details only for articles
    if "details" in reg.keys() and len(
            reg["details"]) > 0 and "article" in reg["details"][0].keys():
        details = reg["details"][0]["article"][0]
        try:
            if "TXT_PAGINA_INICIAL" in details.keys():
                entry["bibliographic_info"]["start_page"] = details["TXT_PAGINA_INICIAL"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing start page on RH: {
                        reg["COD_RH"]} and COD_PROD: {
                        reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_PAGINA_FINAL" in details.keys():
                entry["bibliographic_info"]["end_page"] = details["TXT_PAGINA_FINAL"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing end page on RH: {
                        reg["COD_RH"]} and COD_PROD: {
                        reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_VOLUMEN_REVISTA" in details.keys():
                entry["bibliographic_info"]["volume"] = details["TXT_VOLUMEN_REVISTA"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing volume on RH: {
                        reg["COD_RH"]} and COD_PROD: {
                        reg["COD_PRODUCTO"]}')
                print(e)
        try:
            if "TXT_FASCICULO_REVISTA" in details.keys():
                entry["bibliographic_info"]["issue"] = details["TXT_FASCICULO_REVISTA"]
        except Exception as e:
            if verbose > 4:
                print(
                    f'Error parsing issue on RH: {
                        reg["COD_RH"]} and COD_PROD: {
                        reg["COD_PRODUCTO"]}')
                print(e)
    elif "details" in reg.keys() and len(reg["details"]) > 0 and "oriented_thesis" in reg["details"][0].keys():
        details = None
        details = reg["details"][0]["oriented_thesis"][0]
        if details and details["NRO_PAGINAS"]:
            entry["bibliographic_info"]["start_page"] = "1"
            entry["bibliographic_info"]["end_page"] = str(
                details["NRO_PAGINAS"])

        # source section
        source = {"external_ids": [], "title": ""}
        if "journal" in details.keys():
            journal = details["journal"][0]
            source["title"] = journal["TXT_NME_REVISTA"]
            if "TXT_ISSN_REF_SEP" in journal.keys():
                source["external_ids"].append(
                    {"provenance": "scienti", "source": "issn", "id": journal["TXT_ISSN_REF_SEP"]})
            if "COD_REVISTA" in journal.keys():
                source["external_ids"].append(
                    {"provenance": "scienti", "source": "scienti", "id": journal["COD_REVISTA"]})
        elif "journal_others" in details.keys():
            journal = details["journal_others"][0]
            source["title"] = journal["TXT_NME_REVISTA"]
            if "TXT_ISSN_REF_SEP" in journal.keys():
                source["external_ids"].append(
                    {"provenance": "scienti", "source": "issn", "id": journal["TXT_ISSN_REF_SEP"]})
            if "COD_REVISTA" in journal.keys():
                source["external_ids"].append(
                    {"provenance": "scienti", "source": "scienti", "id": journal["COD_REVISTA"]})

        entry["source"] = source

    # authors section
    affiliations = []
    if "group" in reg.keys():
        group = reg["group"][0]
        affiliations.append({
            "external_ids": [{"provenance": "scienti", "source": "scienti", "id": group["COD_ID_GRUPO"]}],
            "name": group["NME_GRUPO"]
        })
        if "institution" in group.keys():
            inst = group["institution"][0]
            affiliations.append({
                "external_ids": [{"provenance": "scienti", "source": "scienti", "id": inst["COD_INST"]}],
                "name": inst["NME_INST"]
            })

    # Minimal author entry to search the author in the database
    # affiliations are added to the author entry for this paper
    # external_ids are added to the author entry for searching purposes
    author = reg["author"][0]
    author_entry = {
        "full_name": re.sub(
            r'\s+',
            ' ',
            author["TXT_TOTAL_NAMES"].strip()),
        "affiliations": affiliations,
        "external_ids": [
            {
                "provenance": "scienti",
                "source": "scienti",
                "id": {
                    "COD_RH": author["COD_RH"]}}]}
    if author["TPO_DOCUMENTO_IDENT"] == "P":
        author_entry["external_ids"].append(
            {"provenance": "scienti", "source": "Passport", "id": author["NRO_DOCUMENTO_IDENT"]})
    if author["TPO_DOCUMENTO_IDENT"] == "C":
        author_entry["external_ids"].append(
            {
                "provenance": "scienti",
                "source": "Cédula de Ciudadanía",
                "id": author["NRO_DOCUMENTO_IDENT"]})
    if author["TPO_DOCUMENTO_IDENT"] == "E":
        author_entry["external_ids"].append(
            {
                "provenance": "scienti",
                "source": "Cédula de Extranjería",
                "id": author["NRO_DOCUMENTO_IDENT"]})
    entry["authors"] = [author_entry]
    return entry
