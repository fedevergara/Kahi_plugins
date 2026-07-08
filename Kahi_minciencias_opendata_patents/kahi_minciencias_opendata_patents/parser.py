from kahi_impactu_utils.Utils import lang_poll, check_date_format
from time import time
from re import search


def parse_minciencias_opendata(reg, empty_work, verbose=0):
    """
    Parse a record from the minciencias opendata database into a work entry, using the empty_work as template.

    Parameters
    ----------
    reg : dict
        The record to be parsed from minciencias opendata.
    empty_work : dict
        A template for the work entry. Structure is defined in the schema.
    verbose : int
        The verbosity level. Default is 0.
    """
    entry = empty_work.copy()
    entry["updated"] = [{"source": "minciencias", "time": int(time())}]
    title = reg.get("nme_producto_pd")
    if not isinstance(title, str) or not title.strip():
        return None
    lang = lang_poll(title, verbose=verbose)
    entry["titles"].append(
        {"title": title.strip(), "lang": lang, "source": "minciencias"})
    if "id_producto_pd" in reg.keys():
        if reg["id_producto_pd"]:
            entry["external_ids"].append(
                {"provenance": "minciencias", "source": "minciencias", "id": reg["id_producto_pd"]})
            COD_RH = ""
            COD_PRODUCTO = ""
            COD_PATENTE = ""
            product_id = reg["id_producto_pd"]
            match = search(r'(\d{9,11})-(\d{1,7})-(\d{1,7})$', product_id)
            if match:
                COD_RH = match.group(1)
                COD_PRODUCTO = match.group(2)
                COD_PATENTE = match.group(3)
                if COD_RH and COD_PATENTE and COD_PRODUCTO:
                    entry["external_ids"].append(
                        {"provenance": "minciencias", "source": "scienti", "id": {"COD_RH": COD_RH, "COD_PRODUCTO": COD_PRODUCTO, "COD_PATENTE": int(COD_PATENTE)}})
    if "id_tipo_pd_med" in reg.keys():
        date = ""
        if "ano_convo" in reg.keys():
            if reg["ano_convo"]:
                date = check_date_format(reg["ano_convo"])
        if reg["id_tipo_pd_med"]:
            entry["ranking"].append(
                {"provenance": "minciencias", "date": date, "rank": reg["id_tipo_pd_med"], "source": "minciencias"})
    if "nme_tipologia_pd" in reg.keys():
        if reg["nme_tipologia_pd"]:
            typ = reg["nme_tipologia_pd"]
            entry["types"].append(
                {"provenance": "minciencias", "source": "minciencias", "type": typ, "level": 1, "parent": reg.get("nme_clase_pd")})
    if "nme_clase_pd" in reg.keys():
        if reg["nme_clase_pd"]:
            typ = reg["nme_clase_pd"]
            entry["types"].append(
                {"provenance": "minciencias", "source": "minciencias", "type": typ, "level": 0, "parent": None
                 })
    minciencias_id = reg.get("id_persona_pd")
    if minciencias_id:
        affiliation = []
        group_name = ""
        if reg.get("cod_grupo_gr"):
            if reg.get("nme_grupo_gr"):
                group_name = reg["nme_grupo_gr"]
            affiliation.append(
                {
                    "external_ids": [{"provenance": "minciencias", "source": "minciencias", "id": reg["cod_grupo_gr"]}],
                    "name": group_name
                }
            )
        author_entry = {
            "full_name": "",
            "affiliations": [affiliation[0]] if affiliation else [],
            "external_ids": [{"provenance": "minciencias", "source": "scienti", "id": {"COD_RH": minciencias_id}}]
        }
        entry["authors"] = [author_entry]
    return entry
