from kahi_impactu_utils.Utils import lang_poll
from time import time


def parse_siiu(reg, empty_project, verbose=0):
    """
    Parse a record from the SIIU database into a project entry, using the empty_project as template.

    Parameters
    ----------
    reg : dict
        The record to be parsed from siiu
    empty_project : dict
        A template for the work entry. Structure is defined in the schema.
    verbose : int
        The verbosity level. Default is 0.
    """
    entry = empty_project.copy()
    entry["updated"] = [{"source": "siiu", "time": int(time())}]
    title = reg.get("NOMBRE_COMPLETO")
    code = reg.get("CODIGO")
    if not isinstance(title, str) or not title.strip() or code in (None, ""):
        return None
    lang = lang_poll(title, verbose=verbose)
    entry["titles"].append(
        {"title": title.strip(), "lang": lang, "source": "siiu"})
    entry["external_ids"].append(
        {"provenance": "siiu", "source": "codigo", "id": code})
    if "project_participant" in reg:
        for author in reg["project_participant"]:
            # solo investigador principar de momento
            roles = author.get("project_participant_role") or []
            person_id = author.get("PERSONA_NATURAL")
            if roles and roles[0].get("IDENTIFICADOR") == 307 and person_id:

                affiliations = []
                for group in author.get("group") or []:
                    if group.get("CODIGO_COLCIENCIAS"):
                        grec = {
                            "external_ids": [{"provenance": "siiu", "source": "scienti", "id": group["CODIGO_COLCIENCIAS"]}],
                            "name": group["NOMBRE_COMPLETO"]
                        }

                        affiliations.append(grec)
                        # hay que hacer un siiu affiliations y crozar los grupos apra ver si obtenemos mas NRO_ID_GRUPO
                        # affiliations.append(
                        #     {
                        #         "external_ids": [{"provenance": "siiu", "source": "scienti", "id": group["NRO_ID_GRUPO"]}],
                        #         "name": group["NOMBRE_COMPLETO"]
                        #     }
                        # )
                if author.get("INSTITUCION"):
                    affiliations.append({
                        "external_ids": [{"provenance": "siiu", "source": "nit", "id": author["INSTITUCION"]}]
                    })
                # type
                subtypes = reg.get("project_subtype") or []
                project_types = subtypes[0].get("project_type") or [] if subtypes else []
                if subtypes and project_types:

                    entry["types"].append(
                        {
                            "provenance": 'siiu',
                            "source": 'siiu',
                            "type": project_types[0].get("NOMBRE", ""),
                            "level": 0,
                            "code": str(project_types[0].get("IDENTIFICADOR", ""))
                        }
                    )
                    entry["types"].append(
                        {
                            "provenance": 'siiu',
                            "source": 'siiu',
                            "type": subtypes[0].get("NOMBRE", ""),
                            "level": 1,
                            "code": str(project_types[0].get("IDENTIFICADOR", "")) + str(subtypes[0].get("IDENTIFICADOR", ""))
                        }
                    )

                author_entry = {
                    "full_name": "",
                    "affiliations": affiliations,
                    "external_ids": [{"provenance": 'siiu', "source": 'Cédula de Ciudadanía', "id": person_id}]
                }
                entry["authors"].append(author_entry)
    return entry
