from copy import deepcopy

from kahi_scienti_affiliations.Kahi_scienti_affiliations import (
    apply_scienti_institution_data,
    normalize_nit,
    normalize_url,
    scienti_address,
)


def institution_record():
    return {
        "COD_INST": "000000009892",
        "ID_INSTITUCION": 481,
        "NME_INST": "CEMENTOS ARGOS S.A",
        "NME_INST_FILTRO": "CEMENTOS ARGOS",
        "SGL_INST": "Argos",
        "SGL_PAIS": "1",
        "SGL_DEPARTAMENTO": "31",
        "TXT_CIUDAD_INST": "Medellín",
        "TXT_NIT": "890.100.251",
        "TXT_DIGITO_VERIFICADOR": "0",
        "COD_IES": "1201",
        "URL_HOME_PAGE": "argos.com.co",
        "DTA_CONSTITUCION": "1944-08-14 00:00:00",
        "DTA_ACTUALIZACION": "2021-08-17 16:28:47",
        "city": [{
            "SGL_PAIS": "COL",
            "SGL_DEPARTAMENTO": "CU",
            "TXT_NME_MUNICIPIO": "TOCAIMA",
            "department": [{
                "TXT_NME_DEPARTAMENTO": "CUNDINAMARCA",
                "country": [{
                    "SGL_PAIS": "COL",
                    "TXT_NME_PAIS": "Colombia",
                    "SGL_PAIS_ISO_2": "CO",
                }],
            }],
        }],
    }


def test_address_prefers_explicit_city_over_conflicting_nested_city():
    address = scienti_address(institution_record())

    assert address["city"] == "Medellín"
    assert address["state"] == ""
    assert address["country"] == "Colombia"
    assert address["country_code"] == "CO"


def test_address_rejects_cross_country_nested_city():
    reg = {
        "SGL_PAIS": "COL",
        "city": [{
            "SGL_PAIS": "BRA",
            "TXT_NME_MUNICIPIO": "Guaranésia",
            "department": [{
                "TXT_NME_DEPARTAMENTO": "Minas Gerais",
                "country": [{
                    "SGL_PAIS": "BRA",
                    "TXT_NME_PAIS": "Brasil",
                    "SGL_PAIS_ISO_2": "BR",
                }],
            }],
        }],
    }

    address = scienti_address(reg)

    assert address["city"] == ""
    assert address["state"] == ""
    assert address["country"] == "Colombia"


def test_scienti_metadata_uses_only_existing_affiliation_fields():
    entry = apply_scienti_institution_data(
        {"year_established": None}, institution_record())

    assert entry["names"] == [{
        "source": "scienti", "name": "CEMENTOS ARGOS S.A", "lang": "es"}]
    assert entry["abbreviations"] == ["Argos"]
    assert entry["year_established"] == 1944
    assert entry["external_urls"] == [{
        "provenance": "scienti",
        "source": "site",
        "url": "https://argos.com.co",
    }]
    assert {(item["source"], item["id"]) for item in entry["external_ids"]} == {
        ("minciencias", "000000009892"),
        ("scienti", "481"),
        ("nit", "890100251"),
        ("snies", "1201"),
    }
    assert entry["types"] == [{
        "provenance": "scienti", "source": "scienti", "type": "education"}]
    assert set(entry) >= {
        "names", "abbreviations", "year_established", "addresses",
        "external_urls", "external_ids", "types", "updated",
    }


def test_scienti_enrichment_preserves_existing_year_and_geography():
    entry = {
        "year_established": 1950,
        "addresses": [{
            "lat": 6.25,
            "lng": -75.56,
            "postcode": "050021",
            "state": "Antioquia",
            "city": "Medellín",
            "country": "Colombia",
            "country_code": "CO",
        }],
    }
    original_address = deepcopy(entry["addresses"])

    apply_scienti_institution_data(entry, institution_record())

    assert entry["year_established"] == 1950
    assert entry["addresses"] == original_address


def test_placeholder_identifiers_and_urls_are_rejected():
    reg = institution_record()
    reg["TXT_NIT"] = "000000000"

    assert normalize_nit(reg) == ""
    assert normalize_url("Pendiente") == ""
    assert normalize_url("contacto@example.org") == ""
