from kahi_dspace_works.parser import parse_dspace
from kahi_dspace_works.utils import get_dim_fields, get_doi


def empty_work():
    return {
        "titles": [],
        "year_published": None,
        "abstracts": [],
        "authors": [],
        "types": [],
        "rights": [],
        "external_ids": [],
        "external_urls": [],
        "source": {},
        "bibliographic_info": {},
        "doi": None,
    }


def record(fields):
    return {
        "_id": "oai:example.edu:123/456",
        "OAI-PMH": {
            "request": {
                "#text": "https://example.edu/oai/request",
                "@verb": "GetRecord",
                "@metadataPrefix": "dim",
                "@identifier": "oai:example.edu:123/456",
            },
            "GetRecord": {
                "record": {"metadata": {"dim:dim": {"dim:field": fields}}}
            },
        },
    }


def test_parse_abstract_and_public_uri():
    reg = record(
        [
            {"@element": "title", "#text": "Título de prueba"},
            {
                "@element": "description",
                "@qualifier": "abstract",
                "#text": "Resumen suficientemente descriptivo para la prueba.",
            },
            {
                "@element": "identifier",
                "@qualifier": "uri",
                "#text": "https://hdl.handle.net/123/456",
            },
        ]
    )

    parsed = parse_dspace(reg, empty_work(), "https://example.edu")

    assert len(parsed["abstracts"]) == 1
    assert parsed["abstracts"][0]["provenance"] == "dspace"
    assert parsed["external_urls"][0]["url"] == "https://hdl.handle.net/123/456"


def test_singleton_dim_field_is_supported():
    reg = record(
        {
            "@element": "identifier",
            "@qualifier": "doi",
            "#text": "10.1000/test",
        }
    )

    assert len(get_dim_fields(reg)) == 1
    assert get_doi(reg) == "https://doi.org/10.1000/test"


def test_malformed_metadata_returns_no_fields():
    assert get_dim_fields({"OAI-PMH": {}}) == []
