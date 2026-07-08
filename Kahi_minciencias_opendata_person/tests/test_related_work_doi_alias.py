from kahi_minciencias_opendata_person.Kahi_minciencias_opendata_person import (
    ensure_doi_related_work_aliases,
)


def test_doi_alias_remains_discoverable_for_unicity():
    entry = {
        "related_works": [
            {
                "provenance": "minciencias",
                "source": "scienti",
                "id": {"COD_RH": "123", "COD_PRODUCTO": "4"},
                "title": "Trabajo enriquecido",
                "doi": ["10.1000/TEST"],
            }
        ]
    }

    ensure_doi_related_work_aliases(entry, ["10.1000/TEST"])
    ensure_doi_related_work_aliases(entry, ["https://doi.org/10.1000/test"])

    doi_aliases = [item for item in entry["related_works"] if item["source"] == "doi"]
    assert doi_aliases == [
        {
            "provenance": "minciencias",
            "source": "doi",
            "id": "https://doi.org/10.1000/test",
        }
    ]
