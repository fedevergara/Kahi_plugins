from kahi_openalex_affiliations.Kahi_openalex_affiliations import (
    openalex_address,
)


def test_openalex_address_uses_geonames_city_id():
    affiliation = {
        "geo": {
            "latitude": 6.25,
            "longitude": -75.56,
            "region": "Antioquia",
            "city": "Medellín",
            "geonames_city_id": "3674962",
            "country": "Colombia",
            "country_code": "CO",
        }
    }

    assert openalex_address(affiliation) == {
        "lat": 6.25,
        "lng": -75.56,
        "state": "Antioquia",
        "city": "Medellín",
        "city_id": "3674962",
        "country": "Colombia",
        "country_code": "CO",
    }
