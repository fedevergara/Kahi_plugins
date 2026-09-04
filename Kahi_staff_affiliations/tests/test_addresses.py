from kahi_staff_affiliations.Kahi_staff_affiliations import (
    inherit_parent_addresses,
)


def test_academic_unit_inherits_parent_addresses_when_empty():
    parent = {
        "addresses": [{"city": "Medellín", "country_code": "CO"}],
    }
    entry = {"addresses": []}

    assert inherit_parent_addresses(entry, parent) is True
    assert entry["addresses"] == parent["addresses"]
    assert entry["addresses"] is not parent["addresses"]


def test_academic_unit_preserves_existing_addresses():
    entry = {"addresses": [{"city": "Bogotá", "country_code": "CO"}]}
    parent = {
        "addresses": [{"city": "Medellín", "country_code": "CO"}],
    }

    assert inherit_parent_addresses(entry, parent) is False
    assert entry["addresses"] == [{"city": "Bogotá", "country_code": "CO"}]
