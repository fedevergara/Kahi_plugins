import pytest

from kahi_scienti_affiliations.Kahi_scienti_affiliations import (
    inherit_parent_addresses,
    normalize_scienti_rank,
)


@pytest.mark.parametrize("rank", ["A1", "A", "B", "C"])
def test_existing_scienti_ranks_are_preserved(rank):
    assert normalize_scienti_rank(rank) == rank


def test_recognized_scienti_rank_is_translated():
    assert normalize_scienti_rank("00") == "Reconocido"


def test_group_inherits_unique_parent_addresses_when_empty():
    address = {"city": "Medellín", "country_code": "CO"}
    entry = {"addresses": []}

    changed = inherit_parent_addresses(
        entry,
        [{"addresses": [address]}, {"addresses": [address]}],
    )

    assert changed is True
    assert entry["addresses"] == [address]


def test_group_preserves_existing_addresses():
    entry = {"addresses": [{"city": "Bogotá", "country_code": "CO"}]}

    changed = inherit_parent_addresses(
        entry,
        [{"addresses": [{"city": "Medellín", "country_code": "CO"}]}],
    )

    assert changed is False
    assert entry["addresses"] == [{"city": "Bogotá", "country_code": "CO"}]
