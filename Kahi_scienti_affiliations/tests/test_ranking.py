import pytest

from kahi_scienti_affiliations.Kahi_scienti_affiliations import (
    normalize_scienti_rank,
)


@pytest.mark.parametrize("rank", ["A1", "A", "B", "C"])
def test_existing_scienti_ranks_are_preserved(rank):
    assert normalize_scienti_rank(rank) == rank


def test_recognized_scienti_rank_is_translated():
    assert normalize_scienti_rank("00") == "Reconocido"
