from kahi_impactu_postcalculations.denormalization import (
    set_works_authors_affiliations_dates,
    set_works_authors_affiliations_external_data,
    set_works_groups_citations_count,
    set_works_groups_ranking_to_works_collection,
)


class _Collection:
    def __init__(self):
        self.pipeline = None

    def count_documents(self, _query):
        return 1

    def aggregate(self, pipeline, **_kwargs):
        self.pipeline = pipeline
        return []


def test_affiliation_dates_lookup_uses_indexable_equality_join():
    collection = _Collection()

    set_works_authors_affiliations_dates(collection)

    lookup = collection.pipeline[1]["$lookup"]
    assert lookup["localField"] == "authors.id"
    assert lookup["foreignField"] == "_id"
    assert "let" not in lookup
    assert not any("$match" in stage for stage in lookup["pipeline"])


def _captured_lookup(function):
    collection = _Collection()
    function(collection)
    return next(stage["$lookup"] for stage in collection.pipeline if "$lookup" in stage)


def test_remaining_denormalization_lookups_use_indexable_equality_joins():
    cases = [
        (
            set_works_authors_affiliations_external_data,
            "authors.affiliations.id",
        ),
        (set_works_groups_citations_count, "groups.id"),
        (set_works_groups_ranking_to_works_collection, "groups.id"),
    ]

    for function, local_field in cases:
        lookup = _captured_lookup(function)
        assert lookup["localField"] == local_field
        assert lookup["foreignField"] == "_id"
        assert "let" not in lookup
        assert not any(
            "$expr" in stage.get("$match", {})
            for stage in lookup.get("pipeline", [])
        )
