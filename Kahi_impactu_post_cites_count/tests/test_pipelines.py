from kahi_impactu_post_cites_count.Kahi_impactu_post_cites_count import Kahi_impactu_post_cites_count


def test_products_pipeline_deduplicates_entity_per_work_and_merges():
    pipeline = Kahi_impactu_post_cites_count.products_pipeline("person", "person")
    assert {"$unwind": "$authors"} in pipeline
    assert pipeline[-3] == {
        "$group": {"_id": {"entity": "$_entity_id", "work": "$_id"}}
    }
    assert pipeline[-1]["$merge"]["into"] == "person"
    assert pipeline[-1]["$merge"]["whenNotMatched"] == "discard"


def test_affiliation_pipeline_unwinds_nested_affiliations():
    pipeline = Kahi_impactu_post_cites_count.citations_pipeline(
        "affiliation", "affiliations"
    )
    assert {"$unwind": "$authors.affiliations"} in pipeline
    assert pipeline[-1]["$merge"]["into"] == "affiliations"


def test_source_pipeline_uses_source_id():
    pipeline = Kahi_impactu_post_cites_count.products_pipeline("source", "sources")
    assert pipeline[0] == {"$set": {"_entity_id": "$source.id"}}


def test_unknown_entity_is_rejected():
    try:
        Kahi_impactu_post_cites_count.products_pipeline("unknown", "target")
    except ValueError:
        return
    raise AssertionError("ValueError was not raised")
