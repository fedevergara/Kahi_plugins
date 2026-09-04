import pytest

from kahi_impactu_utils.Utils import compare_author
from kahi_unicity_person_graph.Kahi_unicity_person_graph import (
    CandidateEdgeBuilder,
    ComponentResolver,
    ComponentResult,
    Kahi_unicity_person_graph,
    MergePlan,
)
from kahi_unicity_person_graph.graph import (
    CandidateGroup,
    build_evidence_components,
)


ORCID_1 = "https://orcid.org/0000-0003-2895-3084"
ORCID_2 = "https://orcid.org/0000-0003-4072-6140"
SCOPUS_1 = "https://www.scopus.com/authid/detail.uri?authorId=12345678"
SCOPUS_2 = "https://www.scopus.com/authid/detail.uri?authorId=87654321"
SCHOLAR_1 = "https://scholar.google.com/citations?user=abcdefgh1234"
DOI_1 = "https://doi.org/10.1000/first"
DOI_2 = "https://doi.org/10.1000/second"


def person(
    person_id,
    name,
    source,
    external_ids=None,
    aliases=None,
    affiliations=None,
    first_names=None,
    last_names=None,
    initials="",
):
    return {
        "_id": person_id,
        "updated": [{"source": source, "time": 1}],
        "full_name": name,
        "first_names": first_names or [name.split()[0]],
        "last_names": last_names or [name.split()[-1]],
        "initials": initials,
        "aliases": aliases or [],
        "affiliations": affiliations or [],
        "keywords": [],
        "external_ids": external_ids or [],
        "sex": "",
        "marital_status": None,
        "ranking": [],
        "birthplace": {},
        "birthdate": -1,
        "degrees": [],
        "subjects": [],
        "citations_count": [],
        "products_count": 0,
        "related_works": [],
    }


def build(groups, snapshot, compare=compare_author):
    discovery = CandidateEdgeBuilder(compare).add_groups(groups, snapshot).finish()
    components = build_evidence_components(discovery.edges)
    results = tuple(
        ComponentResolver(compare, timestamp=10).resolve(component, snapshot)
        for component in components
    )
    return discovery, results


def test_pair_filtering_prevents_coauthors_from_forming_a_raw_component():
    groups = [CandidateGroup("doi", DOI_1, ("a", "b", "c"), 0)]
    snapshot = {
        "a": person("a", "Ada Lovelace", "staff"),
        "b": person("b", "Ada Lovelace", "openalex", initials="A"),
        "c": person("c", "Charles Babbage", "scholar"),
    }

    discovery, results = build(groups, snapshot)

    assert len(discovery.edges) == 1
    assert {discovery.edges[0].left_id, discovery.edges[0].right_id} == {"a", "b"}
    assert len(results) == 1
    assert results[0].component.member_ids == ("a", "b")


def test_strong_transitive_chain_does_not_require_direct_target_evidence():
    groups = [
        CandidateGroup("orcid", ORCID_1, ("a", "b"), 0),
        CandidateGroup("scopus", SCOPUS_1, ("b", "c"), 1),
    ]
    snapshot = {
        "a": person(
            "a",
            "Ada Lovelace",
            "staff",
            external_ids=[{"source": "orcid", "id": ORCID_1}],
        ),
        "b": person(
            "b",
            "Ada Lovelace",
            "scopus",
            external_ids=[
                {"source": "orcid", "id": ORCID_1},
                {"source": "scopus", "id": SCOPUS_1},
            ],
        ),
        "c": person(
            "c",
            "Ada Lovelace",
            "scholar",
            external_ids=[{"source": "scopus", "id": SCOPUS_1}],
        ),
    }

    _, results = build(groups, snapshot)
    plan = results[0].plans[0]

    assert plan.target_id == "a"
    assert plan.absorbed_ids == ("b", "c")
    assert len(plan.anchor_evidence) == 2
    assert plan.confidence == "high"


def test_cluster_rejects_a_strong_bridge_with_cross_member_name_conflict():
    groups = [
        CandidateGroup("orcid", ORCID_1, ("a", "b"), 0),
        CandidateGroup("scopus", SCOPUS_1, ("b", "c"), 1),
    ]
    snapshot = {
        "a": person(
            "a", "Alice Alpha", "staff", first_names=["Alice"], last_names=["Alpha"]
        ),
        "b": person(
            "b",
            "Alice Bob Alpha",
            "scopus",
            first_names=["Alice", "Bob"],
            last_names=["Alpha"],
        ),
        "c": person(
            "c", "Bob Alpha", "scholar", first_names=["Bob"], last_names=["Alpha"]
        ),
    }

    _, results = build(groups, snapshot)

    assert len(results[0].plans) == 1
    assert set((results[0].plans[0].target_id, *results[0].plans[0].absorbed_ids)) == {
        "a",
        "b",
    }
    assert results[0].rejected_weak_bridges == 1


def test_cluster_rejects_transitive_authoritative_identifier_conflict():
    groups = [
        CandidateGroup("doi", DOI_1, ("a", "b"), 0, 4),
        CandidateGroup("doi", DOI_2, ("b", "c"), 1, 4),
    ]
    snapshot = {
        "a": person(
            "a",
            "Ada Lovelace",
            "staff",
            external_ids=[{"source": "orcid", "id": ORCID_1}],
        ),
        "b": person("b", "Ada Lovelace", "openalex", initials="A"),
        "c": person(
            "c",
            "Ada Lovelace",
            "scholar",
            external_ids=[{"source": "orcid", "id": ORCID_2}],
        ),
    }

    _, results = build(groups, snapshot)

    assert len(results[0].plans) == 1
    assert len(results[0].plans[0].absorbed_ids) == 1
    assert results[0].rejected_identity_conflicts == 1
    assert results[0].rejected_orcid_conflicts == 1


def test_one_doi_with_compatible_non_exact_name_is_review_only():
    groups = [CandidateGroup("doi", DOI_1, ("a", "b"), 0, 2000)]
    snapshot = {
        "a": person(
            "a",
            "A Lovelace",
            "staff",
            first_names=["A"],
            last_names=["Lovelace"],
            initials="A",
        ),
        "b": person(
            "b",
            "Ada Lovelace",
            "scholar",
            first_names=["Ada"],
            last_names=["Lovelace"],
            initials="A",
        ),
    }

    discovery, results = build(groups, snapshot)

    assert discovery.edges[0].confidence == "medium"
    assert results[0].plans == ()
    assert len(results[0].review_plans) == 1
    assert results[0].review_plans[0].confidence == "medium"


def test_one_small_work_doi_with_initial_and_full_name_is_high_confidence():
    groups = [CandidateGroup("doi", DOI_1, ("a", "b"), 0, 4)]
    snapshot = {
        "a": person(
            "a",
            "A Lovelace",
            "staff",
            first_names=["A"],
            last_names=["Lovelace"],
            initials="A",
        ),
        "b": person(
            "b",
            "Ada Lovelace",
            "scholar",
            first_names=["Ada"],
            last_names=["Lovelace"],
            initials="A",
        ),
    }

    discovery, results = build(groups, snapshot)

    assert discovery.edges[0].confidence == "high"
    assert discovery.edges[0].score == 75
    assert discovery.edges[0].match_details[0]["work_author_count"] == 4
    assert discovery.edges[0].match_details[0]["confidence_reasons"] == [
        "small_work_compatible_name",
        "shared_doi",
    ]
    assert len(results[0].plans) == 1


@pytest.mark.parametrize(
    ("author_count", "expected_confidence"),
    [(10, "high"), (11, "medium"), (None, "medium")],
)
def test_compatible_name_uses_actual_work_author_threshold(
    author_count, expected_confidence
):
    snapshot = {
        "a": person(
            "a", "A Lovelace", "staff", first_names=["A"],
            last_names=["Lovelace"], initials="A"
        ),
        "b": person(
            "b", "Ada Lovelace", "scholar", first_names=["Ada"],
            last_names=["Lovelace"], initials="A"
        ),
    }

    discovery, _ = build(
        [CandidateGroup("doi", DOI_1, ("a", "b"), 0, author_count)],
        snapshot,
    )

    assert discovery.edges[0].confidence == expected_confidence


@pytest.mark.parametrize(
    ("author_count", "expected_confidence"),
    [(50, "high"), (51, "medium"), (None, "medium")],
)
def test_exact_name_uses_exact_name_author_threshold(
    author_count, expected_confidence
):
    snapshot = {
        "a": person("a", "Ada Lovelace", "staff"),
        "b": person("b", "Ada Lovelace", "openalex"),
    }

    discovery, _ = build(
        [CandidateGroup("doi", DOI_1, ("a", "b"), 0, author_count)],
        snapshot,
    )

    assert discovery.edges[0].confidence == expected_confidence


def test_two_dois_with_compatible_name_are_high_confidence():
    groups = [
        CandidateGroup("doi", DOI_1, ("a", "b"), 0),
        CandidateGroup("doi", DOI_2, ("a", "b"), 1),
    ]
    snapshot = {
        "a": person(
            "a",
            "Ada Augusta Lovelace",
            "staff",
            first_names=["Ada", "Augusta"],
            last_names=["Lovelace"],
        ),
        "b": person(
            "b",
            "Ada Lovelace",
            "scholar",
            first_names=["Ada"],
            last_names=["Lovelace"],
        ),
    }

    discovery, results = build(groups, snapshot)

    assert discovery.edges[0].score == 90
    assert results[0].plans[0].confidence == "high"
    assert results[0].plans[0].evidence == (
        {"source": "doi", "key": DOI_1},
        {"source": "doi", "key": DOI_2},
    )


def test_exact_name_and_one_doi_are_high_confidence():
    groups = [CandidateGroup("doi", DOI_1, ("a", "b"), 0, 20)]
    snapshot = {
        "a": person("a", "Ada Lovelace", "staff"),
        "b": person("b", "Ada Lovelace", "openalex"),
    }

    discovery, results = build(groups, snapshot)

    assert discovery.edges[0].score == 85
    assert len(results[0].plans) == 1


def test_alias_only_match_is_kept_for_review():
    groups = [CandidateGroup("doi", DOI_1, ("a", "b"), 0)]
    snapshot = {
        "a": person(
            "a",
            "E Restrepo",
            "scholar",
            aliases=["E. Restrepo"],
            first_names=["E"],
            last_names=["Restrepo"],
            initials="E",
        ),
        "b": person(
            "b",
            "Elisabeth Restrepo Parra",
            "staff",
            aliases=["E Restrepo"],
            first_names=["Elisabeth"],
            last_names=["Restrepo", "Parra"],
            initials="ERP",
        ),
    }

    discovery, results = build(groups, snapshot)

    assert discovery.edges[0].confidence == "medium"
    assert discovery.edges[0].match_details[0]["alias_name_match"] is True
    assert results[0].plans == ()
    assert len(results[0].review_plans) == 1


def test_alias_only_match_with_three_shared_name_tokens_is_high_confidence():
    groups = [CandidateGroup("doi", DOI_1, ("a", "b"), 0, 4)]
    snapshot = {
        "a": person(
            "a",
            "Maria Elena Torres Gomez",
            "staff",
            aliases=["Maria Elena Torres"],
            first_names=["Maria", "Elena"],
            last_names=["Torres", "Gomez"],
        ),
        "b": person(
            "b",
            "Maria Elena Torres",
            "scholar",
            first_names=["Maria", "Elena"],
            last_names=["Torres"],
        ),
    }

    discovery, results = build(
        groups,
        snapshot,
        compare=lambda left, right, count: False,
    )

    assert discovery.edges[0].confidence == "high"
    assert discovery.edges[0].score == 80
    assert discovery.edges[0].match_details[0]["confidence_reasons"] == [
        "three_shared_name_tokens",
        "shared_doi",
    ]
    assert len(results[0].plans) == 1
    assert results[0].review_plans == ()


def test_alias_only_match_with_two_shared_name_tokens_stays_for_review():
    groups = [CandidateGroup("doi", DOI_1, ("a", "b"), 0)]
    snapshot = {
        "a": person(
            "a",
            "Maria Torres Gomez",
            "staff",
            aliases=["Maria Torres"],
            first_names=["Maria"],
            last_names=["Torres", "Gomez"],
        ),
        "b": person(
            "b",
            "Maria Torres",
            "scholar",
            first_names=["Maria"],
            last_names=["Torres"],
        ),
    }

    discovery, results = build(
        groups,
        snapshot,
        compare=lambda left, right, count: False,
    )

    assert discovery.edges[0].confidence == "medium"
    assert results[0].plans == ()
    assert len(results[0].review_plans) == 1


def test_alias_match_with_four_dois_is_high_confidence():
    groups = [
        CandidateGroup("doi", DOI_1, ("a", "b"), 0),
        CandidateGroup("doi", DOI_2, ("a", "b"), 1),
        CandidateGroup("doi", "https://doi.org/10.1000/third", ("a", "b"), 2),
        CandidateGroup("doi", "https://doi.org/10.1000/fourth", ("a", "b"), 3),
    ]
    snapshot = {
        "a": person(
            "a",
            "E Restrepo",
            "scholar",
            aliases=["Elisabeth Restrepo Parra"],
            first_names=["E"],
            last_names=["Restrepo"],
            initials="E",
        ),
        "b": person(
            "b",
            "Elisabeth Restrepo Parra",
            "staff",
            aliases=["Elisabeth Restrepo Parra"],
            first_names=["Elisabeth"],
            last_names=["Restrepo", "Parra"],
            initials="ERP",
        ),
    }

    discovery, results = build(groups, snapshot)

    assert discovery.edges[0].confidence == "high"
    assert discovery.edges[0].score == 80
    assert len(results[0].plans) == 1


def test_partial_alias_cannot_override_divergent_complete_names():
    groups = [CandidateGroup("doi", DOI_1, ("a", "b"), 0)]
    snapshot = {
        "a": person(
            "a",
            "Andres Lopez Rubio",
            "staff",
            aliases=["Andres Lopez"],
            first_names=["Andres"],
            last_names=["Lopez", "Rubio"],
        ),
        "b": person(
            "b",
            "Andres Lopez Astudillo",
            "scholar",
            aliases=["Andres Lopez"],
            first_names=["Andres"],
            last_names=["Lopez", "Astudillo"],
        ),
    }

    discovery, results = build(groups, snapshot)

    assert discovery.edges == ()
    assert discovery.rejected_given_name_conflicts == 1
    assert results == ()


def test_shared_affiliation_support_does_not_promote_a_weak_edge_to_high():
    affiliation = [{"id": "https://ror.org/12345"}]
    groups = [CandidateGroup("doi", DOI_1, ("a", "b"), 0, 2000)]
    snapshot = {
        "a": person(
            "a",
            "A Lovelace",
            "staff",
            affiliations=affiliation,
            first_names=["A"],
            last_names=["Lovelace"],
            initials="A",
        ),
        "b": person(
            "b",
            "Ada Lovelace",
            "scholar",
            affiliations=affiliation,
            first_names=["Ada"],
            last_names=["Lovelace"],
            initials="A",
        ),
    }

    discovery, _ = build(groups, snapshot)

    assert discovery.edges[0].confidence == "medium"
    assert discovery.edges[0].score == 65
    assert "shared_affiliation_support" in discovery.edges[0].match_details[0][
        "confidence_reasons"
    ]


def test_high_subcluster_is_preserved_when_another_edge_is_medium():
    groups = [
        CandidateGroup("doi", DOI_1, ("a", "b"), 0, 20),
        CandidateGroup("doi", DOI_2, ("b", "c"), 1, 2000),
    ]
    snapshot = {
        "a": person("a", "Ada Lovelace", "staff"),
        "b": person("b", "Ada Lovelace", "openalex", initials="A"),
        "c": person(
            "c",
            "A Lovelace",
            "scholar",
            first_names=["A"],
            last_names=["Lovelace"],
            initials="A",
        ),
    }

    _, results = build(groups, snapshot)
    result = results[0]

    assert len(result.plans) == 1
    assert set((result.plans[0].target_id, *result.plans[0].absorbed_ids)) == {"a", "b"}
    assert len(result.review_plans) == 1
    assert set(
        (result.review_plans[0].target_id, *result.review_plans[0].absorbed_ids)
    ) == {"b", "c"}


def test_edge_and_cluster_results_are_independent_of_group_order():
    groups = [
        CandidateGroup("scopus", SCOPUS_1, ("b", "c"), 1),
        CandidateGroup("orcid", ORCID_1, ("a", "b"), 0),
    ]
    snapshot = {
        key: person(key, "Ada Lovelace", source)
        for key, source in (("a", "staff"), ("b", "openalex"), ("c", "scholar"))
    }

    first, first_results = build(groups, snapshot, compare=lambda left, right, count: True)
    second, second_results = build(
        list(reversed(groups)),
        snapshot,
        compare=lambda left, right, count: True,
    )

    assert first.edges == second.edges
    assert first_results[0].plans == second_results[0].plans


@pytest.mark.parametrize(
    ("source", "value"),
    [
        ("orcid", "orcid-1"),
        ("scopus", "https://www.scopus.com/authid/detail.uri?authorId=0000"),
        ("scholar", "https://scholar.google.com/citations?user=x"),
        ("doi", "not-a-doi"),
    ],
)
def test_invalid_candidate_identifiers_are_rejected(source, value):
    snapshot = {
        "a": person("a", "Ada Lovelace", "staff"),
        "b": person("b", "Ada Lovelace", "scholar"),
    }
    discovery, results = build(
        [CandidateGroup(source, value, ("a", "b"), 0)],
        snapshot,
        compare=lambda left, right, count: True,
    )

    assert discovery.edges == ()
    assert discovery.rejected_invalid_identifier_groups == 1
    assert results == ()


@pytest.mark.parametrize(
    ("source", "value", "score"),
    [("scholar", SCHOLAR_1, 96), ("scopus", SCOPUS_1, 94)],
)
def test_valid_source_identifiers_are_strong_evidence(source, value, score):
    snapshot = {
        "a": person("a", "Ada Lovelace", "staff"),
        "b": person("b", "Ada Lovelace", "scholar"),
    }
    discovery, results = build(
        [CandidateGroup(source, value, ("a", "b"), 0)],
        snapshot,
        compare=lambda left, right, count: True,
    )

    assert discovery.edges[0].confidence == "high"
    assert discovery.edges[0].score == score
    assert len(results[0].plans) == 1


def test_pair_conflicting_orcids_are_rejected_before_graph_construction():
    snapshot = {
        "a": person(
            "a",
            "Fernando Martinez",
            "staff",
            external_ids=[{"source": "orcid", "id": ORCID_1}],
        ),
        "b": person(
            "b",
            "Fernando Martinez",
            "scholar",
            external_ids=[{"source": "orcid", "id": ORCID_2}],
        ),
    }
    discovery, results = build(
        [CandidateGroup("doi", DOI_1, ("a", "b"), 0)],
        snapshot,
    )

    assert discovery.edges == ()
    assert discovery.rejected_orcid_conflicts == 1
    assert discovery.rejected_identity_conflicts == 1
    assert results == ()


def test_pair_with_different_cedulas_is_rejected_without_rewriting_source():
    source = "Cédula de Ciudadanía"
    left_external_ids = [{"source": source, "id": "1.234.567"}]
    right_external_ids = [{"source": source, "id": "7654321"}]
    snapshot = {
        "a": person(
            "a", "Ada Lovelace", "staff", external_ids=left_external_ids
        ),
        "b": person(
            "b", "Ada Lovelace", "scholar", external_ids=right_external_ids
        ),
    }

    discovery, results = build(
        [CandidateGroup("doi", DOI_1, ("a", "b"), 0, 2)],
        snapshot,
    )

    assert discovery.edges == ()
    assert discovery.rejected_national_id_conflicts == 1
    assert discovery.rejected_identity_conflicts == 1
    assert snapshot["a"]["external_ids"][0]["source"] == source
    assert snapshot["b"]["external_ids"][0]["source"] == source
    assert results == ()


@pytest.mark.parametrize(
    "value",
    [
        "10.1000/FIRST",
        "doi:10.1000/FIRST",
        "http://dx.doi.org/10.1000/FIRST.",
        "https%3A%2F%2Fdoi.org%2F10.1000%2FFIRST",
    ],
)
def test_doi_candidate_key_is_canonicalized(value):
    assert ComponentResolver._canonical_candidate_key("doi", value) == DOI_1


def test_document_with_multiple_valid_orcids_is_rejected_before_graph():
    snapshot = {
        "a": person(
            "a",
            "Fernando Martinez",
            "staff",
            external_ids=[
                {"source": "orcid", "id": ORCID_1},
                {"source": "orcid", "id": ORCID_2},
            ],
        ),
        "b": person("b", "Fernando Martinez", "scholar"),
    }

    discovery, results = build(
        [CandidateGroup("doi", DOI_1, ("a", "b"), 0)],
        snapshot,
    )

    assert discovery.edges == ()
    assert discovery.rejected_orcid_conflicts == 1
    assert discovery.rejected_identity_conflicts == 1
    assert results == ()


def test_conflicting_compound_names_are_rejected_before_graph_construction():
    snapshot = {
        "a": person(
            "a",
            "Sandra Cecilia Bautista Rodriguez",
            "staff",
            first_names=["Sandra", "Cecilia"],
            last_names=["Rodriguez", "Bautista"],
            initials="SCBR",
        ),
        "b": person(
            "b",
            "Sandra Milena Rodriguez Acosta",
            "scholar",
            first_names=["Sandra", "Milena"],
            last_names=["Rodriguez", "Acosta"],
            initials="SMRA",
        ),
    }
    discovery, _ = build(
        [CandidateGroup("doi", DOI_1, ("a", "b"), 0)],
        snapshot,
    )

    assert discovery.edges == ()
    assert discovery.rejected_given_name_conflicts == 1


def test_final_validation_rejects_a_plan_with_conflicting_orcids():
    snapshot = {
        "a": person(
            "a",
            "Fernando Martinez",
            "staff",
            external_ids=[{"source": "orcid", "id": ORCID_1}],
        ),
        "b": person(
            "b",
            "Fernando Martinez",
            "scholar",
            external_ids=[{"source": "orcid", "id": ORCID_2}],
        ),
    }
    component = build_evidence_components(
        CandidateEdgeBuilder(lambda left, right, count: True)
        .add_groups([CandidateGroup("doi", DOI_1, ("a", "b"), 0)], snapshot)
        .finish()
        .edges
    )
    if component:
        collision_component = component[0]
    else:
        from kahi_unicity_person_graph.graph import CollisionComponent

        collision_component = CollisionComponent("a", ("a", "b"))
    plan = MergePlan(
        component_id="a",
        target_id="a",
        absorbed_ids=("b",),
        merged_document=snapshot["a"],
        evidence=(),
        match_details=(),
        anchor_evidence=(),
        confidence="high",
    )
    result = ComponentResult(
        component=collision_component,
        snapshot=snapshot,
        plans=(plan,),
        review_plans=(),
        rejected_orcid_conflicts=0,
        rejected_national_id_conflicts=0,
        rejected_identity_conflicts=0,
        rejected_name_mismatches=0,
        rejected_given_name_conflicts=0,
        rejected_weak_bridges=0,
    )

    with pytest.raises(RuntimeError, match="conflicting authoritative"):
        Kahi_unicity_person_graph._validate_disjoint_results([result])


def test_review_plan_never_enters_apply_transaction():
    component = build_evidence_components(
        CandidateEdgeBuilder(lambda left, right, count: True)
        .add_groups(
            [CandidateGroup("doi", DOI_1, ("a", "b"), 0)],
            {
                "a": person("a", "Ada Augusta Lovelace", "staff"),
                "b": person("b", "Ada Lovelace", "scholar"),
            },
        )
        .finish()
        .edges
    )[0]
    result = ComponentResult(
        component=component,
        snapshot={},
        plans=(),
        review_plans=(),
        rejected_orcid_conflicts=0,
        rejected_national_id_conflicts=0,
        rejected_identity_conflicts=0,
        rejected_name_mismatches=0,
        rejected_given_name_conflicts=0,
        rejected_weak_bridges=0,
    )

    class TransactionMustNotStart:
        def start_session(self):
            raise AssertionError("review plans must not be applied")

    plugin = Kahi_unicity_person_graph.__new__(Kahi_unicity_person_graph)
    plugin.client = TransactionMustNotStart()
    plugin._apply_component(result, "run-1")
