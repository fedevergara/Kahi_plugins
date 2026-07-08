from kahi_unicity_person_graph.graph import (
    CandidateGroup,
    EvidenceEdge,
    build_collision_components,
    build_evidence_components,
)


def test_overlapping_groups_form_one_collision_component():
    groups = [
        CandidateGroup("orcid", "orcid-1", ("a", "b"), 0),
        CandidateGroup("doi", "doi-1", ("b", "c"), 1),
        CandidateGroup("scopus", "scopus-1", ("d", "e"), 2),
    ]

    components = build_collision_components(groups)

    assert [component.member_ids for component in components] == [
        ("a", "b", "c"),
        ("d", "e"),
    ]
    assert [len(component.groups) for component in components] == [2, 1]


def test_components_are_deterministic_when_input_order_changes():
    groups = [
        CandidateGroup("doi", "doi-1", ("c", "b"), 1),
        CandidateGroup("orcid", "orcid-1", ("b", "a"), 0),
    ]

    component = build_collision_components(groups)[0]

    assert component.component_id == "a"
    assert component.member_ids == ("a", "b", "c")
    assert [group.source for group in component.groups] == ["orcid", "doi"]


def test_evidence_components_ignore_unvalidated_raw_group_connections():
    edges = [
        EvidenceEdge("a", "b", "high", 100, (), ()),
        EvidenceEdge("c", "d", "high", 90, (), ()),
    ]

    components = build_evidence_components(edges)

    assert [component.member_ids for component in components] == [
        ("a", "b"),
        ("c", "d"),
    ]


def test_evidence_component_order_is_deterministic():
    edges = [
        EvidenceEdge("b", "c", "high", 90, (), ()),
        EvidenceEdge("a", "b", "high", 100, (), ()),
    ]

    forward = build_evidence_components(edges)[0]
    reverse = build_evidence_components(list(reversed(edges)))[0]

    assert forward == reverse
