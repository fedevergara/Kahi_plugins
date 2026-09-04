from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def object_id_key(value: Any) -> str:
    return str(value)


@dataclass(frozen=True)
class CandidateGroup:
    source: str
    key: Any
    member_ids: Tuple[Any, ...]
    order: int
    work_author_count: Optional[int] = None


@dataclass(frozen=True)
class EvidenceEdge:
    left_id: Any
    right_id: Any
    confidence: str
    score: int
    evidence: Tuple[dict, ...]
    match_details: Tuple[dict, ...]


@dataclass(frozen=True)
class CollisionComponent:
    component_id: str
    member_ids: Tuple[Any, ...]
    groups: Tuple[CandidateGroup, ...] = ()
    edges: Tuple[EvidenceEdge, ...] = ()


class UnionFind:
    def __init__(self, items: Iterable[Any] = ()):
        self.parent = {}
        for item in items:
            self.add(item)

    def add(self, item: Any) -> None:
        self.parent.setdefault(item, item)

    def find(self, item: Any) -> Any:
        self.add(item)
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, left: Any, right: Any) -> Any:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return left_root

        if object_id_key(right_root) < object_id_key(left_root):
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root
        return left_root


def build_collision_components(
    groups: Sequence[CandidateGroup],
) -> List[CollisionComponent]:
    """Join every candidate group that shares at least one person document."""
    union_find = UnionFind()
    for group in groups:
        if not group.member_ids:
            continue
        anchor = group.member_ids[0]
        union_find.add(anchor)
        for member_id in group.member_ids[1:]:
            union_find.union(anchor, member_id)

    members_by_root: Dict[Any, set] = {}
    for member_id in union_find.parent:
        root = union_find.find(member_id)
        members_by_root.setdefault(root, set()).add(member_id)

    groups_by_root: Dict[Any, list] = {}
    for group in groups:
        if group.member_ids:
            root = union_find.find(group.member_ids[0])
            groups_by_root.setdefault(root, []).append(group)

    components = []
    for root, member_ids in members_by_root.items():
        ordered_members = tuple(sorted(member_ids, key=object_id_key))
        ordered_groups = tuple(
            sorted(
                groups_by_root[root],
                key=lambda group: (group.order, group.source, str(group.key)),
            )
        )
        components.append(
            CollisionComponent(
                component_id=object_id_key(ordered_members[0]),
                member_ids=ordered_members,
                groups=ordered_groups,
            )
        )

    return sorted(components, key=lambda component: component.component_id)


def build_evidence_components(
    edges: Sequence[EvidenceEdge],
) -> List[CollisionComponent]:
    """Build components only from identity edges that passed pair validation."""
    union_find = UnionFind()
    for edge in edges:
        union_find.union(edge.left_id, edge.right_id)

    members_by_root: Dict[Any, set] = {}
    for member_id in union_find.parent:
        root = union_find.find(member_id)
        members_by_root.setdefault(root, set()).add(member_id)

    edges_by_root: Dict[Any, list] = {}
    for edge in edges:
        root = union_find.find(edge.left_id)
        edges_by_root.setdefault(root, []).append(edge)

    components = []
    for root, member_ids in members_by_root.items():
        ordered_members = tuple(sorted(member_ids, key=object_id_key))
        ordered_edges = tuple(
            sorted(
                edges_by_root[root],
                key=lambda edge: (
                    -edge.score,
                    object_id_key(edge.left_id),
                    object_id_key(edge.right_id),
                ),
            )
        )
        components.append(
            CollisionComponent(
                component_id=object_id_key(ordered_members[0]),
                member_ids=ordered_members,
                edges=ordered_edges,
            )
        )

    return sorted(components, key=lambda component: component.component_id)
