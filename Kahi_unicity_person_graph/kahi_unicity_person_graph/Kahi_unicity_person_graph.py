import copy
import json
import re
import uuid
from dataclasses import dataclass
from itertools import combinations
from time import time
from typing import Any, Callable, Dict, List, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase
from kahi_impactu_utils.Utils import (
    compare_author,
    doi_processor,
    normalize_name,
    normalize_names,
    split_names,
)
from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from .graph import (
    CandidateGroup,
    CollisionComponent,
    EvidenceEdge,
    build_evidence_components,
    object_id_key,
)


ID_TASKS = {
    "scienti",
    "linkedin",
    "orcid",
    "publons",
    "researchgate",
    "scholar",
    "scopus",
    "ssrn",
    "wos",
}
TRUSTED_SOURCES = {"staff", "scienti", "minciencias", "scholar"}
NATIONAL_ID_SOURCES = {
    "Cédula de Ciudadanía",
    "Cédula de Extranjería",
}
TARGET_PROVENANCE = {"staff": 0, "scienti": 1, "minciencias": 2}
ANCHOR_SOURCE_PRIORITY = {
    "staff": 0,
    "scienti": 1,
    "minciencias": 2,
    "scholar": 3,
    "orcid": 4,
    "openalex": 5,
}
IDENTIFIER_SCORES = {
    "scienti": 100,
    "orcid": 100,
    "scholar": 96,
    "scopus": 94,
    "wos": 92,
    "publons": 92,
    "researchgate": 90,
    "ssrn": 88,
    "linkedin": 86,
}
EVIDENCE_SOURCE_PRIORITY = {
    source: index
    for index, source in enumerate(
        (
            "scienti",
            "orcid",
            "scholar",
            "scopus",
            "wos",
            "publons",
            "researchgate",
            "ssrn",
            "linkedin",
            "doi",
        )
    )
}
SCALAR_FIELDS = (
    "first_names",
    "last_names",
    "initials",
    "keywords",
    "sex",
    "marital_status",
    "birthplace",
    "birthdate",
)
LIST_FIELDS = (
    "aliases",
    "external_ids",
    "ranking",
    "degrees",
    "subjects",
    "related_works",
)


@dataclass
class MergePlan:
    component_id: str
    target_id: Any
    absorbed_ids: Tuple[Any, ...]
    merged_document: dict
    evidence: Tuple[dict, ...]
    match_details: Tuple[dict, ...]
    anchor_evidence: Tuple[dict, ...]
    confidence: str


@dataclass
class ComponentResult:
    component: CollisionComponent
    snapshot: Dict[Any, dict]
    plans: Tuple[MergePlan, ...]
    review_plans: Tuple[MergePlan, ...]
    rejected_orcid_conflicts: int
    rejected_national_id_conflicts: int
    rejected_identity_conflicts: int
    rejected_name_mismatches: int
    rejected_given_name_conflicts: int
    rejected_weak_bridges: int


@dataclass
class EdgeDiscoveryResult:
    edges: Tuple[EvidenceEdge, ...]
    rejected_orcid_conflicts: int
    rejected_national_id_conflicts: int
    rejected_identity_conflicts: int
    rejected_name_mismatches: int
    rejected_given_name_conflicts: int
    rejected_untrusted_doi_pairs: int
    rejected_invalid_identifier_groups: int


class ComponentResolver:
    """Simulate all groups in one collision component without database writes."""

    ORCID_PATTERN = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")

    def __init__(
        self,
        compare: Callable[[dict, dict, int], bool],
        timestamp: int,
        max_authors_threshold: int = 10,
        single_doi_exact_name_max_authors: int = 50,
    ):
        self.compare = compare
        self.timestamp = timestamp
        self.max_authors_threshold = max_authors_threshold
        self.single_doi_exact_name_max_authors = (
            single_doi_exact_name_max_authors
        )

    def resolve(
        self,
        component: CollisionComponent,
        snapshot: Dict[Any, dict],
    ) -> ComponentResult:
        edges = component.edges
        if not edges and component.groups:
            edges = CandidateEdgeBuilder(
                self.compare,
                self.max_authors_threshold,
                self.single_doi_exact_name_max_authors,
            ).add_groups(
                component.groups,
                snapshot,
            ).finish().edges

        parent = {member_id: member_id for member_id in component.member_ids}
        cluster_members = {member_id: {member_id}
                           for member_id in component.member_ids}
        cluster_edges = {member_id: [] for member_id in component.member_ids}
        rejected_orcid_conflicts = 0
        rejected_national_id_conflicts = 0
        rejected_identity_conflicts = 0
        rejected_name_mismatches = 0
        rejected_given_name_conflicts = 0
        rejected_weak_bridges = 0

        def find(member_id):
            root = parent[member_id]
            if root != member_id:
                parent[member_id] = find(root)
            return parent[member_id]

        def join(left_id, right_id, edge):
            left_root = find(left_id)
            right_root = find(right_id)
            if left_root == right_root:
                return
            if object_id_key(right_root) < object_id_key(left_root):
                left_root, right_root = right_root, left_root
            parent[right_root] = left_root
            cluster_members[left_root].update(cluster_members.pop(right_root))
            cluster_edges[left_root].extend(cluster_edges.pop(right_root))
            cluster_edges[left_root].append(edge)

        high_edges = sorted(
            (edge for edge in edges if edge.confidence == "high"),
            key=self._edge_sort_key,
        )
        for edge in high_edges:
            left_root = find(edge.left_id)
            right_root = find(edge.right_id)
            if left_root == right_root:
                continue
            left_members = cluster_members[left_root]
            right_members = cluster_members[right_root]
            identity_conflicts = self._member_sets_identity_conflicts(
                left_members,
                right_members,
                snapshot,
            )
            if identity_conflicts:
                rejected_identity_conflicts += 1
                rejected_orcid_conflicts += "orcid" in identity_conflicts
                rejected_national_id_conflicts += (
                    "national_id" in identity_conflicts
                )
                continue
            if self._member_sets_name_conflict(
                left_members,
                right_members,
                snapshot,
            ):
                rejected_given_name_conflicts += 1
                rejected_weak_bridges += 1
                continue
            join(edge.left_id, edge.right_id, edge)

        plans = []
        for root_id in sorted(cluster_members, key=object_id_key):
            members = tuple(
                sorted(
                    cluster_members[root_id],
                    key=object_id_key))
            if len(members) < 2:
                continue
            selected_edges = tuple(
                sorted(cluster_edges[root_id], key=self._edge_sort_key)
            )
            match_details = tuple(
                detail
                for edge in selected_edges
                for detail in edge.match_details
            )
            target_id = min(
                members,
                key=lambda member_id: self._anchor_key(
                    member_id,
                    snapshot[member_id],
                ),
            )
            membership_evidence = tuple(
                self._best_direct_match(edge.match_details)
                for edge in selected_edges
            )
            merged_document = copy.deepcopy(snapshot[target_id])
            for member_id in members:
                if member_id != target_id:
                    self._merge_document_values(
                        merged_document, snapshot[member_id])
            evidence = self._evidence_from_edges(selected_edges)
            plans.append(
                MergePlan(
                    component_id=component.component_id,
                    target_id=target_id,
                    absorbed_ids=tuple(
                        member_id for member_id in members if member_id != target_id),
                    merged_document=merged_document,
                    evidence=evidence,
                    match_details=match_details,
                    anchor_evidence=membership_evidence,
                    confidence="high",
                ))

        review_plans = []
        seen_review_pairs = set()
        for edge in sorted(
            (edge for edge in edges if edge.confidence == "medium"),
            key=self._edge_sort_key,
        ):
            if find(edge.left_id) == find(edge.right_id):
                continue
            pair_key = frozenset((edge.left_id, edge.right_id))
            if pair_key in seen_review_pairs:
                continue
            seen_review_pairs.add(pair_key)
            members = tuple(sorted(pair_key, key=object_id_key))
            target_id = min(
                members,
                key=lambda member_id: self._anchor_key(
                    member_id,
                    snapshot[member_id],
                ),
            )
            absorbed_id = next(
                member_id for member_id in members if member_id != target_id
            )
            merged_document = copy.deepcopy(snapshot[target_id])
            self._merge_document_values(merged_document, snapshot[absorbed_id])
            review_plans.append(
                MergePlan(
                    component_id=(
                        f"{component.component_id}: review: "
                        f"{object_id_key(members[0])}: {object_id_key(members[1])}"
                    ),
                    target_id=target_id,
                    absorbed_ids=(absorbed_id,),
                    merged_document=merged_document,
                    evidence=edge.evidence,
                    match_details=edge.match_details,
                    anchor_evidence=(self._best_direct_match(edge.match_details),),
                    confidence="medium",
                )
            )

        return ComponentResult(
            component=component,
            snapshot={
                member_id: snapshot[member_id]
                for member_id in component.member_ids
            },
            plans=tuple(plans),
            review_plans=tuple(review_plans),
            rejected_orcid_conflicts=rejected_orcid_conflicts,
            rejected_national_id_conflicts=rejected_national_id_conflicts,
            rejected_identity_conflicts=rejected_identity_conflicts,
            rejected_name_mismatches=rejected_name_mismatches,
            rejected_given_name_conflicts=rejected_given_name_conflicts,
            rejected_weak_bridges=rejected_weak_bridges,
        )

    @staticmethod
    def _edge_sort_key(edge: EvidenceEdge) -> tuple:
        return (
            -edge.score,
            EVIDENCE_SOURCE_PRIORITY.get(
                edge.match_details[0].get("source", ""),
                99,
            ),
            object_id_key(edge.left_id),
            object_id_key(edge.right_id),
            repr(edge.evidence),
        )

    @classmethod
    def _member_sets_name_conflict(
        cls,
        left_members: set,
        right_members: set,
        snapshot: Dict[Any, dict],
    ) -> bool:
        return any(
            cls._has_name_conflict(snapshot[left_id], snapshot[right_id])
            for left_id in left_members
            for right_id in right_members
        )

    @classmethod
    def _valid_orcid_ids(cls, document: dict) -> set:
        valid_ids = set()
        for external_id in document.get("external_ids", []):
            if not isinstance(external_id, dict):
                continue
            if str(external_id.get("source", "")).lower() != "orcid":
                continue
            value = external_id.get("id")
            if not isinstance(value, str):
                continue
            canonical = value.strip().rstrip("/").rsplit("/", 1)[-1].upper()
            if cls.ORCID_PATTERN.fullmatch(
                    canonical) and cls._valid_orcid_checksum(canonical):
                valid_ids.add(canonical)
        return valid_ids

    @staticmethod
    def _valid_orcid_checksum(orcid: str) -> bool:
        total = 0
        for character in orcid.replace("-", "")[:15]:
            total = (total + int(character)) * 2
        remainder = (12 - total % 11) % 11
        expected = "X" if remainder == 10 else str(remainder)
        return orcid[-1] == expected

    @classmethod
    def _canonical_candidate_key(cls, source: str, value: Any) -> Any:
        source = str(source).lower()
        if value in (None, "", {}, []):
            return None
        if source == "scienti":
            if not isinstance(value, dict):
                return None
            cod_rh = str(value.get("COD_RH", "")).strip()
            return {"COD_RH": cod_rh} if re.fullmatch(
                r"\d{10}", cod_rh) else None
        if source == "orcid":
            document = {"external_ids": [{"source": "orcid", "id": value}]}
            valid = cls._valid_orcid_ids(document)
            return next(iter(valid)) if len(valid) == 1 else None
        if source == "doi":
            if not isinstance(value, str):
                return None
            canonical = doi_processor(value)
            return canonical or None
        if not isinstance(value, str):
            return None

        normalized = value.strip()
        if not normalized or normalized.lower() in {
                "none", "null", "unknown", "n/a"}:
            return None
        parsed = urlparse(normalized)
        query = parse_qs(parsed.query)
        if source == "scholar":
            scholar_id = query.get("user", [""])[0]
            return scholar_id if re.fullmatch(
                r"[A-Za-z0-9_-]{8,20}", scholar_id) else None
        if source == "scopus":
            scopus_id = query.get("authorId", [""])[0]
            if not scopus_id and normalized.isdigit():
                scopus_id = normalized
            return (
                scopus_id
                if re.fullmatch(r"\d{8,12}", scopus_id)
                and set(scopus_id) != {"0"}
                else None
            )
        if source == "researchgate":
            path = parsed.path.rstrip("/")
            return path.lower() if re.match(
                r"^/profile/[^/]+$", path) else None
        return normalized.rstrip("/")

    @classmethod
    def _scienti_ids(cls, document: dict) -> set:
        identifiers = set()
        for external_id in document.get("external_ids", []):
            if not isinstance(external_id, dict):
                continue
            if str(external_id.get("source", "")).lower() != "scienti":
                continue
            canonical = cls._canonical_candidate_key(
                "scienti",
                external_id.get("id"),
            )
            if canonical:
                identifiers.add(canonical["COD_RH"])
        return identifiers

    @classmethod
    def _national_ids(cls, document: dict) -> set:
        """Return comparable national IDs without changing their source field."""
        identifiers = set()
        for external_id in document.get("external_ids", []):
            if not isinstance(external_id, dict):
                continue
            if external_id.get("source") not in NATIONAL_ID_SOURCES:
                continue
            value = external_id.get("id")
            if not isinstance(value, (str, int)):
                continue
            digits = re.sub(r"\D", "", str(value))
            if len(digits) < 5 or set(digits) == {"0"}:
                continue
            identifiers.add(digits.lstrip("0") or "0")
        return identifiers

    @classmethod
    def _authoritative_ids(cls, document: dict) -> Dict[str, set]:
        return {
            "orcid": cls._valid_orcid_ids(document),
            "scienti": cls._scienti_ids(document),
            "national_id": cls._national_ids(document),
        }

    @classmethod
    def _identity_conflict_sources(cls, left: dict, right: dict) -> set:
        left_ids = cls._authoritative_ids(left)
        right_ids = cls._authoritative_ids(right)
        return {
            source
            for source in left_ids
            if len(left_ids[source]) > 1
            or len(right_ids[source]) > 1
            or (
                left_ids[source]
                and right_ids[source]
                and len(left_ids[source] | right_ids[source]) > 1
            )
        }

    @staticmethod
    def _normalized_name_parts(document: dict, field: str) -> set:
        values = document.get(field, [])
        if not values:
            values = split_names(document.get("full_name", "")).get(field, [])
        return {
            token
            for value in normalize_names(values)
            for token in value.split()
            if len(token) > 1
        }

    @staticmethod
    def _normalized_full_name_tokens(document: dict) -> set:
        return {
            token
            for token in normalize_name(document.get("full_name", "")).split()
            if len(token) > 1
        }

    @classmethod
    def _name_variants(cls, document: dict) -> set:
        variants = set()
        full_name = normalize_name(document.get("full_name", ""))
        if full_name and len(full_name.split()) >= 2:
            variants.add(full_name)
        last_names = cls._normalized_name_parts(document, "last_names")
        for alias in document.get("aliases", []):
            if not isinstance(alias, str):
                continue
            normalized = normalize_name(alias)
            tokens = {token for token in normalized.split() if len(token) > 1}
            if len(normalized.split()) >= 2 and tokens.intersection(last_names):
                variants.add(normalized)
        return variants

    @classmethod
    def _shared_name_variants(cls, left: dict, right: dict) -> set:
        return cls._name_variants(left).intersection(cls._name_variants(right))

    @classmethod
    def _name_features(cls, document: dict) -> dict:
        return {
            "full_name": normalize_name(document.get("full_name", "")),
            "full_tokens": cls._normalized_full_name_tokens(document),
            "first_names": cls._normalized_name_parts(document, "first_names"),
            "last_names": cls._normalized_name_parts(document, "last_names"),
            "variants": cls._name_variants(document),
        }

    @classmethod
    def _name_features_conflict(cls, left: dict, right: dict) -> bool:
        if left["full_name"] == right["full_name"]:
            return False
        first_conflict = cls._name_parts_conflict(
            left["first_names"],
            right["first_names"],
        )
        last_conflict = cls._name_parts_conflict(
            left["last_names"],
            right["last_names"],
        )
        left_full = left["full_tokens"]
        right_full = right["full_tokens"]
        if (
            len(left_full) >= 3
            and len(right_full) >= 3
            and len(left_full) == len(right_full)
            and not left_full.issubset(right_full)
            and not right_full.issubset(left_full)
        ):
            return True
        if left["variants"].intersection(right["variants"]):
            return False
        if not first_conflict and not last_conflict:
            return False
        shared_full = left_full.intersection(right_full)
        if len(left_full) >= 4 and len(right_full) >= 4:
            return True
        return len(shared_full) < 2

    @staticmethod
    def _name_parts_conflict(left: set, right: set) -> bool:
        if not left or not right:
            return False
        shared = left & right
        if not shared:
            return True
        return (
            len(left) > 1
            and len(right) > 1
            and not left.issubset(right)
            and not right.issubset(left)
        )

    @classmethod
    def _has_name_conflict(cls, left: dict, right: dict) -> bool:
        return cls._name_features_conflict(
            cls._name_features(left),
            cls._name_features(right),
        )

    @classmethod
    def _member_sets_identity_conflicts(
        cls,
        left_members: set,
        right_members: set,
        snapshot: Dict[Any, dict],
    ) -> set:
        identifiers = {
            "orcid": set(),
            "scienti": set(),
            "national_id": set(),
        }
        for member_id in left_members | right_members:
            document_ids = cls._authoritative_ids(snapshot[member_id])
            for source, values in document_ids.items():
                identifiers[source].update(values)
        return {
            source for source, values in identifiers.items() if len(values) > 1
        }

    @staticmethod
    def _anchor_key(member_id: Any, document: dict) -> Tuple[int, str]:
        priorities = [
            TARGET_PROVENANCE.get(external_id.get("provenance"), 99)
            for external_id in document.get("external_ids", [])
            if isinstance(external_id, dict)
        ]
        priorities.extend(
            ANCHOR_SOURCE_PRIORITY.get(updated.get("source"), 99)
            for updated in document.get("updated", [])
            if isinstance(updated, dict)
        )
        return min(priorities, default=99), object_id_key(member_id)

    @staticmethod
    def _shared_affiliation_ids(left: dict, right: dict) -> set:
        def identifiers(document):
            return {
                json.dumps(
                    affiliation.get("id"),
                    sort_keys=True,
                    default=str,
                    separators=(",", ":"),
                )
                for affiliation in document.get("affiliations", [])
                if isinstance(affiliation, dict)
                and affiliation.get("id") not in (None, "")
            }

        return identifiers(left).intersection(identifiers(right))

    @staticmethod
    def _best_direct_match(matches: Sequence[dict]) -> dict:
        return min(
            matches,
            key=lambda match: (
                0 if match.get("confidence") == "high" else 1,
                -int(match.get("score", 0)),
                EVIDENCE_SOURCE_PRIORITY.get(match.get("source", ""), 99),
                str(match.get("key", "")),
            ),
        )

    @classmethod
    def _match_detail(
        cls,
        group: CandidateGroup,
        left_id: Any,
        right_id: Any,
        left: dict,
        right: dict,
        left_name_features: dict = None,
        right_name_features: dict = None,
    ) -> dict:
        left_name_features = left_name_features or cls._name_features(left)
        right_name_features = right_name_features or cls._name_features(right)
        shared_name_variants = left_name_features["variants"].intersection(
            right_name_features["variants"]
        )
        return {
            "source": group.source,
            "key": group.key,
            "left_id": left_id,
            "right_id": right_id,
            "compare_author": True,
            "direct_doi": group.source == "doi",
            "exact_normalized_name": (
                left_name_features["full_name"]
                == right_name_features["full_name"]
            ),
            "shared_affiliation": bool(cls._shared_affiliation_ids(left, right)),
            "shared_name_variants": sorted(shared_name_variants),
            "shared_name_token_count": len(
                left_name_features["full_tokens"].intersection(
                    right_name_features["full_tokens"]
                )
            ),
            "left_orcids": sorted(cls._valid_orcid_ids(left)),
            "right_orcids": sorted(cls._valid_orcid_ids(right)),
            "left_national_ids": sorted(cls._national_ids(left)),
            "right_national_ids": sorted(cls._national_ids(right)),
            "shared_external_ids": cls._shared_external_ids(left, right),
            "conflicting_external_ids": cls._conflicting_external_ids(left, right),
        }

    @staticmethod
    def _external_ids_by_source(document: dict) -> Dict[str, list]:
        identifiers: Dict[str, list] = {}
        for external_id in document.get("external_ids", []):
            if not isinstance(external_id, dict):
                continue
            source = str(external_id.get("source", "")).lower()
            value = external_id.get("id")
            if not source or value in (None, ""):
                continue
            source_values = identifiers.setdefault(source, [])
            if value not in source_values:
                source_values.append(copy.deepcopy(value))
        return identifiers

    @classmethod
    def _shared_external_ids(cls, left: dict, right: dict) -> List[dict]:
        left_ids = cls._external_ids_by_source(left)
        right_ids = cls._external_ids_by_source(right)
        shared = []
        for source in sorted(left_ids.keys() & right_ids.keys()):
            for value in left_ids[source]:
                if value in right_ids[source]:
                    shared.append(
                        {"source": source, "id": copy.deepcopy(value)})
        return shared

    @classmethod
    def _conflicting_external_ids(cls, left: dict, right: dict) -> List[dict]:
        left_ids = cls._external_ids_by_source(left)
        right_ids = cls._external_ids_by_source(right)
        conflicts = []
        for source in sorted(left_ids.keys() & right_ids.keys()):
            if any(value in right_ids[source] for value in left_ids[source]):
                continue
            conflicts.append(
                {
                    "source": source,
                    "left_ids": copy.deepcopy(left_ids[source]),
                    "right_ids": copy.deepcopy(right_ids[source]),
                }
            )
        return conflicts

    @staticmethod
    def _is_trusted(document: dict) -> bool:
        return any(
            updated.get("source") in TRUSTED_SOURCES
            for updated in document.get("updated", [])
            if isinstance(updated, dict)
        )

    def _merge_document_values(self, target: dict, source: dict) -> None:
        target_updated = target.setdefault("updated", [])
        target_sources = {
            value.get("source") for value in target_updated if isinstance(
                value, dict)}
        for value in source.get("updated", []):
            if not isinstance(value, dict):
                continue
            source_name = value.get("source")
            if source_name and source_name not in target_sources:
                target_updated.append(
                    {"source": source_name, "time": self.timestamp})
                target_sources.add(source_name)

        for field in SCALAR_FIELDS:
            if not target.get(field) and source.get(field):
                target[field] = copy.deepcopy(source[field])

        for field in LIST_FIELDS:
            target_values = target.setdefault(field, [])
            for value in source.get(field, []):
                if value not in target_values:
                    target_values.append(copy.deepcopy(value))

    @staticmethod
    def _evidence_from_edges(
        edges: Sequence[EvidenceEdge],
    ) -> Tuple[dict, ...]:
        evidence = []
        seen = set()
        for edge in edges:
            for item in edge.evidence:
                marker = (item["source"], repr(item["key"]))
                if marker not in seen:
                    evidence.append(copy.deepcopy(item))
                    seen.add(marker)
        return tuple(evidence)


class CandidateEdgeBuilder:
    """Accumulate validated pair evidence before graph components are built."""

    def __init__(
        self,
        compare: Callable[[dict, dict, int], bool],
        max_authors_threshold: int = 10,
        single_doi_exact_name_max_authors: int = 50,
    ):
        self.compare = compare
        self.max_authors_threshold = max_authors_threshold
        self.single_doi_exact_name_max_authors = (
            single_doi_exact_name_max_authors
        )
        self.pair_details: Dict[Tuple[Any, Any], list] = {}
        self.rejected_orcid_conflicts = 0
        self.rejected_national_id_conflicts = 0
        self.rejected_identity_conflicts = 0
        self.rejected_name_mismatches = 0
        self.rejected_given_name_conflicts = 0
        self.rejected_untrusted_doi_pairs = 0
        self.rejected_invalid_identifier_groups = 0

    def add_groups(
        self,
        groups: Sequence[CandidateGroup],
        snapshot: Dict[Any, dict],
    ) -> "CandidateEdgeBuilder":
        name_features = {
            member_id: ComponentResolver._name_features(document)
            for member_id, document in snapshot.items()
        }
        for group in groups:
            canonical_key = ComponentResolver._canonical_candidate_key(
                group.source,
                group.key,
            )
            if canonical_key is None:
                self.rejected_invalid_identifier_groups += 1
                continue
            canonical_group = CandidateGroup(
                group.source,
                canonical_key,
                group.member_ids,
                group.order,
                group.work_author_count,
            )
            author_count = (
                group.work_author_count
                if group.source == "doi" and group.work_author_count is not None
                else len(group.member_ids)
            )
            for left_id, right_id in combinations(group.member_ids, 2):
                left = snapshot.get(left_id)
                right = snapshot.get(right_id)
                if left is None or right is None:
                    continue
                if group.source == "doi" and not (
                    ComponentResolver._is_trusted(left)
                    or ComponentResolver._is_trusted(right)
                ):
                    self.rejected_untrusted_doi_pairs += 1
                    continue
                identity_conflicts = ComponentResolver._identity_conflict_sources(
                    left, right, )
                if identity_conflicts:
                    self.rejected_identity_conflicts += 1
                    self.rejected_orcid_conflicts += "orcid" in identity_conflicts
                    self.rejected_national_id_conflicts += (
                        "national_id" in identity_conflicts
                    )
                    continue
                compare_match = bool(self.compare(left, right, author_count))
                left_name_features = name_features[left_id]
                right_name_features = name_features[right_id]
                alias_match = bool(
                    left_name_features["variants"].intersection(
                        right_name_features["variants"]
                    )
                )
                if not compare_match and not alias_match:
                    self.rejected_name_mismatches += 1
                    continue
                if ComponentResolver._name_features_conflict(
                    left_name_features,
                    right_name_features,
                ):
                    self.rejected_given_name_conflicts += 1
                    continue

                pair = tuple(sorted((left_id, right_id), key=object_id_key))
                detail = ComponentResolver._match_detail(
                    canonical_group,
                    pair[0],
                    pair[1],
                    snapshot[pair[0]],
                    snapshot[pair[1]],
                    name_features[pair[0]],
                    name_features[pair[1]],
                )
                detail["compare_author"] = compare_match
                detail["alias_name_match"] = alias_match
                detail["work_author_count"] = group.work_author_count
                details = self.pair_details.setdefault(pair, [])
                marker = (detail["source"], repr(detail["key"]))
                if not any(
                    (existing["source"], repr(existing["key"])) == marker
                    for existing in details
                ):
                    details.append(detail)
        return self

    def _edge_confidence(
            self, details: Sequence[dict]) -> Tuple[str, int, List[str]]:
        identifier_sources = {
            detail["source"]
            for detail in details
            if detail["source"] in IDENTIFIER_SCORES
        }
        if identifier_sources:
            source = max(identifier_sources, key=IDENTIFIER_SCORES.__getitem__)
            return "high", IDENTIFIER_SCORES[source], [
                f"validated_shared_{source}"]

        doi_keys = {
            repr(detail["key"])
            for detail in details
            if detail["source"] == "doi"
        }
        alias_only_match = any(
            detail.get("alias_name_match") and not detail.get("compare_author")
            for detail in details
        )
        work_author_counts = {
            detail.get("work_author_count")
            for detail in details
            if detail["source"] == "doi"
            and isinstance(detail.get("work_author_count"), int)
        }
        single_doi_author_count = (
            next(iter(work_author_counts))
            if len(doi_keys) == 1 and len(work_author_counts) == 1
            else None
        )
        if alias_only_match:
            shared_name_token_count = max(
                (
                    int(detail.get("shared_name_token_count", 0))
                    for detail in details
                ),
                default=0,
            )
            if (
                len(doi_keys) == 1
                and single_doi_author_count is not None
                and single_doi_author_count <= self.max_authors_threshold
                and shared_name_token_count >= 3
            ):
                return "high", 80, ["three_shared_name_tokens", "shared_doi"]
            longest_shared_variant = max(
                (
                    len(variant.split())
                    for detail in details
                    for variant in detail.get("shared_name_variants", [])
                ),
                default=0,
            )
            if len(doi_keys) >= 4 and longest_shared_variant >= 3:
                return "high", 80, [
                    "corroborated_alias", "multiple_shared_dois"]
            return "medium", 70, ["alias_only_name_match", "shared_doi"]

        exact_name = any(detail["exact_normalized_name"] for detail in details)
        shared_affiliation = any(
            detail["shared_affiliation"] for detail in details)
        if len(doi_keys) >= 2:
            return "high", 90, ["multiple_shared_dois"]
        if (
            doi_keys
            and exact_name
            and single_doi_author_count is not None
            and single_doi_author_count
            <= self.single_doi_exact_name_max_authors
        ):
            return "high", 85, ["exact_normalized_name", "shared_doi"]
        if (
            doi_keys
            and single_doi_author_count is not None
            and single_doi_author_count <= self.max_authors_threshold
            and any(detail.get("compare_author") for detail in details)
        ):
            return "high", 75, ["small_work_compatible_name", "shared_doi"]
        reasons = ["compatible_name"]
        if doi_keys:
            reasons.append("shared_doi")
        if shared_affiliation:
            reasons.append("shared_affiliation_support")
        return "medium", 65 if shared_affiliation else 60, reasons

    def finish(self) -> EdgeDiscoveryResult:
        edges = []
        for (left_id, right_id), raw_details in self.pair_details.items():
            confidence, score, reasons = self._edge_confidence(raw_details)
            details = tuple(
                {
                    **detail,
                    "confidence": confidence,
                    "score": score,
                    "confidence_reasons": list(reasons),
                }
                for detail in sorted(
                    raw_details,
                    key=lambda item: (
                        EVIDENCE_SOURCE_PRIORITY.get(item["source"], 99),
                        repr(item["key"]),
                    ),
                )
            )
            evidence = tuple(
                {"source": detail["source"], "key": copy.deepcopy(detail["key"])}
                for detail in details
            )
            edges.append(
                EvidenceEdge(
                    left_id=left_id,
                    right_id=right_id,
                    confidence=confidence,
                    score=score,
                    evidence=evidence,
                    match_details=details,
                )
            )
        edges.sort(key=ComponentResolver._edge_sort_key)
        return EdgeDiscoveryResult(
            edges=tuple(edges),
            rejected_orcid_conflicts=self.rejected_orcid_conflicts,
            rejected_national_id_conflicts=(
                self.rejected_national_id_conflicts
            ),
            rejected_identity_conflicts=self.rejected_identity_conflicts,
            rejected_name_mismatches=self.rejected_name_mismatches,
            rejected_given_name_conflicts=self.rejected_given_name_conflicts,
            rejected_untrusted_doi_pairs=self.rejected_untrusted_doi_pairs,
            rejected_invalid_identifier_groups=(
                self.rejected_invalid_identifier_groups
            ),
        )


class Kahi_unicity_person_graph(KahiBase):
    def __init__(self, config):
        self.config = config
        plugin_config = config["unicity_person_graph"]

        self.client = MongoClient(config["database_url"])
        self.db = self.client[config["database_name"]]
        self.collection_name = plugin_config["collection_name"]
        if self.collection_name not in self.db.list_collection_names():
            raise ValueError(
                f"Collection {self.collection_name!r} was found not in "
                f"database {config['database_name']!r}"
            )

        self.collection = self.db[self.collection_name]
        self.merged_collection = self.db[
            plugin_config.get(
                "merged_collection_name",
                f"{self.collection_name}_graph_merged",
            )
        ]
        self.sets_collection = self.db[
            plugin_config.get(
                "sets_collection_name",
                f"{self.collection_name}_graph_merged_sets",
            )
        ]
        self.runs_collection = self.db[
            plugin_config.get(
                "runs_collection_name",
                f"{self.collection_name}_graph_runs",
            )
        ]

        tasks = plugin_config.get("task", [])
        self.tasks = [tasks] if isinstance(tasks, str) else list(tasks or [])
        invalid_tasks = set(self.tasks).difference(ID_TASKS | {"doi"})
        if invalid_tasks:
            raise ValueError(
                f"Unsupported unicity tasks: {sorted(invalid_tasks)}")
        if not self.tasks:
            raise ValueError("At least one unicity task must be configured")

        self.n_jobs = max(1, int(plugin_config.get("num_jobs", 1)))
        self.verbose = int(plugin_config.get("verbose", 0))
        self.max_authors_threshold = max(
            1, int(plugin_config.get("max_authors_threshold", 10))
        )
        self.single_doi_exact_name_max_authors = max(
            self.max_authors_threshold,
            int(
                plugin_config.get(
                    "single_doi_exact_name_max_authors",
                    50,
                )
            ),
        )
        self.max_profiles_per_doi = max(
            0, int(plugin_config.get("max_profiles_per_doi", 100))
        )
        self.dry_run = bool(plugin_config.get("dry_run", True))
        self.snapshot_batch_size = max(
            1, int(plugin_config.get("snapshot_batch_size", 5000))
        )
        self.component_batch_size = max(
            1, int(plugin_config.get("component_batch_size", 100))
        )
        self.audit_batch_size = max(
            1, int(plugin_config.get("audit_batch_size", 100))
        )
        self.candidate_group_batch_size = max(
            1, int(plugin_config.get("candidate_group_batch_size", 1000))
        )
        self.compare = compare_author

        self.collection.create_index("external_ids.id")
        self.collection.create_index("related_works.id")
        self.sets_collection.create_index([("run_id", 1), ("component_id", 1)])

    @staticmethod
    def _mongo_canonical_doi_expression(field: str) -> dict:
        """Build the Mongo expression equivalent to the DOI canonicalizer."""
        return {
            "$let": {
                "vars": {
                    "candidate": {
                        "$replaceAll": {
                            "input": {
                                "$replaceAll": {
                                    "input": {
                                        "$toLower": {
                                            "$trim": {"input": field}
                                        }
                                    },
                                    "find": "%2f",
                                    "replacement": "/",
                                }
                            },
                            "find": " ",
                            "replacement": "",
                        }
                    }
                },
                "in": {
                    "$let": {
                        "vars": {
                            "found": {
                                "$regexFind": {
                                    "input": "$$candidate",
                                    "regex": r"10\.[0-9]{3,}/[^\s]+",
                                }
                            }
                        },
                        "in": {
                            "$cond": [
                                {"$ne": ["$$found", None]},
                                {
                                    "$concat": [
                                        "https://doi.org/",
                                        {
                                            "$rtrim": {
                                                "input": "$$found.match",
                                                "chars": ".",
                                            }
                                        },
                                    ]
                                },
                                None,
                            ]
                        },
                    }
                },
            }
        }

    @staticmethod
    def _effective_author_count_expression() -> dict:
        return {
            "$cond": [
                {
                    "$and": [
                        {"$isNumber": "$author_count"},
                        {"$gt": ["$author_count", 0]},
                    ]
                },
                {"$toInt": "$author_count"},
                {
                    "$cond": [
                        {"$isArray": "$authors"},
                        {"$size": "$authors"},
                        None,
                    ]
                },
            ]
        }

    def discover_candidate_groups(self) -> List[CandidateGroup]:
        groups = []
        order = 0

        for task in (task for task in self.tasks if task != "doi"):
            pipeline = [
                {"$match": {"external_ids.source": task}},
                {"$unwind": "$external_ids"},
                {"$match": {"external_ids.source": task}},
                {
                    "$group": {
                        "_id": "$external_ids.id",
                        "member_ids": {"$addToSet": "$_id"},
                    }
                },
                {"$match": {"$expr": {"$gt": [{"$size": "$member_ids"}, 1]}}},
                {"$sort": {"_id": 1}},
            ]
            for record in self.collection.aggregate(
                    pipeline, allowDiskUse=True):
                member_ids = tuple(
                    sorted(
                        record["member_ids"],
                        key=object_id_key))
                groups.append(
                    CandidateGroup(
                        task,
                        record["_id"],
                        member_ids,
                        order))
                order += 1

        if "doi" in self.tasks:
            size_conditions = [{"$gt": [{"$size": "$member_ids"}, 1]}]
            if self.max_profiles_per_doi > 0:
                size_conditions.append(
                    {
                        "$lte": [
                            {"$size": "$member_ids"},
                            self.max_profiles_per_doi,
                        ]
                    }
                )
            pipeline = [
                {"$match": {"related_works.source": "doi"}},
                {"$unwind": "$related_works"},
                {
                    "$match": {
                        "related_works.source": "doi",
                        "related_works.id": {"$type": "string"},
                    }
                },
                {
                    "$set": {
                        "canonical_doi": self._mongo_canonical_doi_expression(
                            "$related_works.id"
                        )
                    }
                },
                {"$match": {"canonical_doi": {"$ne": None}}},
                {
                    "$group": {
                        "_id": "$canonical_doi",
                        "member_ids": {"$addToSet": "$_id"},
                    }
                },
                {"$match": {"$expr": {"$and": size_conditions}}},
                {
                    "$lookup": {
                        "from": "works",
                        "localField": "_id",
                        "foreignField": "external_ids.id",
                        "pipeline": [
                            {
                                "$project": {
                                    "_id": 0,
                                    "count": (
                                        self._effective_author_count_expression()
                                    ),
                                }
                            }
                        ],
                        "as": "matched_works",
                    }
                },
                {
                    "$set": {
                        "work_author_count": {
                            "$max": "$matched_works.count"
                        }
                    }
                },
                {"$sort": {"_id": 1}},
            ]
            for record in self.collection.aggregate(
                    pipeline, allowDiskUse=True):
                member_ids = tuple(
                    sorted(
                        record["member_ids"],
                        key=object_id_key))
                groups.append(
                    CandidateGroup(
                        "doi",
                        record["_id"],
                        member_ids,
                        order,
                        record.get("work_author_count"),
                    )
                )
                order += 1

        return groups

    def discover_candidate_edges(
        self,
        groups: Sequence[CandidateGroup],
    ) -> EdgeDiscoveryResult:
        builder = CandidateEdgeBuilder(
            self.compare,
            self.max_authors_threshold,
            self.single_doi_exact_name_max_authors,
        )
        projection = {
            "full_name": 1,
            "first_names": 1,
            "last_names": 1,
            "initials": 1,
            "aliases": 1,
            "external_ids": 1,
            "affiliations": 1,
            "updated": 1,
        }
        for start in range(0, len(groups), self.candidate_group_batch_size):
            batch = groups[start:start + self.candidate_group_batch_size]
            member_ids = {
                member_id for group in batch for member_id in group.member_ids
            }
            snapshot = {
                document["_id"]: document
                for document in self.collection.find(
                    {"_id": {"$in": list(member_ids)}},
                    projection,
                )
            }
            builder.add_groups(batch, snapshot)
        return builder.finish()

    def run(self):
        run_id = uuid.uuid4().hex
        timestamp = int(time())
        self.runs_collection.insert_one(
            {
                "_id": run_id,
                "status": "running",
                "dry_run": self.dry_run,
                "started_at": timestamp,
                "tasks": self.tasks,
                "num_jobs": self.n_jobs,
                "component_batch_size": self.component_batch_size,
                "audit_batch_size": self.audit_batch_size,
                "candidate_group_batch_size": self.candidate_group_batch_size,
                "max_authors_threshold": self.max_authors_threshold,
                "single_doi_exact_name_max_authors": (
                    self.single_doi_exact_name_max_authors
                ),
                "max_profiles_per_doi": self.max_profiles_per_doi,
            }
        )

        try:
            groups = self.discover_candidate_groups()
            edge_discovery = self.discover_candidate_edges(groups)
            components = build_evidence_components(edge_discovery.edges)
            self._validate_disjoint_components(components)
            resolver = ComponentResolver(
                self.compare,
                timestamp,
                self.max_authors_threshold,
                self.single_doi_exact_name_max_authors,
            )
            if not self.dry_run:
                self._require_transaction_support()

            plan_count = 0
            review_plan_count = 0
            absorbed_count = 0
            high_confidence_plans = 0
            medium_confidence_plans = 0
            high_confidence_absorbed_documents = 0
            rejected_orcid_conflicts = edge_discovery.rejected_orcid_conflicts
            rejected_national_id_conflicts = (
                edge_discovery.rejected_national_id_conflicts
            )
            rejected_identity_conflicts = edge_discovery.rejected_identity_conflicts
            rejected_name_mismatches = edge_discovery.rejected_name_mismatches
            rejected_given_name_conflicts = (
                edge_discovery.rejected_given_name_conflicts
            )
            rejected_weak_bridges = 0

            for start in range(0, len(components), self.component_batch_size):
                component_batch = components[start:start +
                                             self.component_batch_size]
                snapshot = self._load_snapshot(component_batch)
                results = Parallel(
                    n_jobs=self.n_jobs,
                    verbose=self.verbose,
                    backend="threading",
                )(
                    delayed(resolver.resolve)(component, snapshot)
                    for component in component_batch
                )
                self._validate_disjoint_results(results)

                if self.dry_run:
                    self._store_dry_run(results, run_id)
                else:
                    self._store_review_plans(results, run_id)
                    Parallel(
                        n_jobs=self.n_jobs,
                        verbose=self.verbose,
                        backend="threading",
                    )(
                        delayed(self._apply_component)(result, run_id)
                        for result in results
                        if result.plans
                    )

                plan_count += sum(len(result.plans) for result in results)
                review_plan_count += sum(
                    len(result.review_plans) for result in results
                )
                absorbed_count += sum(
                    len(plan.absorbed_ids)
                    for result in results
                    for plan in result.plans
                )
                high_confidence_plans += sum(
                    plan.confidence == "high"
                    for result in results
                    for plan in result.plans
                )
                medium_confidence_plans += sum(
                    len(result.review_plans) for result in results
                )
                high_confidence_absorbed_documents += sum(
                    len(plan.absorbed_ids)
                    for result in results
                    for plan in result.plans
                    if plan.confidence == "high"
                )
                rejected_orcid_conflicts += sum(
                    result.rejected_orcid_conflicts for result in results
                )
                rejected_national_id_conflicts += sum(
                    result.rejected_national_id_conflicts
                    for result in results
                )
                rejected_identity_conflicts += sum(
                    result.rejected_identity_conflicts for result in results
                )
                rejected_name_mismatches += sum(
                    result.rejected_name_mismatches for result in results
                )
                rejected_given_name_conflicts += sum(
                    result.rejected_given_name_conflicts for result in results
                )
                rejected_weak_bridges += sum(
                    result.rejected_weak_bridges for result in results
                )
                processed_components = start + len(component_batch)
                self.runs_collection.update_one(
                    {"_id": run_id},
                    {
                        "$set": {
                            "candidate_groups": len(groups),
                            "candidate_edges": len(edge_discovery.edges),
                            "components": len(components),
                            "processed_components": processed_components,
                            "merge_plans": plan_count,
                            "review_plans": review_plan_count,
                            "absorbed_documents": absorbed_count,
                            "high_confidence_plans": high_confidence_plans,
                            "medium_confidence_plans": medium_confidence_plans,
                            "high_confidence_absorbed_documents": (
                                high_confidence_absorbed_documents
                            ),
                            "rejected_orcid_conflicts": rejected_orcid_conflicts,
                            "rejected_national_id_conflicts": (
                                rejected_national_id_conflicts
                            ),
                            "rejected_identity_conflicts": (
                                rejected_identity_conflicts
                            ),
                            "rejected_name_mismatches": rejected_name_mismatches,
                            "rejected_given_name_conflicts": (
                                rejected_given_name_conflicts
                            ),
                            "rejected_weak_bridges": rejected_weak_bridges,
                            "rejected_untrusted_doi_pairs": (
                                edge_discovery.rejected_untrusted_doi_pairs
                            ),
                            "rejected_invalid_identifier_groups": (
                                edge_discovery.rejected_invalid_identifier_groups
                            ),
                        }
                    },
                )

            self.runs_collection.update_one(
                {"_id": run_id},
                {
                    "$set": {
                        "status": "planned" if self.dry_run else "applied",
                        "finished_at": int(time()),
                        "candidate_groups": len(groups),
                        "candidate_edges": len(edge_discovery.edges),
                        "components": len(components),
                        "processed_components": len(components),
                        "merge_plans": plan_count,
                        "review_plans": review_plan_count,
                        "absorbed_documents": absorbed_count,
                        "high_confidence_plans": high_confidence_plans,
                        "medium_confidence_plans": medium_confidence_plans,
                        "high_confidence_absorbed_documents": (
                            high_confidence_absorbed_documents
                        ),
                        "rejected_orcid_conflicts": rejected_orcid_conflicts,
                        "rejected_national_id_conflicts": (
                            rejected_national_id_conflicts
                        ),
                        "rejected_identity_conflicts": rejected_identity_conflicts,
                        "rejected_name_mismatches": rejected_name_mismatches,
                        "rejected_given_name_conflicts": (
                            rejected_given_name_conflicts
                        ),
                        "rejected_weak_bridges": rejected_weak_bridges,
                        "rejected_untrusted_doi_pairs": (
                            edge_discovery.rejected_untrusted_doi_pairs
                        ),
                        "rejected_invalid_identifier_groups": (
                            edge_discovery.rejected_invalid_identifier_groups
                        ),
                    }
                },
            )
            return 0
        except Exception as error:
            self.runs_collection.update_one(
                {"_id": run_id},
                {
                    "$set": {
                        "status": "failed",
                        "finished_at": int(time()),
                        "error": str(error),
                    }
                },
            )
            raise

    @staticmethod
    def _validate_disjoint_components(
        components: Sequence[CollisionComponent],
    ) -> None:
        member_ids = set()
        for component in components:
            current_ids = set(component.member_ids)
            overlap = member_ids.intersection(current_ids)
            if overlap:
                raise RuntimeError(
                    "Collision components overlap: "
                    + ", ".join(sorted(map(str, overlap)))
                )
            member_ids.update(current_ids)

    def _load_snapshot(
        self,
        components: Sequence[CollisionComponent],
    ) -> Dict[Any, dict]:
        member_ids = {
            member_id for component in components for member_id in component.member_ids}
        if not member_ids:
            return {}
        snapshot = {}
        ordered_ids = sorted(member_ids, key=str)
        for start in range(0, len(ordered_ids), self.snapshot_batch_size):
            batch = ordered_ids[start:start + self.snapshot_batch_size]
            snapshot.update(
                (document["_id"], document)
                for document in self.collection.find({"_id": {"$in": batch}})
            )
        missing = member_ids.difference(snapshot)
        if missing:
            raise RuntimeError(
                "Person documents changed during discovery; missing IDs: "
                + ", ".join(sorted(map(str, missing)))
            )
        return snapshot

    @staticmethod
    def _validate_disjoint_results(results: Sequence[ComponentResult]) -> None:
        component_ids = set()
        planned_ids = set()
        for result in results:
            current_component_ids = set(result.component.member_ids)
            overlap = component_ids.intersection(current_component_ids)
            if overlap:
                raise RuntimeError(
                    "Collision components overlap: "
                    + ", ".join(sorted(map(str, overlap)))
                )
            component_ids.update(current_component_ids)

            for plan in result.plans:
                if plan.confidence != "high":
                    raise RuntimeError(
                        "An automatic merge plan is not high confidence")
                current_plan_ids = {plan.target_id, *plan.absorbed_ids}
                plan_overlap = planned_ids.intersection(current_plan_ids)
                if plan_overlap:
                    raise RuntimeError(
                        "A person appears in more than one merge plan: "
                        + ", ".join(sorted(map(str, plan_overlap)))
                    )
                planned_ids.update(current_plan_ids)

                identity_conflicts = (
                    ComponentResolver._member_sets_identity_conflicts(
                        current_plan_ids,
                        set(),
                        result.snapshot,
                    )
                )
                if identity_conflicts:
                    raise RuntimeError(
                        "A merge plan contains conflicting authoritative "
                        "identifiers: "
                        + ", ".join(sorted(identity_conflicts))
                    )

                for match in plan.match_details:
                    if not (
                        match.get("compare_author")
                        or match.get("alias_name_match")
                    ):
                        raise RuntimeError(
                            "A merge plan contains a rejected name match")
                    if match.get("source") != "doi":
                        continue
                    if not match.get("direct_doi"):
                        raise RuntimeError(
                            "A DOI merge is not marked as direct")
            for plan in result.review_plans:
                if plan.confidence != "medium":
                    raise RuntimeError(
                        "A review plan is not medium confidence")
                current_plan_ids = {plan.target_id, *plan.absorbed_ids}
                identity_conflicts = ComponentResolver._member_sets_identity_conflicts(
                    current_plan_ids, set(), result.snapshot, )
                if identity_conflicts:
                    raise RuntimeError(
                        "A review plan contains conflicting authoritative identifiers"
                    )

    def _store_dry_run(
        self,
        results: Sequence[ComponentResult],
        run_id: str,
    ) -> None:
        records = []
        for result in results:
            for plan in result.plans:
                records.append(self._audit_record(plan, run_id, "planned"))
                if len(records) >= self.audit_batch_size:
                    self.sets_collection.insert_many(records, ordered=False)
                    records.clear()
            for plan in result.review_plans:
                records.append(self._audit_record(plan, run_id, "review"))
                if len(records) >= self.audit_batch_size:
                    self.sets_collection.insert_many(records, ordered=False)
                    records.clear()
        if records:
            self.sets_collection.insert_many(records, ordered=False)

    def _store_review_plans(
        self,
        results: Sequence[ComponentResult],
        run_id: str,
    ) -> None:
        records = []
        for result in results:
            for plan in result.review_plans:
                records.append(self._audit_record(plan, run_id, "review"))
                if len(records) >= self.audit_batch_size:
                    self.sets_collection.insert_many(records, ordered=False)
                    records.clear()
        if records:
            self.sets_collection.insert_many(records, ordered=False)

    def _apply_component(self, result: ComponentResult, run_id: str) -> None:
        applicable_plans = [
            plan for plan in result.plans if plan.confidence == "high"
        ]
        if not applicable_plans:
            return

        def transaction(session):
            current_documents = {
                document["_id"]: document
                for document in self.collection.find(
                    {"_id": {"$in": list(result.component.member_ids)}},
                    session=session,
                )
            }
            if current_documents != result.snapshot:
                raise RuntimeError(
                    f"Component {result.component.component_id} changed after discovery")

            for plan in applicable_plans:
                for absorbed_id in plan.absorbed_ids:
                    self.merged_collection.replace_one(
                        {"_id": absorbed_id},
                        result.snapshot[absorbed_id],
                        upsert=True,
                        session=session,
                    )
                self.collection.replace_one(
                    {"_id": plan.target_id},
                    plan.merged_document,
                    session=session,
                )
                self.collection.delete_many(
                    {"_id": {"$in": list(plan.absorbed_ids)}},
                    session=session,
                )
                audit = self._audit_record(plan, run_id, "applied")
                self.sets_collection.replace_one(
                    {"_id": audit["_id"]},
                    audit,
                    upsert=True,
                    session=session,
                )

        with self.client.start_session() as session:
            session.with_transaction(
                transaction,
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern("majority"),
            )

    @staticmethod
    def _audit_record(plan: MergePlan, run_id: str, status: str) -> dict:
        record = {
            "_id": f"{run_id}: {plan.component_id}: {plan.target_id}",
            "run_id": run_id,
            "component_id": plan.component_id,
            "status": status,
            "target_author": {
                "_id": plan.target_id,
                "full_name": plan.merged_document.get("full_name", ""),
            },
            "absorbed": list(plan.absorbed_ids),
            "set": [plan.target_id, *plan.absorbed_ids],
            "evidence": list(plan.evidence),
            "match_details": list(plan.match_details),
            "anchor_evidence": list(plan.anchor_evidence),
            "confidence": plan.confidence,
        }
        if status in {"planned", "review"}:
            record["proposed_document"] = plan.merged_document
        return record

    def _require_transaction_support(self) -> None:
        hello = self.client.admin.command("hello")
        if not hello.get("setName") and hello.get("msg") != "isdbgrid":
            raise RuntimeError(
                "dry_run=false requires MongoDB transaction support "
                "(replica set or sharded cluster)"
            )
