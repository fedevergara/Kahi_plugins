"""Promote enriched person.related_works records into the works collection."""

from copy import deepcopy
from hashlib import sha256
from time import time
from unicodedata import normalize as unicode_normalize
import re
from kahi_minciencias_opendata_works.process_one import run_es_operation

from kahi_impactu_utils.Utils import doi_processor, lang_poll
from thefuzz import fuzz


def normalize_title(value):
    value = unicode_normalize("NFKD", str(value or ""))
    value = "".join(char for char in value if not 0x300 <= ord(char) <= 0x36F)
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def normalize_doi(value):
    if not isinstance(value, str) or not value.strip():
        return ""
    return doi_processor(value.strip()) or ""


def work_fingerprint(related_work):
    """Return a stable ingestion identity; it is not a bibliographic ID."""
    identity = "|".join(
        [
            normalize_title(related_work.get("title")),
            str(related_work.get("year") or ""),
            normalize_title(
                related_work.get("type_impactu")
                or related_work.get("product_type")
            ),
        ]
    )
    return sha256(identity.encode("utf-8")).hexdigest()


def _append_unique(target, values, key=lambda value: value):
    existing = {key(value) for value in target}
    for value in values:
        value_key = key(value)
        if value_key not in existing:
            target.append(deepcopy(value))
            existing.add(value_key)


def _related_dois(related_work):
    candidates = list(related_work.get("doi") or [])
    if related_work.get("source") == "doi":
        candidates.append(related_work.get("id"))
    return [doi for doi in (normalize_doi(value)
                            for value in candidates) if doi]


def _author_from_person(person, advisor_role=""):
    author = {
        "id": person["_id"],
        "full_name": person.get("full_name", ""),
        "affiliations": deepcopy(person.get("affiliations", [])),
    }
    if advisor_role:
        author["type"] = advisor_role
    return author


def collect_related_works(person_collection):
    """Group equivalent references from different people into work candidates."""
    candidates = {}
    doi_to_key = {}
    projection = {"full_name": 1, "affiliations": 1, "related_works": 1}
    for person in person_collection.find(
        {"related_works.title": {"$exists": True, "$ne": ""}}, projection
    ):
        for related_work in person.get("related_works", []):
            title = str(related_work.get("title") or "").strip()
            if not normalize_title(title):
                continue
            fingerprint = work_fingerprint(related_work)
            dois = _related_dois(related_work)
            key = next(
                (doi_to_key[doi] for doi in dois if doi in doi_to_key),
                fingerprint)
            candidate = candidates.setdefault(
                key,
                {
                    "fingerprint": fingerprint,
                    "title": title,
                    "year": related_work.get("year"),
                    "product_type": related_work.get("product_type", ""),
                    "type_impactu": related_work.get("type_impactu", ""),
                    "advisor_role": related_work.get("advisor_role", ""),
                    "keywords": [],
                    "areas": [],
                    "oriented_people": [],
                    "dois": [],
                    "isbn": [],
                    "issn": [],
                    "identifiers": [],
                    "authors": [],
                },
            )
            _append_unique(
                candidate["keywords"],
                related_work.get("keywords") or [])
            _append_unique(candidate["areas"], related_work.get("areas") or [])
            _append_unique(
                candidate["oriented_people"],
                related_work.get("oriented_people") or [],
            )
            _append_unique(candidate["dois"], dois)
            _append_unique(candidate["isbn"], related_work.get("isbn") or [])
            _append_unique(candidate["issn"], related_work.get("issn") or [])
            if related_work.get("source") == "scienti" and isinstance(
                related_work.get("id"), dict
            ):
                _append_unique(
                    candidate["identifiers"],
                    [{"source": "scienti", "id": related_work["id"]}],
                    key=lambda value: repr(value),
                )
            author = _author_from_person(
                person, related_work.get(
                    "advisor_role", ""))
            _append_unique(
                candidate["authors"], [author], key=lambda value: str(
                    value.get("id")) or normalize_title(
                    value.get("full_name")), )
            for doi in dois:
                doi_to_key[doi] = key
    return list(candidates.values())


def candidate_to_work(candidate, empty_work):
    entry = deepcopy(empty_work)
    entry["updated"] = [{"source": "minciencias", "time": int(time())}]
    entry["titles"] = [
        {
            "title": candidate["title"],
            "lang": lang_poll(candidate["title"]),
            "source": "minciencias",
        }
    ]
    entry["year_published"] = candidate.get("year")
    entry["authors"] = deepcopy(candidate["authors"])
    entry["author_count"] = len(entry["authors"])
    entry["keywords"] = deepcopy(candidate["keywords"])
    for type_name, level in (
        (candidate.get("type_impactu"), 0),
        (candidate.get("product_type"), 1),
    ):
        if type_name and not any(
                item.get("type") == type_name for item in entry["types"]):
            entry["types"].append(
                {
                    "provenance": "minciencias",
                    "source": "minciencias",
                    "type": type_name,
                    "level": level,
                }
            )
    if candidate["areas"]:
        entry["subjects"] = [
            {
                "source": "minciencias",
                "subjects": [
                    {"id": "", "name": area, "level": None}
                    for area in candidate["areas"]
                ],
            }
        ]
    for doi in candidate["dois"]:
        entry["external_ids"].append(
            {"provenance": "minciencias", "source": "doi", "id": doi}
        )
    if candidate["dois"]:
        entry["doi"] = candidate["dois"][0]
    for identifier in candidate["identifiers"]:
        entry["external_ids"].append(
            {
                "provenance": "minciencias",
                "source": identifier["source"],
                "id": deepcopy(identifier["id"]),
            }
        )
    for source in ("isbn", "issn"):
        for identifier in candidate[source]:
            entry["external_ids"].append(
                {
                    "provenance": "minciencias",
                    "source": source,
                    "id": identifier,
                }
            )
    entry["external_ids"].append(
        {
            "provenance": "minciencias",
            "source": "minciencias_title_fingerprint",
            "id": candidate["fingerprint"],
        }
    )
    entry["bibliographic_info"]["minciencias"] = {
        "advisor_role": candidate.get("advisor_role", ""),
        "oriented_people": deepcopy(candidate["oriented_people"]),
    }
    return entry


def _merge_author(existing, incoming):
    existing.setdefault("affiliations", [])
    _append_unique(
        existing["affiliations"],
        incoming.get("affiliations", []),
        key=lambda value: str(value.get("id")),
    )
    if incoming.get("type") and not existing.get("type"):
        existing["type"] = incoming["type"]


def merge_work(existing, incoming):
    """Merge source metadata without replacing richer values from other sources."""
    _append_unique(
        existing.setdefault(
            "titles", []), incoming.get(
            "titles", []), key=lambda value: (
                value.get("source"), normalize_title(
                    value.get("title"))), )
    _append_unique(
        existing.setdefault("external_ids", []),
        incoming.get("external_ids", []),
        key=lambda value: (value.get("source"), repr(value.get("id"))),
    )
    _append_unique(
        existing.setdefault(
            "types", []), incoming.get(
            "types", []), key=lambda value: (
                value.get("source"), value.get("type"), value.get("level")), )
    _append_unique(
        existing.setdefault(
            "keywords", []), incoming.get(
            "keywords", []))
    if not existing.get("doi") and incoming.get("doi"):
        existing["doi"] = incoming["doi"]
    if not existing.get("year_published") and incoming.get("year_published"):
        existing["year_published"] = incoming["year_published"]
    incoming_subjects = incoming.get("subjects", [])
    _append_unique(
        existing.setdefault(
            "subjects",
            []),
        incoming_subjects,
        key=repr)
    existing_biblio = existing.setdefault("bibliographic_info", {})
    for key, value in incoming.get("bibliographic_info", {}).items():
        if value and not existing_biblio.get(key):
            existing_biblio[key] = deepcopy(value)
    existing_authors = existing.setdefault("authors", [])
    for author in incoming.get("authors", []):
        match = next(
            (
                item
                for item in existing_authors
                if item.get("id") == author.get("id")
                or (
                    normalize_title(item.get("full_name"))
                    and normalize_title(item.get("full_name"))
                    == normalize_title(author.get("full_name"))
                )
            ),
            None,
        )
        if match:
            _merge_author(match, author)
        else:
            existing_authors.append(deepcopy(author))
    existing["author_count"] = len(existing_authors)
    updates = [item for item in existing.setdefault(
        "updated", []) if item.get("source") != "minciencias"]
    updates.extend(incoming.get("updated", []))
    existing["updated"] = updates
    return existing


def _find_exact(collection, candidate):
    if candidate["dois"]:
        found = collection.find_one(
            {
                "$or": [
                    {"doi": {"$in": candidate["dois"]}},
                    {
                        "external_ids": {
                            "$elemMatch": {
                                "source": "doi",
                                "id": {"$in": candidate["dois"]},
                            }
                        }
                    },
                ]
            }
        )
        if found:
            return found, "doi"
    for identifier in candidate["identifiers"]:
        found = collection.find_one(
            {
                "external_ids": {
                    "$elemMatch": {
                        "source": identifier["source"],
                        "id": identifier["id"],
                    }
                }
            }
        )
        if found:
            return found, "scienti"
    found = collection.find_one(
        {
            "external_ids": {
                "$elemMatch": {
                    "source": "minciencias_title_fingerprint",
                    "id": candidate["fingerprint"],
                }
            }
        }
    )
    return (found, "fingerprint") if found else (None, None)


def _similar_work(candidate, collection, es_handler, thresholds,
                  es_semaphore=None):
    if not es_handler:
        return None
    responses = run_es_operation(es_semaphore,
                                 es_handler.search_work,
                                 title=candidate["title"],
                                 source="",
                                 year=candidate.get("year") or "0",
                                 authors=[author.get("full_name",
                                                     "") for author in candidate["authors"][:5]],
                                 volume="",
                                 issue="",
                                 page_start="",
                                 page_end="",
                                 use_es_thold=True,
                                 es_thold=0,
                                 hits=20,
                                 ) or []
    author_threshold, low_threshold, high_threshold = thresholds
    candidate_authors = [
        normalize_title(
            author.get("full_name")) for author in candidate["authors"]]
    for response in responses:
        source = response.get("_source", {})
        title_score = fuzz.ratio(
            normalize_title(
                candidate["title"]), normalize_title(
                source.get("title")))
        author_match = any(
            fuzz.partial_ratio(author, normalize_title(indexed_author)) >= author_threshold
            for author in candidate_authors
            for indexed_author in source.get("authors", [])
            if author and indexed_author
        )
        if title_score >= (low_threshold if author_match else high_threshold):
            found = collection.find_one({"_id": response.get("_id")})
            if found:
                return found
            try:
                from bson import ObjectId

                found = collection.find_one(
                    {"_id": ObjectId(response.get("_id"))})
            except Exception:
                found = None
            if found:
                return found
    return None


def _index_work(es_handler, work_id, entry, es_semaphore=None):
    if not es_handler or not entry.get("titles"):
        return
    bibliography = entry.get("bibliographic_info", {})
    es_client = getattr(es_handler, "es", None)
    es_index = getattr(es_handler, "es_index", None)
    if es_client is not None and es_index and run_es_operation(
        es_semaphore, es_client.exists, index=es_index, id=str(work_id)
    ):
        return
    run_es_operation(
        es_semaphore, es_handler.insert_work, _id=str(work_id), work={
            "title": entry["titles"][0].get(
                "title", ""), "source": entry.get(
                "source", {}).get(
                    "name", ""), "year": entry.get("year_published") or "0", "volume": bibliography.get(
                        "volume", ""), "issue": bibliography.get(
                            "issue", ""), "first_page": bibliography.get(
                                "start_page", ""), "last_page": bibliography.get(
                                    "end_page", ""), "authors": [
                                        author.get(
                                            "full_name", "") for author in entry.get(
                                                "authors", [])[
                                                    :5]], "provenance": "elasticsearch", }, )


def process_person_related_works(
    person_collection,
    works_collection,
    empty_work_factory,
    es_handler=None,
    insert_all=False,
    thresholds=None,
    es_semaphore=None,
):
    thresholds = thresholds if thresholds and len(
        thresholds) == 3 else [65, 90, 95]
    counters = {
        key: 0 for key in (
            "candidates",
            "doi",
            "scienti",
            "fingerprint",
            "similarity",
            "inserted",
            "skipped")}
    candidates = collect_related_works(person_collection)
    counters["candidates"] = len(candidates)
    for candidate in candidates:
        incoming = candidate_to_work(candidate, empty_work_factory())
        existing, match_type = _find_exact(works_collection, candidate)
        if not existing:
            existing = _similar_work(
                candidate,
                works_collection,
                es_handler,
                thresholds,
                es_semaphore)
            match_type = "similarity" if existing else None
        if existing:
            merged = merge_work(existing, incoming)
            works_collection.replace_one({"_id": existing["_id"]}, merged)
            counters[match_type] += 1
            continue
        if not insert_all:
            counters["skipped"] += 1
            continue
        response = works_collection.insert_one(incoming)
        _index_work(es_handler, response.inserted_id, incoming, es_semaphore)
        counters["inserted"] += 1
    return counters
