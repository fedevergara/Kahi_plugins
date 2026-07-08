import copy
import re
from time import time

from kahi_impactu_utils.Utils import doi_processor, split_names
from unidecode import unidecode


DASH_RE = re.compile(r"[\u2010-\u2015\u2212]")
SPACE_RE = re.compile(r"\s+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
SENTINEL_NAMES = {"other", "others", "et al", "etal"}


def clean_text(value):
    value = DASH_RE.sub("-", str(value or ""))
    return SPACE_RE.sub(" ", value).strip()


def normalize_name(value):
    value = unidecode(clean_text(value)).lower()
    return NON_ALNUM_RE.sub(" ", value).strip()


def is_sentinel_name(value):
    return normalize_name(value) in SENTINEL_NAMES


def display_tokens(value):
    tokens = clean_text(value).replace("-", " ").split()
    output = []
    for token in tokens:
        if token.isupper() and len(token) <= 3:
            output.append(token)
        elif token.isupper() or token.islower():
            output.append(token.capitalize())
        else:
            output.append(token)
    return output


def parse_author_candidate(raw_author):
    raw_author = clean_text(raw_author)
    if not raw_author or is_sentinel_name(raw_author):
        return None

    parts = [clean_text(part) for part in raw_author.split(",")]
    if len(parts) > 1:
        last_names = display_tokens(parts[0])
        first_names = display_tokens(" ".join(parts[1:]))
        full_name = " ".join(first_names + last_names).strip()
    else:
        full_name = " ".join(display_tokens(raw_author))
        try:
            parsed = split_names(full_name)
        except Exception:
            parsed = {"first_names": [], "last_names": [], "initials": []}
        first_names = parsed.get("first_names", [])
        last_names = parsed.get("last_names", [])

    if not full_name or is_sentinel_name(full_name):
        return None
    return {
        "raw": raw_author,
        "full_name": full_name,
        "first_names": first_names,
        "last_names": last_names,
    }


def compact_initials(tokens):
    initials = []
    for token in tokens:
        token = normalize_name(token).replace(" ", "")
        if not token:
            continue
        initials.append(token if len(token) <= 3 else token[0])
    return "".join(initials)


def profile_match_score(profile_name, candidate):
    profile_tokens = normalize_name(profile_name).split()
    first_tokens = normalize_name(" ".join(candidate["first_names"])).split()
    last_tokens = normalize_name(" ".join(candidate["last_names"])).split()
    if not profile_tokens or not first_tokens or not last_tokens:
        return None

    surname_overlap = set(profile_tokens) & set(last_tokens)
    if not surname_overlap:
        return None

    profile_given = [token for token in profile_tokens if token not in last_tokens]
    profile_initials = compact_initials(profile_given)
    candidate_initials = compact_initials(first_tokens)
    if not profile_initials or not candidate_initials:
        return None
    if profile_initials[0] != candidate_initials[0]:
        return None

    score = 10 * len(surname_overlap)
    if profile_tokens[-1] in last_tokens:
        score += 4
    if profile_initials == candidate_initials:
        score += 6
    elif candidate_initials.startswith(profile_initials) or profile_initials.startswith(candidate_initials):
        score += 3
    score += len(set(profile_tokens) & set(first_tokens))
    return score


def fallback_profile_candidate(profile_name):
    full_name = " ".join(display_tokens(profile_name))
    try:
        parsed = split_names(full_name)
    except Exception:
        parsed = {"first_names": [], "last_names": [], "initials": []}
    return {
        "raw": "",
        "full_name": full_name,
        "first_names": parsed.get("first_names", []),
        "last_names": parsed.get("last_names", []),
    }


def process_authors(s_work, verbose=0):
    raw_authors = re.split(r"\s+and\s+", str(s_work.get("author") or ""), flags=re.IGNORECASE)
    candidates = [candidate for candidate in (parse_author_candidate(raw) for raw in raw_authors) if candidate]
    profiles = [
        (clean_text(name), clean_text(profile_id))
        for name, profile_id in (s_work.get("profiles") or {}).items()
        if clean_text(profile_id) and not is_sentinel_name(name)
    ]

    proposals = []
    for profile_index, (profile_name, _) in enumerate(profiles):
        scores = []
        for candidate_index, candidate in enumerate(candidates):
            score = profile_match_score(profile_name, candidate)
            if score is not None:
                scores.append((score, candidate_index))
        scores.sort(reverse=True)
        if scores and scores[0][0] >= 14 and (len(scores) == 1 or scores[0][0] > scores[1][0]):
            proposals.append((scores[0][0], profile_index, scores[0][1]))

    assignments = {}
    used_candidates = set()
    for _, profile_index, candidate_index in sorted(proposals, reverse=True):
        if profile_index not in assignments and candidate_index not in used_candidates:
            assignments[profile_index] = candidate_index
            used_candidates.add(candidate_index)

    output = []
    for profile_index, (profile_name, profile_id) in enumerate(profiles):
        candidate = candidates[assignments[profile_index]] if profile_index in assignments else fallback_profile_candidate(profile_name)
        if not candidate["full_name"] or is_sentinel_name(candidate["full_name"]):
            continue
        output.append({
            "full_name": candidate["full_name"],
            "first_names": candidate["first_names"],
            "last_names": candidate["last_names"],
            "author": candidate["raw"],
            "alias": profile_name,
            "scholar_id": profile_id,
        })
    return output


def scholar_url(profile_id):
    if profile_id.startswith("http://") or profile_id.startswith("https://"):
        return profile_id
    return "https://scholar.google.com/citations?user=" + profile_id


def parse_scholar(reg, empty_person, verbose=0):
    entries = []
    for author in process_authors(reg, verbose=verbose):
        entry = copy.deepcopy(empty_person)
        entry["updated"].append({"source": "scholar", "time": int(time())})
        entry["full_name"] = author["full_name"]
        entry["first_names"] = author["first_names"]
        entry["last_names"] = author["last_names"]
        entry["initials"] = "".join(name[0] for name in entry["first_names"] if name)

        aliases = [author["author"], author["alias"]]
        entry["aliases"] = list(dict.fromkeys(alias for alias in aliases if alias and alias != entry["full_name"]))
        entry["external_ids"].append({
            "provenance": "scholar",
            "source": "scholar",
            "id": scholar_url(author["scholar_id"]),
        })

        cid = clean_text(reg.get("cid"))
        if cid:
            entry["related_works"].append({"provenance": "scholar", "source": "cid", "id": cid})
        doi = doi_processor(clean_text(reg.get("doi"))) if reg.get("doi") else None
        if doi:
            entry["related_works"].append({"provenance": "scholar", "source": "doi", "id": doi})
        entries.append(entry)
    return entries
