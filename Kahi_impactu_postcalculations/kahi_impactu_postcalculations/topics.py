import requests
from pymongo import UpdateOne
from time import sleep


type_url_base = "https://openalex.org/T"


def _topic_payload(work):
    """Build the inference payload for a work, or return None if it is invalid."""
    titles = work.get("titles") or []
    abstracts = work.get("abstracts") or []
    if not titles or not abstracts:
        return None
    title = titles[0].get("title")
    abstract = abstracts[0].get("abstract")
    if not isinstance(title, str) or not title.strip() or not abstract:
        return None
    source = work.get("source") or {}
    return {
        "title": title,
        "abstract_inverted_index": abstract,
        "inverted": True,
        "referenced_works": [],
        "journal_display_name": source.get("name", ""),
    }


def request_topic_inference_batch(
    payload,
    inference_endpoint="http://localhost:8080/invocations",
    timeout=300,
):
    """Request topic inference for an already validated batch."""
    return requests.post(inference_endpoint, json=payload, timeout=timeout)


def request_topic_inference(
    title,
    abstract=None,
    journal_name="",
    inference_endpoint="http://localhost:8080/invocations",
    timeout=300,
):
    """Backward-compatible single-work inference request."""
    payload = [{
        "title": title,
        "abstract_inverted_index": abstract or {},
        "inverted": True,
        "referenced_works": [],
        "journal_display_name": journal_name,
    }]
    return request_topic_inference_batch(payload, inference_endpoint, timeout)


def load_openalex_topics(col_oa):
    """Load OpenAlex topic metadata once instead of querying it per prediction."""
    projection = {
        "_id": 0,
        "id": 1,
        "display_name": 1,
        "subfield": 1,
        "field": 1,
        "domain": 1,
    }
    return {
        topic["id"]: topic
        for topic in col_oa.find({}, projection)
        if topic.get("id")
    }


def get_openalex_topic(topic_cache, topic_pred):
    """Resolve predicted topic metadata from the in-memory OpenAlex cache."""
    topic_url = type_url_base + str(topic_pred["topic_id"])
    cached = topic_cache.get(topic_url)
    if cached is None:
        topic = {
            "id": topic_url,
            "display_name": "Unknown",
            "subfield": "Unknown",
            "field": "Unknown",
            "domain": "Unknown",
        }
        print(
            "WARNING: Topic not found predicted in inference",
            topic_url,
            topic_pred)
    else:
        # Each prediction has its own score; never mutate the shared cache.
        topic = dict(cached)
    topic["score"] = topic_pred["topic_score"]
    return topic


def _topic_update(work, predictions, topic_cache):
    if not isinstance(predictions, list):
        return None

    current_topics = list(work.get("topics") or [])
    current_ids = {topic.get("id") for topic in current_topics}
    primary_topic = work.get("primary_topic") or {}
    for topic_pred in predictions:
        if (
            not isinstance(topic_pred, dict)
            or topic_pred.get("topic_id") in (None, -1)
            or "topic_score" not in topic_pred
        ):
            continue
        topic = get_openalex_topic(topic_cache, topic_pred)
        if not primary_topic:
            primary_topic = topic
        if topic.get("id") not in current_ids:
            current_topics.append(topic)
            current_ids.add(topic.get("id"))

    if not primary_topic:
        return None
    return UpdateOne(
        {"_id": work["_id"], "primary_topic": {}},
        {"$set": {"primary_topic": primary_topic, "topics": current_topics}},
    )


def process_topic_batch(
    col,
    topic_cache,
    works,
    inference_endpoint="http://localhost:8080/invocations",
    timeout=300,
    retries=3,
):
    """Infer and persist topics for a batch of works using one HTTP request/write."""
    valid = []
    payload = []
    for work in works:
        item = _topic_payload(work)
        if item is not None:
            valid.append(work)
            payload.append(item)
    if not payload:
        return 0

    response = None
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = request_topic_inference_batch(
                payload, inference_endpoint, timeout
            )
            if response.status_code == 200:
                break
            last_error = RuntimeError(
                f"topic inference returned HTTP {response.status_code}: "
                f"{response.text[:300]}"
            )
        except requests.RequestException as error:
            last_error = error
        if attempt < retries:
            print(
                f"WARNING: Topic batch inference attempt {attempt}/{retries} "
                "failed; retrying"
            )
            sleep(attempt)
    if response is None or response.status_code != 200:
        raise RuntimeError(
            f"Topic batch inference failed after {retries} attempts"
        ) from last_error
    try:
        predictions = response.json()
    except ValueError as error:
        print(f"ERROR: Invalid topic batch inference response: {error}")
        return 0
    if not isinstance(predictions, list) or len(predictions) != len(valid):
        print(
            "ERROR: Topic batch inference response size mismatch: "
            f"expected {len(valid)}, got "
            f"{len(predictions) if isinstance(predictions, list) else 'invalid'}"
        )
        return 0

    operations = []
    for work, work_predictions in zip(valid, predictions):
        operation = _topic_update(work, work_predictions, topic_cache)
        if operation is not None:
            operations.append(operation)
    if not operations:
        return 0
    result = col.bulk_write(operations, ordered=False)
    return result.modified_count


def process_topic(
    col,
    col_oa,
    work,
    inference_endpoint="http://localhost:8080/invocations",
    timeout=300,
):
    """Backward-compatible wrapper for processing a single work."""
    topic_cache = load_openalex_topics(col_oa)
    return bool(
        process_topic_batch(
            col, topic_cache, [work], inference_endpoint, timeout
        )
    )
