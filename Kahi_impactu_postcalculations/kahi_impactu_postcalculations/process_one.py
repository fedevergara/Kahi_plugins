from math import log, exp
from collections import Counter, defaultdict
from pymongo import MongoClient, UpdateOne
from spacy import load

# for multiprocessing have to be loaded global
en_model = None
es_model = None


def split_network_edges(edges):
    """Split edges without dropping either half."""
    split_at = (len(edges) + 1) // 2
    return edges[:split_at], edges[split_at:]


def load_nlp_models():
    global en_model
    global es_model
    if not en_model:
        en_model = load('en_core_web_sm')
    if not es_model:
        es_model = load('es_core_news_sm')


def get_title_word_counts(title, lang, es_model, en_model, stopwords):
    """Return the same top-word token counts used by the legacy routines."""
    if not title:
        return Counter()
    model = es_model if lang == "es" else en_model
    results = Counter()
    for token in model(title.lower()):
        if token.lemma_.isnumeric():
            continue
        if token.lemma_ in stopwords:
            continue
        if len(token.lemma_) < 4:
            continue
        results[token.lemma_] += 1
    return results


def format_top_words(counter, limit=20):
    return [
        {"name": name, "value": value}
        for name, value in counter.most_common(limit)
    ]


def _flush_top_word_counts(collection, counts):
    operations = []
    for entity_id, word_counts in counts.items():
        for word, value in word_counts.items():
            operations.append(
                UpdateOne(
                    {"_id": str(entity_id) + "::" + str(word)},
                    {
                        "$setOnInsert": {"entity_id": entity_id, "word": word},
                        "$inc": {"value": value},
                    },
                    upsert=True,
                )
            )
    if operations:
        try:
            collection.bulk_write(operations, ordered=False)
        except TypeError:
            # mongomock does not support every PyMongo UpdateOne option.
            for operation in operations:
                collection.update_one(
                    operation._filter,
                    operation._doc,
                    upsert=operation._upsert,
                )


def _write_top_words_results(
        temp_collection,
        output_collection,
        batch_size=1000):
    operations = []
    current_id = None
    current_counter = Counter()
    sort_spec = [("entity_id", 1), ("value", -1), ("word", 1)]
    for row in temp_collection.find(
            {}, {"entity_id": 1, "word": 1, "value": 1}).sort(sort_spec):
        if current_id is not None and row["entity_id"] != current_id:
            operations.append(
                UpdateOne(
                    {"_id": current_id},
                    {"$set": {"top_words": format_top_words(current_counter)}},
                    upsert=True,
                )
            )
            if len(operations) >= batch_size:
                try:
                    output_collection.bulk_write(operations, ordered=False)
                except TypeError:
                    for operation in operations:
                        output_collection.update_one(
                            operation._filter,
                            operation._doc,
                            upsert=operation._upsert,
                        )
                operations = []
            current_counter = Counter()
        current_id = row["entity_id"]
        current_counter[row["word"]] = row["value"]
    if current_id is not None:
        operations.append(
            UpdateOne(
                {"_id": current_id},
                {"$set": {"top_words": format_top_words(current_counter)}},
                upsert=True,
            )
        )
    if operations:
        try:
            output_collection.bulk_write(operations, ordered=False)
        except TypeError:
            for operation in operations:
                output_collection.update_one(
                    operation._filter,
                    operation._doc,
                    upsert=operation._upsert,
                )


def top_words_from_works_scan(
    db_in,
    db_out,
    es_model,
    en_model,
    stopwords,
    batch_size=5000,
    force_recalculate=False,
):
    """
    Build person and affiliation top_words by scanning works once.

    The output schema is unchanged:
    - db_out.person: {"_id": person_id, "top_words": [{"name": ..., "value": ...}]}
    - db_out.affiliations: {"_id": affiliation_id, "top_words": [{"name": ..., "value": ...}]}
    """
    person_tmp = db_out["top_words_person_tmp"]
    affiliation_tmp = db_out["top_words_affiliations_tmp"]
    person_tmp.drop()
    affiliation_tmp.drop()
    person_tmp.create_index([("entity_id", 1), ("value", -1), ("word", 1)])
    affiliation_tmp.create_index(
        [("entity_id", 1), ("value", -1), ("word", 1)])

    processed = 0
    person_counts = defaultdict(Counter)
    affiliation_counts = defaultdict(Counter)
    projection = {"titles": 1, "authors.id": 1, "authors.affiliations.id": 1}
    query = {"titles.title": {"$exists": True}, "authors.0": {"$exists": True}}
    for work in db_in["works"].find(query, projection, no_cursor_timeout=True):
        titles = work.get("titles") or []
        if not titles or not titles[0].get("title"):
            continue
        word_counts = get_title_word_counts(
            titles[0]["title"],
            titles[0].get("lang"),
            es_model,
            en_model,
            stopwords,
        )
        if not word_counts:
            continue
        person_ids = set()
        affiliation_ids = set()
        for author in work.get("authors") or []:
            author_id = author.get("id")
            if author_id:
                person_ids.add(author_id)
            for affiliation in author.get("affiliations") or []:
                affiliation_id = affiliation.get("id")
                if affiliation_id:
                    affiliation_ids.add(affiliation_id)
        for person_id in person_ids:
            person_counts[person_id].update(word_counts)
        for affiliation_id in affiliation_ids:
            affiliation_counts[affiliation_id].update(word_counts)
        processed += 1
        if processed % batch_size == 0:
            _flush_top_word_counts(person_tmp, person_counts)
            _flush_top_word_counts(affiliation_tmp, affiliation_counts)
            person_counts.clear()
            affiliation_counts.clear()
            print(f"INFO: top words works-scan processed {processed} works")

    _flush_top_word_counts(person_tmp, person_counts)
    _flush_top_word_counts(affiliation_tmp, affiliation_counts)

    _write_top_words_results(person_tmp, db_out["person"])
    _write_top_words_results(affiliation_tmp, db_out["affiliations"])
    person_tmp.drop()
    affiliation_tmp.drop()


def count_works_one(db, author_id):
    """
    Count the number of works for an author.

    Parameters:
    ----------
    author : dict
        Author information.

    Returns:
        str: The author identifier.
    """
    count = db["works"].count_documents({"authors.id": author_id})
    if count != 0:
        return author_id


def network_creation_process_one(
    config,
    client,
    impactu_client,
    idx,
    author_count,
    net,
    backend,
    force_recalculate=False,
):
    """
    Function to create the network of coauthorships for an affiliation or author.

    Parameters:
    ----------
    config : dict
        The configuration dictionary.
    client : pymongo.database.Database
        The database where the information is stored.
    impactu_client : pymongo.database.Database
        The database where the information will be stored.
    idx : str
        The affiliation or author identifier.
    author_count : int
        The maximum number of authors in a work to consider.
    net : str
        The network type, either affiliations or authors.
    backend : str
        The backend to use for the parallel processing. "mutiprocessing" or "threading".
    """
    if backend != "threading":
        client = MongoClient(config["database_url"])
        db_in = client[config["database_name"]]
        impactu_client = MongoClient(
            config["impactu_postcalculations"]["database_url"])
        db_out = impactu_client[config["impactu_postcalculations"]
                                ["database_name"]]
    else:
        db_in = client[config["database_name"]]
        db_out = impactu_client[
            config["impactu_postcalculations"]["database_name"]
        ]
    if net not in ["affiliations", "person"]:
        print("ERROR: Invalid network type options are affiliations or authors")
        return

    if net == "affiliations":
        network_creation_affiliations(
            db_in, db_out, idx, author_count, force_recalculate
        )
    if net == "person":
        network_creation_person(
            db_in, db_out, idx, author_count, force_recalculate
        )

    if backend != "threading":
        client.close()
        impactu_client.close()


def network_creation_affiliations(
    db_in, db_out, idx, author_count, force_recalculate=False
):
    """
    Function to create the network of coauthorships for an affiliation.

    Parameters:
    ----------
    db_in : pymongo.database.Database (kahi dabatabase)
        The database where the information is stored.
    db_out : pymongo.database.Database (calculation database)
        The database where the information will be stored.
    idx : str
        The affiliation identifier.
    author_count : int
        The maximum number of authors in a work to consider.
    """
    already = db_out["affiliations"].find_one(
        {"_id": idx, "coauthorship_network": {"$exists": True}})
    if already and not force_recalculate:
        return None
    aff_info = db_in["affiliations"].find_one({"_id": idx})
    name = aff_info["names"][0]["name"]
    for n in aff_info["names"]:
        if n["lang"] == "es":
            name = n["name"]
            break
        elif n["lang"] == "en":
            name = n["name"]
    nodes = [idx]
    nodes_labels = [name]
    edges = []
    edges_coauthorships = {}
    works_count = 0
    for work in db_in["works"].find(
            {"authors.affiliations.id": idx, "author_count": {"$lte": author_count}}):
        works_count += 1
        work_nodes = [idx]
        work_edges = []
        for author in work["authors"]:
            for aff in author["affiliations"]:
                if not aff["id"]:
                    continue
                if aff["id"] == "":
                    continue
                if aff["id"] == idx:
                    continue
                if not aff["id"] in nodes:
                    nodes.append(aff["id"])
                    name = aff["name"]

                    nodes_labels.append(name)
                if not aff["id"] in work_nodes:
                    for node in work_nodes:
                        edge_found = False
                        if (idx, aff["id"]) in work_edges:
                            edge_found = True
                        elif (aff["id"], idx) in edges:
                            edge_found = True
                        if edge_found is False:
                            work_edges.append((idx, aff["id"]))
                    work_nodes.append(aff["id"])
        # Connecting all the nodes in the work among them
        # checking if the connection already exists to add one to the count of
        # coauthorships
        for node in work_nodes:
            if node not in nodes:
                nodes.append(node)
        for nodea, nodeb in work_edges:
            edge_found = False
            if (nodea, nodeb) in edges:
                edges_coauthorships[str(nodea) + str(nodeb)] += 1
                edge_found = True
            elif (nodeb, nodea) in edges:
                edges_coauthorships[str(nodeb) + str(nodea)] += 1
                edge_found = True
            if edge_found is False:
                edges_coauthorships[str(nodea) + str(nodeb)] = 1
                edges.append((nodea, nodeb))
    # adding the connections between the coauthoring institutions
    for node in nodes:
        if node == idx:
            continue
        for work in db_in["works"].find({"$and": [{"authors.affiliations.id": node}, {
                                        "authors.affiliations.id": {"$ne": idx}}], "author_count": {"$lte": author_count}}):
            for author in work["authors"]:
                for aff in author["affiliations"]:
                    if aff["id"] == idx:
                        print("Problem found")
                        continue
                    if not aff["id"] in nodes:
                        continue
                    if node == aff["id"]:
                        continue
                    if (node, aff["id"]) in edges:
                        edges_coauthorships[str(node) + str(aff["id"])] += 1
                    elif (aff["id"], node) in edges:
                        edges_coauthorships[str(aff["id"]) + str(node)] += 1
                    else:
                        edges_coauthorships[str(node) + str(aff["id"])] = 1
                        edges.append((node, aff["id"]))
    # Constructing the actual format to insrt in db
    num_nodes = len(nodes)
    nodes_db = []
    for i, node in enumerate(nodes):
        degree = len([1 for i, j in edges if i == node or j == node])
        size = 50 * log(1 + degree / (num_nodes - 1),
                        2) if num_nodes > 1 else 1
        nodes_db.append(
            {
                "id": str(node),
                "label": nodes_labels[i],
                "degree": degree,
                "size": size
            }
        )
    edges_db = []
    for nodea, nodeb in edges:
        coauthorships = 0
        if str(nodea) + str(nodeb) in edges_coauthorships.keys():
            coauthorships = edges_coauthorships[str(nodea) + str(nodeb)]
        elif str(nodeb) + str(nodea) in edges_coauthorships.keys():
            coauthorships = edges_coauthorships[str(nodeb) + str(nodea)]
        edges_db.append({
            "source": str(nodea),
            "target": str(nodeb),
            "coauthorships": coauthorships,
            "size": coauthorships,
        })
    top = max([e["coauthorships"]
              for e in edges_db]) if len(edges_db) > 0 else 1
    bot = min([e["coauthorships"]
              for e in edges_db]) if len(edges_db) > 0 else 1
    for edge in edges_db:
        if abs(top - edge["coauthorships"]) < 0.01:
            edge["size"] = 10
        elif abs(bot - edge["coauthorships"]) < 0.01:
            edge["size"] = 1
        else:
            size = 10 / (1 + exp(6 - 10 * edge["coauthorships"] / top))
            edge["size"] = size if size >= 1 else 1
    primary_edges, overflow_edges = split_network_edges(edges_db)
    record = {
        "coauthorship_network": {
            "nodes": nodes_db,
            "edges": primary_edges,
        }
    }
    record_edges = {
        "coauthorship_network": {
            "edges": overflow_edges,
        }
    }
    try:
        db_out["affiliations"].update_one(
            {"_id": idx, }, {"$set": record}, upsert=True)
        db_out["affiliations_edges"].update_one(
            {"_id": idx, }, {"$set": record_edges}, upsert=True)
    except Exception as e:
        print(f"ERROR: Could not store affiliation network for id {idx}", e)


def network_creation_person(
    db_in, db_out, idx, author_count, force_recalculate=False
):
    """
    Function to create the network of coauthorships for an author.

    Parameters:
    ----------
    db_in : pymongo.database.Database (kahi dabatabase)
        The database where the information is stored.
    db_out : pymongo.database.Database (calculation database)
        The database where the information will be stored.
    idx : str
        The author identifier.
    """

    already = db_out["person"].find_one(
        {"_id": idx, "coauthorship_network": {"$exists": True}})
    if already and not force_recalculate:
        return None
    aff_info = db_in["person"].find_one({"_id": idx})
    name = aff_info["full_name"]
    nodes = [idx]
    nodes_labels = [name]
    edges = []
    edges_coauthorships = {}
    works_count = 0
    for work in db_in["works"].find(
            {"authors.id": idx, "author_count": {"$lte": author_count}}):
        works_count += 1
        work_nodes = [idx]
        work_edges = []
        for author in work["authors"]:
            if not author["id"]:
                continue
            if author["id"] == "":
                continue
            if author["id"] == idx:
                continue
            if not author["id"] in nodes:
                nodes.append(author["id"])
                name = author["full_name"]
                nodes_labels.append(name)
            if not author["id"] in work_nodes:
                for node in work_nodes:
                    edge_found = False
                    if (idx, author["id"]) in work_edges:
                        edge_found = True
                    elif (author["id"], idx) in edges:
                        edge_found = True
                    if edge_found is False:
                        work_edges.append((idx, author["id"]))
                work_nodes.append(author["id"])
        # Connecting all the nodes in the work among them
        # checking if the connection already exists to add one to the count of
        # coauthorships
        for node in work_nodes:
            if node not in nodes:
                nodes.append(node)
        for nodea, nodeb in work_edges:
            edge_found = False
            if (nodea, nodeb) in edges:
                edges_coauthorships[str(nodea) + str(nodeb)] += 1
                edge_found = True
            elif (nodeb, nodea) in edges:
                edges_coauthorships[str(nodeb) + str(nodea)] += 1
                edge_found = True
            if edge_found is False:
                edges_coauthorships[str(nodea) + str(nodeb)] = 1
                edges.append((nodea, nodeb))
    # adding the connections between the coauthoring institutions
    for node in nodes:
        if node == idx:
            continue
        for work in db_in["works"].find({"$and": [{"authors.id": node}, {"authors.id": {
                                        "$ne": idx}}], "author_count": {"$lte": author_count}}):
            for author in work["authors"]:
                if author["id"] == idx:
                    print("Problem found")
                    continue
                if not author["id"] in nodes:
                    continue
                if node == author["id"]:
                    continue
                if (node, author["id"]) in edges:
                    edges_coauthorships[str(node) + str(author["id"])] += 1
                elif (author["id"], node) in edges:
                    edges_coauthorships[str(author["id"]) + str(node)] += 1
                else:
                    edges_coauthorships[str(node) + str(author["id"])] = 1
                    edges.append((node, author["id"]))
    # Constructing the actual format to insrt in db
    num_nodes = len(nodes)
    nodes_db = []
    for i, node in enumerate(nodes):
        degree = len([1 for i, j in edges if i == node or j == node])
        size = 50 * log(1 + degree / (num_nodes - 1),
                        2) if num_nodes > 1 else 1
        nodes_db.append(
            {
                "id": str(node),
                "label": nodes_labels[i],
                "degree": degree,
                "size": size
            }
        )
    edges_db = []
    for nodea, nodeb in edges:
        coauthorships = 0
        if str(nodea) + str(nodeb) in edges_coauthorships.keys():
            coauthorships = edges_coauthorships[str(nodea) + str(nodeb)]
        elif str(nodeb) + str(nodea) in edges_coauthorships.keys():
            coauthorships = edges_coauthorships[str(nodeb) + str(nodea)]
        edges_db.append({
            "source": str(nodea),
            "target": str(nodeb),
            "coauthorships": coauthorships,
            "size": coauthorships,
        })
    top = max([e["coauthorships"]
              for e in edges_db]) if len(edges_db) > 0 else 1
    bot = min([e["coauthorships"]
              for e in edges_db]) if len(edges_db) > 0 else 1
    for edge in edges_db:
        if abs(top - edge["coauthorships"]) < 0.01:
            edge["size"] = 10
        elif abs(bot - edge["coauthorships"]) < 0.01:
            edge["size"] = 1
        else:
            size = 10 / (1 + exp(6 - 10 * edge["coauthorships"] / top))
            edge["size"] = size if size >= 1 else 1
    try:
        db_out["person"].update_one(
            {"_id": idx},
            {"$set": {"coauthorship_network": {
                "nodes": nodes_db, "edges": edges_db
            }}},
            upsert=True,
        )
    except Exception as error:
        print(f"ERROR: Could not store person network for id {idx}", error)


def top_words_process_one(
    config,
    client,
    impactu_client,
    aff,
    stopwords,
    top_words,
    backend,
    force_recalculate=False,
):
    """
    Function to create the network of coauthorships for an affiliation or author.

    Parameters:
    ----------
    config : dict
        The configuration dictionary.
    client : pymongo.database.Database
        The database where the information is stored.
    impactu_client : pymongo.database.Database
        The database where the information will be stored.
    idx : str
        The affiliation or author identifier.
    author_count : int
        The maximum number of authors in a work to consider.
    net : str
        The network type, either affiliations or authors.
    backend : str
        The backend to use for the parallel processing. "mutiprocessing" or "threading".
    """
    if backend != "threading":
        client = MongoClient(config["database_url"])
        db_in = client[config["database_name"]]
        impactu_client = MongoClient(
            config["impactu_postcalculations"]["database_url"])
        db_out = impactu_client[config["impactu_postcalculations"]
                                ["database_name"]]
    else:
        db_in = client[config["database_name"]]
        db_out = impactu_client[
            config["impactu_postcalculations"]["database_name"]
        ]
    if top_words not in ["affiliations", "affiliations_others", "person"]:
        print(
            'ERROR: Invalid network type options are '
            f'{["affiliations", "affiliations_others", "person"]}')
        return

    if top_words == "affiliations":
        top_words_affiliations(
            db_in, db_out, aff, es_model, en_model, stopwords,
            force_recalculate)
    if top_words == "affiliations_others":
        top_words_affiliations_others(
            db_in, db_out, aff, es_model, en_model, stopwords,
            force_recalculate)
    if top_words == "person":
        top_words_person(
            db_in, db_out, aff, es_model, en_model, stopwords,
            force_recalculate,
        )
    if backend != "threading":
        client.close()
        impactu_client.close()


def top_words_affiliations(
    db_in, db_out, aff, es_model, en_model, stopwords,
    force_recalculate=False,
):
    """
    Function to get the top words for an affiliation.

    This algorithm gets the top words for an affiliation based on the titles of the works of the authors of the affiliation.

    Parameters:
    ----------
    db_in : pymongo.database.Database (kahi dabatabase)
        The database where the information is stored.
    db_out : pymongo.database.Database (calculation database)
        The database where the information will be stored.
    aff : dict
        The affiliation information.
    es_model : spacy.lang.es.Spanish
        The Spanish model for the NLP.
    en_model : spacy.lang.en.English
        The English model for the NLP.
    stopwords : list
        The list of stopwords to ignore.
    """
    aff_db = db_out["affiliations"].find_one(
        {"_id": aff["_id"], "top_words": {"$exists": 1}})
    if aff_db and not force_recalculate:
        return
    results = {}
    for work in db_in["works"].find(
            {"authors.affiliations.id": aff["_id"], "titles.title": {"$exists": 1}}, {"titles": 1}):
        title = work["titles"][0]["title"].lower()
        lang = work["titles"][0]["lang"]
        if lang == "es":
            model = es_model
        else:
            model = en_model
        title = model(title)
        for token in title:
            if token.lemma_.isnumeric():
                continue
            if token.lemma_ in stopwords:
                continue
            if len(token.lemma_) < 4:
                continue
            if token.lemma_ in results.keys():
                results[token.lemma_] += 1
            else:
                results[token.lemma_] = 1
    topN = sorted(results.items(), key=lambda x: x[1], reverse=True)[:20]
    results = []
    for top in topN:
        results.append({"name": top[0], "value": top[1]})
    aff_db = db_out["affiliations"].find_one({"_id": aff["_id"]})
    if aff_db:
        db_out["affiliations"].update_one(
            {"_id": aff["_id"]},
            {"$set": {"top_words": results}},
            upsert=True,
        )
    else:
        db_out["affiliations"].insert_one(
            {"_id": aff["_id"], "top_words": results})


def top_words_affiliations_others(
    db_in, db_out, aff, es_model, en_model, stopwords,
    force_recalculate=False,
):
    """
    Function to get the top words for an affiliation for other than institutions, such as group, department, faculty.

    This algorithm gets the top words for an affiliation based on the titles of the works of the authors of the affiliation.

    Parameters:
    ----------
    db_in : pymongo.database.Database (kahi dabatabase)
        The database where the information is stored.
    db_out : pymongo.database.Database (calculation database)
        The database where the information will be stored.
    aff : dict
        The affiliation information.
    es_model : spacy.lang.es.Spanish
        The Spanish model for the NLP.
    en_model : spacy.lang.en.English
        The English model for the NLP.
    stopwords : list
        The list of stopwords to ignore.
    """
    aff_db = db_out["affiliations"].find_one(
        {"_id": aff["_id"], "top_words": {"$exists": 1}})
    if aff_db and not force_recalculate:
        return
    results = {}
    for author in db_in["person"].find(
            {"affiliations.id": aff["_id"]}, {"_id": 1}):
        for work in db_in["works"].find(
                {"authors.id": author["_id"], "titles.title": {"$exists": 1}}, {"titles": 1}):
            title = work["titles"][0]["title"].lower()
            lang = work["titles"][0]["lang"]
            if lang == "es":
                model = es_model
            else:
                model = en_model
            title = model(title)
            for token in title:
                if token.lemma_.isnumeric():
                    continue
                if token.lemma_ in stopwords:
                    continue
                if len(token.lemma_) < 4:
                    continue
                if token.lemma_ in results.keys():
                    results[token.lemma_] += 1
                else:
                    results[token.lemma_] = 1
    topN = sorted(results.items(), key=lambda x: x[1], reverse=True)[:20]
    results = []
    for top in topN:
        results.append({"name": top[0], "value": top[1]})
    db_out["affiliations"].update_one(
        {"_id": aff["_id"]},
        {"$set": {"top_words": results}},
        upsert=True,
    )


def top_words_person(
    db_in, db_out, aff, es_model, en_model, stopwords,
    force_recalculate=False,
):
    """
    Function to get the top words for an author.

    This algorithm gets the top words for an author based on the titles of the works of the author.

    Parameters:
    ----------
    db_in : pymongo.database.Database (kahi dabatabase)
        The database where the information is stored.
    db_out : pymongo.database.Database (calculation database)
        The database where the information will be stored.
    aff : dict
        The author information.
    es_model : spacy.lang.es.Spanish
        The Spanish model for the NLP.
    en_model : spacy.lang.en.English
        The English model for the NLP.
    stopwords : list
        The list of stopwords to ignore.
    """
    aff_db = db_out["person"].find_one(
        {"_id": aff["_id"], "top_words": {"$exists": 1}})
    if aff_db and not force_recalculate:
        return
    results = {}
    for work in db_in["works"].find(
            {"authors.id": aff["_id"], "titles.title": {"$exists": 1}}, {"titles": 1}):
        title = work["titles"][0]["title"].lower()
        lang = work["titles"][0]["lang"]
        if lang == "es":
            model = es_model
        else:
            model = en_model
        title = model(title)
        for token in title:
            if token.lemma_.isnumeric():
                continue
            if token.lemma_ in stopwords:
                continue
            if len(token.lemma_) < 4:
                continue
            if token.lemma_ in results.keys():
                results[token.lemma_] += 1
            else:
                results[token.lemma_] = 1
    topN = sorted(results.items(), key=lambda x: x[1], reverse=True)[:20]
    results = []
    for top in topN:
        results.append({"name": top[0], "value": top[1]})
    aff_db = db_out["person"].find_one({"_id": aff["_id"]})
    if aff_db:
        db_out["person"].update_one(
            {"_id": aff["_id"]}, {"$set": {"top_words": results}})
    else:
        db_out["person"].insert_one({"_id": aff["_id"], "top_words": results})
