from joblib import Parallel, delayed
from kahi.KahiBase import KahiBase
from mohan.Similarity import Similarity
from pymongo import MongoClient
from threading import BoundedSemaphore
from elasticsearch import Elasticsearch

from kahi_dspace_works.process_one import process_one
from kahi_dspace_works.utils import (
    get_doi,
    is_similarity_reg,
    is_thesis_work,
    process_affiliation,
)


DIM_FIELDS = "OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field"


def normalize_repository_url(url):
    """Convert a DSpace OAI endpoint into the repository public base URL."""
    if not url:
        return ""
    url = url.rstrip("/")
    for suffix in (
        "/server/oai/request",
        "/oai/request",
        "/oai-pmh/request",
        "/oai",
    ):
        if url.lower().endswith(suffix):
            return url[: -len(suffix)]
    return url


def doi_query():
    """Return records containing a non-empty DOI field."""
    return _dim_query(
        {
            "@element": "identifier",
            "@qualifier": "doi",
            "#text": {"$exists": True, "$nin": [None, ""]},
        }
    )


def title_query():
    """Return records containing a non-empty title field."""
    return _dim_query(
        {
            "@element": "title",
            "#text": {"$exists": True, "$nin": [None, ""]},
        }
    )


def _dim_query(condition):
    """Match one DIM field whether xmltodict produced a list or a singleton."""
    singleton = [
        {"$expr": {"$eq": [{"$type": f"${DIM_FIELDS}"}, "object"]}}
    ]
    singleton.extend(
        {f"{DIM_FIELDS}.{key}": value} for key, value in condition.items()
    )
    return {
        "$or": [
            {DIM_FIELDS: {"$elemMatch": condition}},
            {"$and": singleton},
        ]
    }


class Kahi_dspace_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config
        plugin_config = config["dspace_works"]

        self.mongodb_url = config["database_url"]
        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[config["database_name"]]
        self.collection = self.db["works"]

        self.task = plugin_config.get("task")
        self.n_jobs = plugin_config.get("num_jobs", 1)
        self.verbose = plugin_config.get("verbose", 0)
        self.batch_size = plugin_config.get("batch_size", 500)
        self.es_max_concurrency = plugin_config.get("es_max_concurrency", 8)
        if self.es_max_concurrency < 1:
            raise ValueError(
                "dspace_works.es_max_concurrency must be greater than zero"
            )

        thresholds = plugin_config.get("thresholds")
        if thresholds and len(thresholds) == 3:
            self.thresholds = {
                "author_thd": thresholds[0],
                "paper_thd_low": thresholds[1],
                "paper_thd_high": thresholds[2],
            }
        else:
            self.thresholds = {
                "author_thd": 65,
                "paper_thd_low": 90,
                "paper_thd_high": 95,
            }

        es_keys = ("es_index", "es_url", "es_user", "es_password")
        if all(key in plugin_config for key in es_keys):
            es_auth = None
            if plugin_config["es_user"] and plugin_config["es_password"]:
                es_auth = (
                    plugin_config["es_user"],
                    plugin_config["es_password"])
            self.es_handler = Similarity(
                plugin_config["es_index"],
                es_uri=plugin_config["es_url"],
                es_auth=es_auth,
            )
            # Similarity creates an Elasticsearch client with a pool of 10
            # connections. Match the pool to the configured semaphore so
            # workers never fail with EmptyPoolError under higher concurrency.
            previous_client = self.es_handler.es
            self.es_handler.es = Elasticsearch(
                plugin_config["es_url"],
                basic_auth=es_auth,
                request_timeout=120,
                retry_on_timeout=True,
                max_retries=5,
                connections_per_node=max(10, self.es_max_concurrency),
            )
            previous_client.close()
            print("INFO: ES handler created successfully")
        else:
            self.es_handler = None
            print("WARNING: No elasticsearch configuration provided")

        self.es_semaphore = None
        if self.es_handler is not None:
            self.es_semaphore = BoundedSemaphore(self.es_max_concurrency)

        print("INFO: Creating index for full_name in person collection")
        self.db["person"].create_index("full_name")
        self.db["person"].create_index(
            [("full_name", 1), ("affiliations.id", 1)],
            name="full_name_affiliation_es",
            collation={"locale": "es", "strength": 1},
        )
        self.collection.create_index("external_ids.id")

    def _repositories(self, source_db):
        """Resolve explicit repositories or discover every ``*_records`` collection."""
        plugin_config = self.config["dspace_works"]
        explicit = plugin_config.get("repositories")
        if explicit:
            for repository in explicit:
                yield repository
            return

        affiliations = plugin_config.get("repository_affiliations", {})
        include = set(plugin_config.get("collections", []))
        exclude = set(plugin_config.get("exclude_collections", []))
        collection_names = sorted(
            name
            for name in source_db.list_collection_names()
            if name.startswith("dspace_")
            and name.endswith("_records")
            and (not include or name in include)
            and name not in exclude
        )
        for collection_name in collection_names:
            identity_name = collection_name[: -len("records")] + "identity"
            identity = source_db[identity_name].find_one(
                {}, {"base_url": 1}) or {}
            yield {
                "collection_name": collection_name,
                "institution_id": affiliations.get(collection_name),
                "repository_url": normalize_repository_url(identity.get("base_url")),
            }

    def process_repository(self, affiliation, base_url, dspace_collection):
        if self.task == "doi":
            cursor = dspace_collection.find(
                doi_query()).batch_size(
                self.batch_size)
            records = (
                work
                for work in cursor
                if get_doi(work) and not is_thesis_work(work)
            )
            similarity = False
        else:
            cursor = dspace_collection.find(
                title_query()).batch_size(
                self.batch_size)
            records = (work for work in cursor if is_similarity_reg(work))
            similarity = True

        try:
            results = Parallel(
                n_jobs=self.n_jobs,
                verbose=self.verbose,
                backend="threading",
                pre_dispatch="2*n_jobs",
                return_as="generator_unordered",
            )(
                delayed(process_one)(
                    dspace_reg=work,
                    affiliation=affiliation,
                    base_url=base_url,
                    db=self.db,
                    collection=self.collection,
                    empty_work=self.empty_work(),
                    es_handler=self.es_handler,
                    similarity=similarity,
                    thresholds=self.thresholds,
                    verbose=self.verbose,
                    es_semaphore=self.es_semaphore,
                )
                for work in records
            )
            for _ in results:
                pass
        finally:
            cursor.close()

    def run(self):
        print(
            f"INFO: Running dspace works with num_jobs = {self.n_jobs} "
            f"task = {self.task}"
        )
        plugin_config = self.config["dspace_works"]
        source_client = MongoClient(plugin_config["database_url"])
        try:
            source_db = source_client[plugin_config["database_name"]]
            repositories = list(self._repositories(source_db))
            if not repositories:
                raise ValueError("No DSpace *_records collections were found")

            for repository in repositories:
                collection_name = repository["collection_name"]
                institution_id = repository.get("institution_id")
                base_url = normalize_repository_url(
                    repository.get("repository_url"))
                print(
                    f"INFO: Processing collection {collection_name} "
                    f"institution {institution_id or 'unmapped'}"
                )
                affiliation = (
                    process_affiliation(institution_id, self.db)
                    if institution_id
                    else None
                )
                if institution_id and affiliation is None and self.verbose:
                    print(
                        "WARNING: Affiliation not found for institution "
                        f"{institution_id} - processing without affiliation"
                    )
                self.process_repository(
                    affiliation, base_url, source_db[collection_name]
                )
        finally:
            source_client.close()

        return 0
