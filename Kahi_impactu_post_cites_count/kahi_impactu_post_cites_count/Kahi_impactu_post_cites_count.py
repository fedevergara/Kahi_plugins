from kahi.KahiBase import KahiBase
from pymongo import MongoClient


class Kahi_impactu_post_cites_count(KahiBase):
    """Calculate product and citation totals with set-based MongoDB pipelines."""

    config = {}

    def __init__(self, config):
        self.config = config
        self.mongodb_url = config["database_url"]
        self.database_name = config["database_name"]
        plugin_config = config["impactu_post_cites_count"]
        self.verbose = plugin_config.get("verbose", 0)

        self.client = MongoClient(self.mongodb_url)
        self.db = self.client[self.database_name]
        self.works_collection = self.db["works"]
        self.person_collection = self.db["person"]
        self.affiliations_collection = self.db["affiliations"]
        self.sources_collection = self.db["sources"]

        self.works_collection.create_index("authors.id")
        self.works_collection.create_index("authors.affiliations.id")
        self.works_collection.create_index("source.id")

    @staticmethod
    def _entity_stages(entity):
        if entity == "person":
            return [
                {"$unwind": "$authors"},
                {"$set": {"_entity_id": "$authors.id"}},
            ]
        if entity == "affiliation":
            return [
                {"$unwind": "$authors"},
                {"$unwind": "$authors.affiliations"},
                {"$set": {"_entity_id": "$authors.affiliations.id"}},
            ]
        if entity == "source":
            return [{"$set": {"_entity_id": "$source.id"}}]
        raise ValueError(f"Unsupported entity: {entity}")

    @classmethod
    def products_pipeline(cls, entity, target_collection):
        return cls._entity_stages(entity) + [
            {"$match": {"_entity_id": {"$nin": [None, ""]}}},
            {"$group": {"_id": {"entity": "$_entity_id", "work": "$_id"}}},
            {"$group": {"_id": "$_id.entity", "products_count": {"$sum": 1}}},
            {"$merge": {
                "into": target_collection,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]

    @classmethod
    def citations_pipeline(cls, entity, target_collection):
        return cls._entity_stages(entity) + [
            {"$match": {"_entity_id": {"$nin": [None, ""]}}},
            {"$unwind": "$citations_count"},
            {"$match": {"citations_count.source": {"$nin": [None, ""]}}},
            {"$group": {
                "_id": {
                    "entity": "$_entity_id",
                    "work": "$_id",
                    "source": "$citations_count.source",
                },
                "count": {"$first": {"$convert": {
                    "input": "$citations_count.count",
                    "to": "long",
                    "onError": 0,
                    "onNull": 0,
                }}},
            }},
            {"$group": {
                "_id": {"entity": "$_id.entity", "source": "$_id.source"},
                "count": {"$sum": "$count"},
            }},
            {"$group": {
                "_id": "$_id.entity",
                "citations_count": {"$push": {
                    "source": "$_id.source", "count": "$count"
                }},
            }},
            {"$merge": {
                "into": target_collection,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]

    def _calculate_entity(self, entity, collection):
        if self.verbose:
            print(f"INFO: Calculating products and citations for {entity}")
        collection.update_many(
            {}, {"$set": {"products_count": 0, "citations_count": []}}
        )
        list(self.works_collection.aggregate(
            self.products_pipeline(entity, collection.name),
            allowDiskUse=True,
        ))
        list(self.works_collection.aggregate(
            self.citations_pipeline(entity, collection.name),
            allowDiskUse=True,
        ))

    def run_cites_count(self):
        self._calculate_entity("person", self.person_collection)
        self._calculate_entity("affiliation", self.affiliations_collection)
        self._calculate_entity("source", self.sources_collection)

    def run(self):
        try:
            self.run_cites_count()
        finally:
            self.client.close()
        return 0
