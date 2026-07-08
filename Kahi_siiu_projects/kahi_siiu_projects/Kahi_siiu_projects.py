from kahi.KahiBase import KahiBase
from kahi_siiu_projects.process_one import process_one
from pymongo import MongoClient
from joblib import Parallel, delayed


class Kahi_siiu_projects(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config
        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["projects"]

        self.siiu_client = MongoClient(
            config["siiu_projects"]["database_url"])
        if config["siiu_projects"]["database_name"] not in list(self.siiu_client.list_database_names()):
            raise RuntimeError(
                f'''Database {config["siiu_projects"]["database_name"]} was not found''')
        self.siiu_db = self.siiu_client[config["siiu_projects"]
                                        ["database_name"]]
        if config["siiu_projects"]["collection_name"] not in self.siiu_db.list_collection_names():
            raise RuntimeError(
                f'''Collection {config["siiu_projects"]["collection_name"]} was not found on database {config["siiu_projects"]["database_name"]}''')
        self.siiu_collection = self.siiu_db[config["siiu_projects"]
                                            ["collection_name"]]

        self.task = config["siiu_projects"].get("task")
        self.n_jobs = config["siiu_projects"]["num_jobs"] if "num_jobs" in config["siiu_projects"].keys(
        ) else 1
        self.verbose = config["siiu_projects"]["verbose"] if "verbose" in config["siiu_projects"].keys(
        ) else 0
        self.collection.create_index("external_ids.id")
        self.siiu_collection.create_index("project_participant")

    def run(self):
        project_cursor = self.siiu_collection.find(
            {"project_participant": {"$exists": True}}).batch_size(500)
        # process_one(siiu_reg, db, collection, empty_project, es_handler, verbose=0):
        try:
            results = Parallel(
                n_jobs=self.n_jobs, verbose=self.verbose, backend="threading",
                pre_dispatch="2*n_jobs", return_as="generator_unordered")(
                delayed(process_one)(
                    project, self.db, self.collection, self.empty_project(),
                    None, verbose=self.verbose
                ) for project in project_cursor
            )
            for _ in results:
                pass
        finally:
            project_cursor.close()
            self.siiu_client.close()
        return 0
