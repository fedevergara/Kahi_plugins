from kahi.KahiBase import KahiBase
from pymongo import MongoClient
from time import time
from joblib import Parallel, delayed


def process_one(kahi_col, wikid_col, inst, verbose):
    """
    This function processes one institution, and seve the the titles and images from wikidata.
    Images are taken from P154 (logo) or P18 (image) if P154 is not present then P18 is used.

    Parameters:
    ----------
    kahi_col: pymongo.collection.Collection
        collection of institutions
    wikid_col: pymongo.collection.Collection
        collection of wikidata records
    inst: dict
        institution record
    verbose: int
    """
    for name in inst["names"]:
        if name["source"] == "wikidata":
            return
    wikidata_id = None
    for j in inst["external_ids"]:
        if j["source"] == "wikidata":
            wikidata_id = j["id"]
            break
    rec = wikid_col.find_one({"id": wikidata_id},
                             {"labels": 1,
                              "claims.P154.mainsnak.datavalue.value": 1,
                              "claims.P18.mainsnak.datavalue.value": 1})

    if not rec:
        if verbose > 4:
            print(f"WARNING: record with id {wikidata_id} not found")
        return
    for lang in rec["labels"].keys():
        name = {"name": rec["labels"][lang]["value"], "lang": lang,
                "source": "wikidata", "provenance": "wikidata"}
        inst["names"].append(name)
    if "P154" in rec["claims"].keys() and len(rec["claims"]["P154"]) > 0:
        if "datavalue" in rec["claims"]["P154"][0]["mainsnak"].keys():
            img = rec["claims"]["P154"][0]["mainsnak"]["datavalue"]["value"]
            url_img = f"https: //commons.wikimedia.org/w/index.php?title=Special: Redirect/file/{img}&width=300"
            inst["external_urls"].append(
                {"provenance": "wikidata", "source": "logo", "url": url_img})
    elif "P18" in rec["claims"].keys() and len(rec["claims"]["P18"]) > 0:
        if "datavalue" in rec["claims"]["P18"][0]["mainsnak"].keys():
            img = rec["claims"]["P18"][0]["mainsnak"]["datavalue"]["value"]
            url_img = f"https: //commons.wikimedia.org/w/index.php?title=Special: Redirect/file/{img}&width=300"
            inst["external_urls"].append(
                {"provenance": "wikidata", "source": "logo", "url": url_img})
    inst["updated"].append({"source": "wikidata", "time": int(time())})
    kahi_col.update_one({"_id": inst["_id"]},
                        {"$set": {"names": inst["names"],
                                  "external_urls": inst["external_urls"],
                                  "updated": inst["updated"]}})


class Kahi_wikidata_affiliations(KahiBase):
    """
    Plugin to process wikidata affiliations.
    Information is taken from the wikidata collection and added to the affiliations collection.

    This plugin is not making requests to wikidata api, it is using the data from the wikidata collection.
    please raead https://github.com/colav-playground/wikidata_laod for more information on how to load the data.
    """

    config = {}

    def __init__(self, config):
        """
        Config is a dictionary with the following keys:
        wikidata_affiliations:
            database_name: wikidata
            collection_name: data
            num_jobs: 5
            verbose: 5

        This has to be added in the workflow file in yml.
        """
        self.config = config

        self.mongodb_url = config["database_url"]

        self.client = MongoClient(self.mongodb_url)

        self.db = self.client[config["database_name"]]
        self.collection = self.db["affiliations"]

        self.wikidata_db = self.client[config["wikidata_affiliations"]
                                       ["database_name"]]
        self.wikidata_col = self.wikidata_db[config["wikidata_affiliations"]
                                             ["collection_name"]]

        self.n_jobs = config["wikidata_affiliations"]["num_jobs"]

        self.verbose = config["wikidata_affiliations"]["verbose"] if "verbose" in config["wikidata_affiliations"].keys(
        ) else 0

        self.wikidata_col.create_index("id")
        self.wikidata_col.create_index(
            "claims.P31.mainsnak.datavalue.value.id")

    def process_wikidata(self):
        institutions = list(self.collection.find(
            {"external_ids.source": "wikidata"}))

        Parallel(
            n_jobs=self.n_jobs,
            backend="threading",
            verbose=10
        )(delayed(process_one)(
            self.collection, self.wikidata_col,
            inst, self.verbose) for inst in institutions)

    def run(self):
        self.process_wikidata()
        return 0
