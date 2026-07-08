from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from pandas import read_excel, to_datetime, DataFrame
from time import time
from kahi_impactu_utils.Mapping import ciarp_mapping
from kahi_impactu_utils.String import title_case
from kahi_impactu_utils.Utils import parse_sex, doi_processor, split_name_part
from datetime import datetime as dt
from datetime import datetime, timezone


def has_text(value):
    return value is not None and str(value).strip().lower() not in {
        "", "nan", "nat", "none"}


class Kahi_staff_person(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]
        self.collection = self.db["person"]

        self.collection.create_index("external_ids.id")
        self.collection.create_index("external_ids.id.COD_RH")
        self.collection.create_index("affiliations.id")
        self.collection.create_index([("full_name", TEXT)])
        self.db["affiliations"].create_index("relations.id")

        self.verbose = config["verbose"] if "verbose" in config else 0

        self.staff_required_columns = [
            "tipo_documento",
            "identificación",
            "primer_apellido",
            "segundo_apellido",
            "nombres",
            "nivel_académico",
            "tipo_contrato",
            "jornada_laboral",
            "categoría_laboral",
            "sexo",
            "fecha_nacimiento",
            "fecha_inicial_vinculación",
            "fecha_final_vinculación",
            "código_unidad_académica",
            "unidad_académica",
            "código_subunidad_académica",
            "subunidad_académica"]

        self.ciarp_required_columns = [
            "código_unidad_académica",
            "código_subunidad_académica",
            "tipo_documento",
            "identificación",
            "año",
            "título",
            "idioma",
            "revista",
            "editorial",
            "doi",
            "issn",
            "isbn",
            "volumen",
            "issue",
            "primera_página",
            "pais_producto",
            "última_página",
            "entidad_premiadora",
            "ranking"]

    def filter_research_products(
            self,
            df: DataFrame,
            categories: list,
            document: str) -> list:
        """
        Filter a DataFrame of research products by specified categories, identification code,
        and valid DOI field.

        A DOI is considered valid if it is returned by the doi_processor() function.

        Parameters:
        - df (pd.DataFrame): DataFrame containing research products data.
        - categories (list): List of categories to filter the 'ranking' column.
        - document (str): Id to filter the 'identificación' column.

        Returns:
        - list: A list of dictionaries, where each dictionary represents a filtered record.
        """
        # Filter the DataFrame where 'ranking' is in the provided categories list
        # and 'identificación' matches the given cod_rh code.
        filtered_df = df[(df['ranking'].isin(categories)) &
                         (df['identificación'] == document)]

        # Further filter the DataFrame to keep only records with a valid DOI.
        # A DOI is valid if doi_processor(doi) does not return None.
        filtered_df = filtered_df[filtered_df['doi'].apply(
            lambda x: bool(x) and doi_processor(x) is not None)]

        # Convert the filtered DataFrame into a list of dictionaries.
        return filtered_df.to_dict(orient='records')

    def _load_file_data(self, file_path, required_columns):
        dtype_mapping = {col: str for col in required_columns}
        data = read_excel(file_path, dtype=dtype_mapping).fillna("")
        for col in required_columns:
            if col not in data.columns:
                print(
                    f"Column {col} found not in file {file_path}, and it is required.")
                raise ValueError(
                    f"Column {col} found not in file {file_path}")
        return data

    def _source_collection(self, config, collection_key):
        source_url = config.get("database_url", self.config["database_url"])
        source_db_name = config.get("database_name", "institutional_data")
        source_collection_name = config.get(
            f"{collection_key}_collection_name", collection_key)
        client = MongoClient(source_url)
        return client, client[source_db_name][source_collection_name], source_db_name, source_collection_name

    def _institution_ids(self):
        if "databases" in self.config["staff_person"]:
            return [
                config["institution_id"].replace("https://ror.org/", "")
                for config in self.config["staff_person"]["databases"]
            ]

        client, collection, _, _ = self._source_collection(
            self.config["staff_person"], "staff")
        try:
            return sorted(collection.distinct("institution_id"))
        finally:
            client.close()

    def _config_for_institution(self, institution_id):
        for config in self.config["staff_person"].get("databases", []):
            if config["institution_id"].replace(
                    "https://ror.org/", "") == institution_id:
                return config
        return self.config["staff_person"]

    def _load_collection_data(
            self,
            config,
            collection_key,
            institution_id,
            required_columns):
        client, collection, _, _ = self._source_collection(
            config, collection_key)
        try:
            records = list(collection.find(
                {"institution_id": institution_id}, {"_id": 0}))
        finally:
            client.close()

        data = DataFrame(records).fillna("")
        for col in required_columns:
            if col not in data.columns:
                data[col] = ""
            data[col] = data[col].astype(str)
        return data

    def _load_staff_data(self, config, institution_id):
        if "staff_file_path" in config:
            return self._load_file_data(
                config["staff_file_path"],
                self.staff_required_columns)
        data = self._load_collection_data(
            config, "staff", institution_id, self.staff_required_columns)
        if data.empty:
            client, _, source_db_name, source_collection_name = self._source_collection(
                config, "staff")
            client.close()
            raise ValueError(
                f"No staff records found for institution {institution_id} in "
                f"{source_db_name}.{source_collection_name}")
        return data

    def _load_ciarp_data(self, config, institution_id):
        if "ciarp_file_path" in config:
            return self._load_file_data(
                config["ciarp_file_path"],
                self.ciarp_required_columns)
        return self._load_collection_data(
            config, "ciarp", institution_id, self.ciarp_required_columns)

    def process_staff(self):
        # Iterate over the unique identifiers
        for idx in list(self.cedula_dep.keys()):
            # Set to store the years of vinculation
            years = set()
            # Get the current year
            current_year = datetime.now(timezone.utc).year
            # Check if the person is already in the database
            check_db = self.collection.find_one({"external_ids.id": idx})
            if not check_db:
                check_db = self.collection.find_one(
                    {"external_ids.id.COD_RH": idx})
            if check_db:
                continue
            entry = self.empty_person()
            entry["updated"].append(
                {"time": int(time()), "source": "staff", "provenance": "staff"})
            entry["first_names"] = split_name_part(
                self.staff_data[self.staff_data["identificación"] == idx].iloc[0]["nombres"])
            first_lastname = self.staff_data[self.staff_data["identificación"]
                                             == idx].iloc[0]["primer_apellido"]
            entry["last_names"].append(first_lastname.strip())
            second_lastname = self.staff_data[self.staff_data["identificación"]
                                              == idx].iloc[0]["segundo_apellido"]
            if second_lastname.strip() != "":
                entry["last_names"].append(second_lastname.strip())
            entry["full_name"] = " ".join(
                entry["first_names"] + entry["last_names"])
            entry["initials"] = "".join([name[0]
                                        for name in entry["first_names"]])

            for i, reg in self.staff_data[self.staff_data["identificación"] == idx].iterrows(
            ):
                start_date = end_date = None
                start_date = reg.get("fecha_inicial_vinculación")
                end_date = reg.get("fecha_final_vinculación")
                if end_date:
                    end_date = int(
                        datetime.strptime(
                            end_date,
                            "%d/%m/%Y").timestamp())
                if start_date:
                    start_date = int(
                        datetime.strptime(
                            start_date,
                            "%d/%m/%Y").timestamp())
                    start_year = datetime.fromtimestamp(
                        start_date, tz=timezone.utc).year
                    end_year = datetime.fromtimestamp(
                        end_date, tz=timezone.utc).year if end_date else current_year
                    years.update(range(start_year, end_year + 1))

                name = self.staff_reg["names"][0]["name"]
                for n in self.staff_reg["names"]:
                    if n["lang"] == "es":
                        name = n["name"]
                        break
                    elif n["lang"] == "en":
                        name = n["name"]
                name = title_case(name)
                auth_aff = {
                    "id": self.staff_reg["_id"],
                    "name": name,
                    "types": self.staff_reg["types"],
                    "start_date": start_date,
                    "end_date": end_date if end_date else -1}
                if auth_aff["id"] not in [aff["id"]
                                          for aff in entry["affiliations"]]:
                    entry["affiliations"].append(auth_aff)
                # Define a mapping between document types and their respective
                # sources
                document_types = {
                    "cédula de ciudadanía": "Cédula de Ciudadanía",
                    "cédula de extranjería": "Cédula de Extranjería",
                    "pasaporte": "Pasaporte",
                    "COD_RH": "scienti"
                }
                # Get the document type from the record
                doc_type = reg["tipo_documento"]
                # Check if the document type is in the predefined mapping
                if reg["tipo_documento"] in document_types:
                    # Construct the ID entry
                    id_entry = {
                        "provenance": "staff",
                        # Get corresponding source name
                        "source": document_types[doc_type],
                        "id": idx if doc_type != "COD_RH" else {"COD_RH": idx}
                    }
                    # Add the entry only if it's not already in the list
                    if id_entry not in entry["external_ids"]:
                        entry["external_ids"].append(id_entry)
                else:
                    # Print an error message if the document type is invalid
                    print(
                        f"ERROR: tipo_documento must be one of {', '.join(document_types.keys())}, not '{doc_type}'")

                if reg["nombres"].lower() not in entry["aliases"]:
                    entry["aliases"].append(reg["nombres"].lower())
                dep = None
                if has_text(reg["subunidad_académica"]):
                    dep = self.db["affiliations"].find_one({"names.name": title_case(
                        reg["subunidad_académica"]), "relations.id": self.staff_reg["_id"]})
                if dep:
                    name = dep["names"][0]["name"]
                    for n in dep["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    name = title_case(name)
                    dep_affiliation = {
                        "id": dep["_id"],
                        "name": name,
                        "types": dep["types"],
                        "start_date": start_date,
                        "end_date": end_date if end_date else -1}
                    if dep_affiliation["id"] not in [aff["id"]
                                                     for aff in entry["affiliations"]]:
                        entry["affiliations"].append(dep_affiliation)
                fac = None
                if has_text(reg["unidad_académica"]):
                    fac = self.db["affiliations"].find_one({"names.name": title_case(
                        reg["unidad_académica"]), "relations.id": self.staff_reg["_id"]})
                if fac:
                    name = fac["names"][0]["name"]
                    for n in fac["names"]:
                        if n["lang"] == "es":
                            name = n["name"]
                            break
                        elif n["lang"] == "en":
                            name = n["name"]
                    name = title_case(name)
                    fac_affiliation = {
                        "id": fac["_id"],
                        "name": name,
                        "types": fac["types"],
                        "start_date": start_date,
                        "end_date": end_date if end_date else -1}
                    if fac_affiliation["id"] not in [aff["id"]
                                                     for aff in entry["affiliations"]]:
                        entry["affiliations"].append(fac_affiliation)

                if reg["fecha_nacimiento"] != "":
                    entry["birthdate"] = int(
                        dt.strptime(
                            reg["fecha_nacimiento"],
                            "%d/%m/%Y").timestamp())
                sex = reg.get("sexo")
                if sex:
                    entry["sex"] = sex.capitalize() if sex.capitalize() in {
                        "Hombre", "Mujer", "Intersexual"} else parse_sex(sex)
                if reg["nivel_académico"]:
                    degree = {
                        "date": -1,
                        "degree": reg["nivel_académico"],
                        "id": "",
                        "institutions": [],
                        "source": "nivel_académico",
                        "provenance": "staff"}
                    if degree not in entry["degrees"]:
                        entry["degrees"].append(degree)
                if reg["tipo_contrato"]:
                    ranking = {
                        "date": start_date,
                        "rank": reg["tipo_contrato"],
                        "source": "tipo_contrato",
                        "provenance": "staff"}
                    if ranking not in entry["ranking"]:
                        entry["ranking"].append(ranking)
                if reg["jornada_laboral"]:
                    ranking = {
                        "date": start_date,
                        "rank": reg["jornada_laboral"],
                        "source": "jornada_laboral",
                        "provenance": "staff"}
                    if ranking not in entry["ranking"]:
                        entry["ranking"].append(ranking)
                if reg["categoría_laboral"]:
                    ranking = {
                        "date": start_date,
                        "rank": reg["categoría_laboral"],
                        "source": "categoría_laboral",
                        "provenance": "staff"}
                    if ranking not in entry["ranking"]:
                        entry["ranking"].append(ranking)

            # Set vinculations years to affiliation
            aff = next(
                (a for a in entry["affiliations"] if a["id"] == self.staff_reg["_id"]),
                None)
            years_list = sorted(list(years)) if years else []
            aff["years"] = years_list

            # Get the research products for the author
            if self.allowed_categories:
                author_works = self.filter_research_products(
                    self.ciarp_data, self.allowed_categories, idx)
                if author_works:
                    for work in author_works:
                        work_doi = doi_processor(work["doi"])
                        if work_doi:
                            rec = {
                                "provenance": "ciarp",
                                "source": "doi",
                                "id": work_doi,
                                "year": work["año"]}
                            if rec not in entry["related_works"]:
                                entry["related_works"].append(rec)

            # Add the entry to the database
            self.collection.insert_one(entry)

    def run(self):
        if self.verbose > 4:
            start_time = time()

        for source_institution_id in self._institution_ids():
            config = self._config_for_institution(source_institution_id)
            institution_id = f"https: //ror.org/{source_institution_id}"
            # Get the allowed categories for the institution
            try:
                self.allowed_categories = ciarp_mapping(
                    institution_id, "works")
            except ValueError:
                self.allowed_categories = []

            self.staff_reg = self.db["affiliations"].find_one(
                {"external_ids.id": institution_id})
            if not self.staff_reg:
                self.staff_reg = self.db["affiliations"].find_one(
                    {"_id": source_institution_id})
            if not self.staff_reg:
                print("Institution not found in database")
                raise ValueError(
                    f"Institution {institution_id} found not in database")

            self.staff_data = self._load_staff_data(
                config, source_institution_id)
            self.ciarp_data = self._load_ciarp_data(
                config, source_institution_id)

            # logs for higher verbosity
            self.facs_inserted = {}
            self.deps_inserted = {}
            self.fac_dep = []

            self.cedula_dep = {}
            self.cedula_fac = {}

            if self.verbose > 1:
                print(
                    "Processing staff authors for institution: ",
                    self.staff_reg["names"][0]["name"])

            for idx, reg in self.staff_data.iterrows():
                self.cedula_fac[reg["identificación"]] = title_case(
                    reg["unidad_académica"])
                self.cedula_dep[reg["identificación"]] = title_case(
                    reg["subunidad_académica"])

            # convert dates to the correct format
            for col in [
                "fecha_nacimiento",
                "fecha_inicial_vinculación",
                    "fecha_final_vinculación"]:
                self.staff_data[col] = to_datetime(
                    self.staff_data[col], dayfirst=True, errors='coerce')
                self.staff_data[col] = self.staff_data[col].dt.strftime(
                    '%d/%m/%Y').fillna('')

            self.facs_inserted = {}
            self.deps_inserted = {}
            self.fac_dep = []

            self.process_staff()

        if self.verbose > 4:
            print("Execution time: {} minutes".format(
                round((time() - start_time) / 60, 2)))
        return 0
