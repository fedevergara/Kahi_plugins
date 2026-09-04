from kahi_minciencias_opendata_affiliations.Kahi_minciencias_opendata_affiliations import (
    Kahi_minciencias_opendata_affiliations,
    is_strict_institution_match,
)


def plugin_without_database():
    plugin = Kahi_minciencias_opendata_affiliations.__new__(
        Kahi_minciencias_opendata_affiliations)
    plugin.institution_match_cache = {}
    return plugin


def test_legal_suffix_variant_is_a_strict_match():
    plugin = plugin_without_database()
    match = plugin.institution_match_score(
        {"names": [{"name": "Cementos Argos"}]},
        "cementos argos s a",
    )

    assert is_strict_institution_match(match)


def test_weakly_related_institution_is_not_a_strict_match():
    plugin = plugin_without_database()
    match = plugin.institution_match_score(
        {"names": [{"name": "Universidad de Antioquia"}]},
        "universidad industrial de santander",
    )

    assert not is_strict_institution_match(match)


def test_cataloged_institution_outranks_legacy_iua():
    plugin = plugin_without_database()
    match = plugin.institution_match_score(
        {"names": [{"name": "Cementos Argos"}]},
        "cementos argos",
    )
    catalog = {
        "_id": "01gyh5y52",
        "names": [{"name": "Cementos Argos"}],
        "addresses": [{"city": "Medellín", "state": "Antioquia"}],
        "relations": [],
        "external_ids": [{"source": "minciencias", "id": "000000009892"}],
    }
    synthetic = {
        "_id": "IUA123456",
        "names": [{"name": "Cementos Argos"}],
        "addresses": [{"city": "Medellín", "state": "Antioquia"}],
        "relations": [],
        "external_ids": [],
    }
    reg = {"nme_municipio_gr": "Medellín", "nme_departamento_gr": "Antioquia"}

    assert plugin.institution_candidate_rank(catalog, match, reg) > plugin.institution_candidate_rank(
        synthetic, match, reg)
