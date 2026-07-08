import pandas as pd

from kahi_impactu_postcalculations.typing import (
    build_type_lookup,
    functors,
    process_minciencias,
    process_type,
)


def test_minciencias_accepts_single_type():
    types = pd.DataFrame(
        {"Tipo": ["Capítulo de libro"], "Tipo ImpactU": ["book-chapter"]}
    )
    result = process_minciencias(
        {"types": [{"source": "minciencias", "type": "Capítulo de libro"}]},
        types,
    )
    assert result["type"] == "book-chapter"


def test_minciencias_accepts_already_normalized_single_type():
    types = pd.DataFrame(
        {
            "Tipo": ["Nuevo conocimiento: Capítulos de libro"],
            "Tipo ImpactU": ["Capítulo de libro"],
        }
    )
    result = process_minciencias(
        {"types": [{"source": "minciencias", "type": "Capítulo de libro"}]},
        types,
    )
    assert result["type"] == "Capítulo de libro"


def test_minciencias_prefers_two_level_mapping():
    types = pd.DataFrame(
        {
            "Tipo": ["Producción bibliográfica: Capítulo de libro"],
            "Tipo ImpactU": ["book-chapter"],
        }
    )
    result = process_minciencias(
        {
            "types": [
                {"type": "Producción bibliográfica"},
                {"type": "Capítulo de libro"},
            ]
        },
        types,
    )
    assert result["type"] == "book-chapter"


def test_crossref_functor_is_registered():
    types = pd.DataFrame(
        {"Tipo": ["journal-article"], "Tipo ImpactU": ["article"]}
    )
    result = functors["crossref"](
        {"types": [{"type": "journal-article"}]}, types
    )
    assert result == {
        "provenance": "crossref",
        "source": "impactu",
        "type": "article",
    }


class _Works:
    def __init__(self):
        self.update = None

    def update_one(self, query, update):
        self.update = (query, update)


def test_process_type_is_idempotent():
    works = _Works()
    db = {"works": works}
    types = pd.DataFrame(
        {
            "Fuente": ["crossref"],
            "Tipo": ["journal-article"],
            "Tipo ImpactU": ["article"],
        }
    )
    process_type(
        db,
        {"_id": "w1", "types": [{"type": "journal-article"}]},
        "crossref",
        types,
        verbose=False,
    )
    assert "$addToSet" in works.update[1]


def test_lookup_matches_dataframe_result():
    types = pd.DataFrame({
        "Fuente": ["coar", "coar"],
        "Tipo": ["article", "book"],
        "Tipo ImpactU": ["Artículo", "Libro"],
    })
    lookup = build_type_lookup(types, "coar")

    assert functors["coar"](
        {"types": [{"type": "article"}]}, lookup
    )["type"] == "Artículo"


def test_warnings_depend_on_verbose(capsys):
    types = pd.DataFrame({
        "Fuente": ["coar"],
        "Tipo": ["article"],
        "Tipo ImpactU": ["Artículo"],
    })
    work = {"_id": "w1", "types": [{"type": "unknown"}, {"type": "other"}]}
    db = {"works": _Works()}

    process_type(db, work, "coar", build_type_lookup(types, "coar"), verbose=False)
    assert capsys.readouterr().out == ""

    process_type(db, work, "coar", build_type_lookup(types, "coar"), verbose=True)
    assert "WARNING:" in capsys.readouterr().out
