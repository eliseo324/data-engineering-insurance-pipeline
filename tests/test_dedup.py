from datetime import date, datetime

from src.transformations.dedup import etiquetar_registros

SCHEMA = ("id STRING, fecha DATE, valor INT, _row_hash STRING, _ingestion_ts TIMESTAMP, "
          "_source_file_hash STRING, _dq_cuarentena ARRAY<STRING>")
T1, T2 = datetime(2026, 9, 5, 6, 0), datetime(2026, 10, 5, 6, 0)


def _etiquetas(spark, rows):
    df = spark.createDataFrame(rows, SCHEMA)
    out = etiquetar_registros(df, ["id"], ["fecha"])
    return {(r["id"], r["valor"], r["_row_hash"], r["_source_file_hash"]): r["_categoria"] for r in out.collect()}


def test_duplicado_exacto_y_version_mas_reciente(spark):
    et = _etiquetas(spark, [
        ("A", date(2026, 1, 1), 1, "h1", T1, "f1", []),
        ("A", date(2026, 1, 1), 1, "h1", T1, "f2", []),
        ("A", date(2026, 2, 1), 2, "h2", T1, "f1", []),
        ("B", date(2026, 1, 1), 3, "h3", T1, "f1", []),
    ])
    assert et[("A", 2, "h2", "f1")] == "ACEPTADO"
    assert et[("B", 3, "h3", "f1")] == "ACEPTADO"
    assert sorted(v for k, v in et.items() if k[2] == "h1") == ["DUP_CLAVE", "DUP_EXACTO"]


def test_misma_marca_temporal_desempate_deterministico(spark):
    rows = [
        ("A", date(2026, 1, 1), 1, "h_a", T1, "f1", []),
        ("A", date(2026, 1, 1), 2, "h_b", T2, "f2", []),
        ("C", date(2026, 1, 1), 5, "h_x", T1, "f1", []),
        ("C", date(2026, 1, 1), 6, "h_y", T1, "f1", []),
    ]
    for _ in range(2):
        et = _etiquetas(spark, rows)
        assert et[("A", 2, "h_b", "f2")] == "ACEPTADO" and et[("A", 1, "h_a", "f1")] == "DUP_CLAVE"
        assert et[("C", 6, "h_y", "f1")] == "ACEPTADO" and et[("C", 5, "h_x", "f1")] == "DUP_CLAVE"


def test_version_mas_reciente_invalida_cede_a_la_ultima_valida(spark):
    et = _etiquetas(spark, [
        ("A", date(2026, 1, 1), 1, "h1", T1, "f1", []),
        ("A", date(2026, 3, 1), -9, "h2", T1, "f1", ["t.VAL.valor"]),
    ])
    assert et[("A", -9, "h2", "f1")] == "CUARENTENA"
    assert et[("A", 1, "h1", "f1")] == "ACEPTADO"
