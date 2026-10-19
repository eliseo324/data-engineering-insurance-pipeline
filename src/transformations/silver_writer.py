from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession

FECHA_MIN = "DATE'1900-01-01'"


def _q(c: str) -> str:
    return f"`{c}`"


def condicion_claves(clave: Sequence[str], t: str = "t", s: str = "s") -> str:
    return " AND ".join(f"{t}.{_q(k)} = {s}.{_q(k)}" for k in clave)


def condicion_mas_reciente(orden_version: Sequence[str], t: str = "t", s: str = "s") -> str:
    """Comparación lexicográfica (orden_version..., _ingestion_ts): la fila entrante es más reciente."""
    cols = list(orden_version) + ["_ingestion_ts"]

    def val(alias, c):
        return f"coalesce({alias}.{_q(c)}, {FECHA_MIN})" if c != "_ingestion_ts" else f"{alias}.{_q(c)}"

    partes = []
    for i, c in enumerate(cols):
        iguales = [f"{val(s, p)} = {val(t, p)}" for p in cols[:i]]
        partes.append("(" + " AND ".join(iguales + [f"{val(s, c)} > {val(t, c)}"]) + ")")
    return "(" + " OR ".join(partes) + ")"


def sql_merge_tipo1(target: str, source_view: str, clave: Sequence[str], orden_version: Sequence[str]) -> str:
    return f"""
MERGE WITH SCHEMA EVOLUTION INTO {target} AS t
USING {source_view} AS s
ON {condicion_claves(clave)}
WHEN MATCHED AND t._row_hash <> s._row_hash AND {condicion_mas_reciente(orden_version)}
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""".strip()


def sql_merge_append_only(target: str, source_view: str) -> str:
    return f"""
MERGE WITH SCHEMA EVOLUTION INTO {target} AS t
USING {source_view} AS s
ON t._event_key = s._event_key
WHEN NOT MATCHED THEN INSERT *
""".strip()


def sql_merge_scd2(target: str, source_view: str, clave: Sequence[str], columnas: Sequence[str]) -> str:
    """SCD tipo 2 en un solo MERGE (patrón de staging con `_merge_key`).

    La fuente se duplica: (a) con `_merge_key` = clave, para cerrar la versión vigente cuando los
    atributos cambian; (b) con `_merge_key` NULL, para insertar la nueva versión. Una fila idéntica
    a la vigente no genera cambios (idempotencia). Una versión más antigua que la vigente no altera
    la historia (deuda técnica DT-02).
    """
    key_expr_s = "concat_ws('|', " + ", ".join(f"s.{_q(k)}" for k in clave) + ")"
    key_expr_t = "concat_ws('|', " + ", ".join(f"t.{_q(k)}" for k in clave) + ")"
    join_cur = " AND ".join(f"cur.{_q(k)} = s.{_q(k)}" for k in clave)
    cols = ", ".join(_q(c) for c in columnas)
    vals = ", ".join(f"u.{_q(c)}" for c in columnas)
    return f"""
MERGE WITH SCHEMA EVOLUTION INTO {target} AS t
USING (
  SELECT {key_expr_s} AS _merge_key, s.* FROM {source_view} AS s
  UNION ALL
  SELECT NULL AS _merge_key, s.* FROM {source_view} AS s
  JOIN {target} AS cur ON {join_cur} AND cur.es_actual
  WHERE cur._attr_hash <> s._attr_hash AND s.vigente_desde >= cur.vigente_desde
) AS u
ON {key_expr_t} = u._merge_key AND t.es_actual
WHEN MATCHED AND t._attr_hash <> u._attr_hash AND u.vigente_desde >= t.vigente_desde THEN UPDATE SET
  t.es_actual = false, t.vigente_hasta = u.vigente_desde,
  t._silver_run_id = u._silver_run_id, t._silver_ts = u._silver_ts
WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})
""".strip()


QUARANTINE_DDL = """
    dataset STRING, clave_negocio STRING, regla_id STRING, reglas_incumplidas ARRAY<STRING>,
    severidad STRING, dimension STRING, descripcion STRING, registro STRING,
    _source_file STRING, _source_file_hash STRING, _row_hash STRING, _bronze_run_id STRING,
    run_id STRING, pipeline STRING, fecha_corte DATE, fecha_cuarentena TIMESTAMP,
    ultima_deteccion_run_id STRING, ultima_deteccion_ts TIMESTAMP, estado_gestion STRING
"""


def sql_merge_cuarentena(target: str, source_view: str) -> str:
    """Idempotente por (dataset, _row_hash, _source_file_hash): un mismo registro rechazado en varias
    ejecuciones queda una sola vez, con su primera fecha y la última detección."""
    return f"""
MERGE INTO {target} AS t
USING {source_view} AS s
ON t.dataset = s.dataset AND t._row_hash = s._row_hash AND t._source_file_hash = s._source_file_hash
WHEN MATCHED THEN UPDATE SET
  t.reglas_incumplidas = s.reglas_incumplidas, t.regla_id = s.regla_id,
  t.ultima_deteccion_run_id = s.run_id, t.ultima_deteccion_ts = s.fecha_cuarentena
WHEN NOT MATCHED THEN INSERT *
""".strip()


def asegurar_tabla(spark: SparkSession, df: DataFrame, target: str) -> None:
    """Crea la tabla vacía con el esquema del DataFrame si no existe (el MERGE requiere destino)."""
    if not spark.catalog.tableExists(target):
        df.limit(0).write.format("delta").saveAsTable(target)


def asegurar_cuarentena(spark: SparkSession, target: str) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} ({' '.join(QUARANTINE_DDL.split())}) USING DELTA "
        "COMMENT 'Registros rechazados por reglas de calidad, con regla, fecha, pipeline y run_id'"
    )


def ejecutar_merge(spark: SparkSession, df: DataFrame, view_name: str, sql: str) -> None:
    df.createOrReplaceTempView(view_name)
    spark.sql(sql)
