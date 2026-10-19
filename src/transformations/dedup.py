from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CATEGORIAS = ("DUP_EXACTO", "CUARENTENA", "DUP_CLAVE", "ACEPTADO")


def etiquetar_registros(df: DataFrame, clave: Sequence[str], orden_version: Sequence[str]) -> DataFrame:
    """Agrega `_categoria`. Requiere `_row_hash`, `_ingestion_ts`, `_source_file_hash` y `_dq_cuarentena`."""
    w_exacto = Window.partitionBy("_row_hash").orderBy(
        F.col("_ingestion_ts").asc(), F.col("_source_file_hash").asc()
    )
    df = df.withColumn("__rn_exacto", F.row_number().over(w_exacto))
    elegible = (F.col("__rn_exacto") == 1) & (F.size("_dq_cuarentena") == 0)

    orden = [F.when(elegible, F.lit(0)).otherwise(F.lit(1)).asc()]
    orden += [F.col(c).desc_nulls_last() for c in orden_version]
    orden += [F.col("_ingestion_ts").desc(), F.col("_row_hash").desc()]
    w_clave = Window.partitionBy(*[F.col(c) for c in clave]).orderBy(*orden)
    df = df.withColumn("__rn_clave", F.row_number().over(w_clave))

    categoria = (
        F.when(F.col("__rn_exacto") > 1, F.lit("DUP_EXACTO"))
        .when(F.size("_dq_cuarentena") > 0, F.lit("CUARENTENA"))
        .when(F.col("__rn_clave") > 1, F.lit("DUP_CLAVE"))
        .otherwise(F.lit("ACEPTADO"))
    )
    return df.withColumn("_categoria", categoria).drop("__rn_exacto", "__rn_clave")
