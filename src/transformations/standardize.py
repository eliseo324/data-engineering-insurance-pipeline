from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.common.contracts import Columna, Contrato
from src.common.dates import parse_date

LINEAGE_COLUMNS = ["_source_file", "_source_file_hash", "_row_hash", "_ingestion_ts", "_run_id", "_fecha_corte"]


def _clean(col: str) -> Column:
    trimmed = F.trim(F.col(col))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def cast_column(spec: Columna, value: Column) -> Column:
    """Castea un valor de texto ya limpio al tipo declarado en el contrato."""
    tipo = spec.tipo
    if tipo == "string":
        return value
    if tipo == "date":
        return parse_date(value)
    if tipo.startswith("decimal") or tipo in ("int", "bigint", "double"):
        return F.expr(f"try_cast(`{spec.nombre}__clean` AS {tipo})")
    raise ValueError(f"Tipo no soportado: {tipo}")


def standardize(df: DataFrame, contrato: Contrato) -> DataFrame:
    """Devuelve columnas tipadas del contrato + `_raw` + columnas de linaje de Bronze."""
    for spec in contrato.columnas:
        if spec.nombre not in df.columns:
            df = df.withColumn(spec.nombre, F.lit(None).cast("string"))

    raw = F.struct(*[F.col(c).alias(c) for c in contrato.nombres]).alias("_raw")
    cleaned = df.select(
        *[_clean(c).alias(f"{c}__clean") for c in contrato.nombres],
        raw,
        *[F.col(c) for c in LINEAGE_COLUMNS if c in df.columns],
    )
    typed = [
        cast_column(spec, F.col(f"{spec.nombre}__clean")).alias(spec.nombre) for spec in contrato.columnas
    ]
    return cleaned.select(*typed, "_raw", *[c for c in LINEAGE_COLUMNS if c in cleaned.columns])
