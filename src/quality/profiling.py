from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.contracts import Contrato
from src.common.dates import detect_date_format

DATE_PATTERNS = ("yyyy-MM-dd", "dd/MM/yyyy", "yyyy/MM/dd", "OTRO")


def _is_blank(c: str):
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def profile_dataframe(df: DataFrame, contrato: Contrato) -> DataFrame:
    """Calcula métricas por columna y a nivel dataset. 
    `df` debe venir con todas las columnas string.  (alias, columna, metrica)"""
    exprs = [F.count(F.lit(1)).alias("__total")]
    metricas: list[tuple[str, str, str]] = []  

    def add(expr, columna: str, metrica: str):
        alias = f"m{len(metricas)}"
        exprs.append(expr.alias(alias))
        metricas.append((alias, columna, metrica))

    for col in df.columns:
        if col.startswith("_"):
            continue
        add(F.sum(_is_blank(col).cast("int")), col, "nulos_o_vacios")
        add(F.approx_count_distinct(col), col, "distintos_aprox")
        add(F.max(F.length(col)), col, "longitud_max")
        spec = next((c for c in contrato.columnas if c.nombre == col), None)
        if spec is None:
            add(F.lit(1), col, "columna_no_declarada_en_contrato")
            continue
        if spec.tipo == "date":
            fmt = detect_date_format(col)
            for p in DATE_PATTERNS:
                add(F.sum((fmt == p).cast("int")), col, f"formato_{p}")
        if spec.es_monetaria or spec.tipo in ("int", "bigint", "double"):
            num = F.expr(f"try_cast(`{col}` AS DOUBLE)")
            add(F.sum((num.isNull() & ~_is_blank(col)).cast("int")), col, "no_numericos")
            add(F.sum((num < 0).cast("int")), col, "negativos")
            add(F.sum((num == 0).cast("int")), col, "ceros")
            add(F.min(num), col, "minimo")
            add(F.max(num), col, "maximo")
            add(F.sum(num), col, "suma")
        if spec.valores_permitidos:
            ok = F.trim(F.col(col)).isin(list(spec.valores_permitidos))
            add(F.sum((~ok & ~_is_blank(col)).cast("int")), col, "fuera_de_dominio")
        if spec.patron:
            add(F.sum((~F.col(col).rlike(spec.patron) & ~_is_blank(col)).cast("int")), col, "no_cumple_patron")

    data_cols = [c for c in df.columns if not c.startswith("_")]
    key_cols = [c for c in contrato.clave_negocio if c in df.columns]
    row = df.agg(*exprs).first()
    total = int(row["__total"])
    exact_distinct = df.select(*data_cols).distinct().count()
    key_distinct = df.select(*key_cols).distinct().count() if key_cols else total

    spark = df.sparkSession
    records = [
        (contrato.dataset, "*", "registros", str(total)),
        (contrato.dataset, "*", "columnas", str(len(data_cols))),
        (contrato.dataset, "*", "duplicados_exactos", str(total - exact_distinct)),
        (contrato.dataset, ",".join(key_cols), "duplicados_clave_negocio", str(total - key_distinct)),
    ]
    for alias, columna, metrica in metricas:
        val = row[alias]
        records.append((contrato.dataset, columna, metrica, None if val is None else str(val)))
        if metrica == "nulos_o_vacios" and total:
            records.append((contrato.dataset, columna, "pct_nulos_o_vacios", f"{100.0 * (val or 0) / total:.2f}"))
    return spark.createDataFrame(records, "dataset STRING, columna STRING, metrica STRING, valor STRING")


def orphan_count(child: DataFrame, child_col: str, parent: DataFrame, parent_col: str) -> DataFrame:
    """Registros hijos cuya clave foránea no existe en el padre (left anti join)."""
    parents = parent.select(F.trim(F.col(parent_col)).alias("__k")).distinct()
    return child.join(parents, F.trim(F.col(child_col)) == F.col("__k"), "left_anti")
