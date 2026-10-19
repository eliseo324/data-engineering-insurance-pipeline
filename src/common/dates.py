from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

DEFAULT_DATE_FORMATS: tuple[str, ...] = ("yyyy-MM-dd", "dd/MM/yyyy", "yyyy/MM/dd")


def parse_date(col: str | Column, formats: tuple[str, ...] = DEFAULT_DATE_FORMATS) -> Column:
    """Devuelve una columna `date` probando los formatos en orden; NULL si ninguno aplica."""
    c = F.trim(F.col(col) if isinstance(col, str) else col)
    attempts = [F.try_to_timestamp(c, F.lit(fmt)).cast("date") for fmt in formats]
    return F.coalesce(*attempts)


def detect_date_format(col: str | Column) -> Column:
    """Clasifica el patrón textual de una fecha (útil para perfilamiento y observabilidad)."""
    c = F.trim(F.col(col) if isinstance(col, str) else col)
    return (
        F.when(c.isNull() | (c == ""), F.lit("NULO"))
        .when(c.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.lit("yyyy-MM-dd"))
        .when(c.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.lit("dd/MM/yyyy"))
        .when(c.rlike(r"^\d{4}/\d{2}/\d{2}$"), F.lit("yyyy/MM/dd"))
        .otherwise(F.lit("OTRO"))
    )
