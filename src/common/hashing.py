from __future__ import annotations

import hashlib
from collections.abc import Iterable

from pyspark.sql import Column
from pyspark.sql import functions as F

_NULL_TOKEN = "∅"
_SEP = "||"


def row_hash(columns: Iterable[str]) -> Column:
    """SHA-256 determinístico del contenido de las columnas indicadas (en el orden dado).

    Se usa para detectar duplicados exactos y como identificador estable de un registro.
    """
    cols = [F.coalesce(F.col(c).cast("string"), F.lit(_NULL_TOKEN)) for c in columns]
    return F.sha2(F.concat_ws(_SEP, *cols), 256)


def schema_hash(columns: Iterable[str]) -> str:
    """Huella del esquema recibido (nombres de columna en orden) para detectar evolución."""
    return hashlib.sha256(",".join(columns).encode("utf-8")).hexdigest()


def file_sha256(path: str, chunk_size: int = 1024 * 1024) -> str:
    """SHA-256 del contenido de un archivo. Identifica reenvíos aunque cambie el nombre."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()
