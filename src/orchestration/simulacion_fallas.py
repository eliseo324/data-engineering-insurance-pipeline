from __future__ import annotations

import csv
import os

from pyspark.sql import functions as F

from src.common.control import append_rows, utcnow
from src.common.hashing import file_sha256

ESCENARIOS = {
    "esquema": "polizas_SIMULACION_esquema_invalido.csv",
    "calidad": "polizas_SIMULACION_claves_nulas.csv",
}


def _leer(origen: str, n: int) -> tuple[list[str], list[list[str]]]:
    with open(origen, encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter=";")
        header = next(reader)
        filas = [r for _, r in zip(range(n), reader, strict=False)]
    return header, filas


def _escribir(destino: str, header: list[str], filas: list[list[str]]) -> int:
    with open(destino, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow(header)
        w.writerows(filas)
    return len(filas)


def generar_esquema_invalido(origen: str, destino: str, columna: str = "prima", n: int = 50) -> int:
    header, filas = _leer(origen, n)
    i = header.index(columna)
    return _escribir(destino, header[:i] + header[i + 1:], [f[:i] + f[i + 1:] for f in filas])


def generar_claves_nulas(origen: str, destino: str, pct: float = 0.2, n: int = 200) -> int:
    header, filas = _leer(origen, n)
    i = header.index("poliza_id")
    cada = max(1, round(1 / pct))
    for k, f in enumerate(filas):
        if k % cada == 0:
            f[i] = ""
    return _escribir(destino, header, filas)


def archivo_origen(landing_dir: str) -> str:
    """Primer archivo real del dataset (excluye archivos de simulación)."""
    candidatos = sorted(f for f in os.listdir(landing_dir) if f.lower().endswith(".csv") and "SIMULACION" not in f)
    if not candidatos:
        raise FileNotFoundError(f"No hay archivos fuente en {landing_dir}")
    return os.path.join(landing_dir, candidatos[0])


def limpiar_simulacion(spark, cfg, dataset: str, nombre: str, logger) -> dict:
    """Retira un archivo inyectado y revierte sus efectos en Bronze y en la cuarentena (idempotente)."""
    ruta = os.path.join(cfg.landing_path(dataset), nombre)
    hashes = {r[0] for r in spark.table(cfg.ctl("archivos_procesados"))
              .where((F.col("dataset") == dataset) & (F.col("archivo") == nombre)).select("hash_archivo").collect()}
    if os.path.exists(ruta):
        hashes.add(file_sha256(ruta))
        os.remove(ruta)
        logger.info(f"Archivo retirado de la zona de aterrizaje: {ruta}")
    resumen = {"archivo": nombre, "hashes": sorted(hashes), "filas_bronze_borradas": 0}
    if not hashes:
        logger.info("No hay rastros del archivo: nada que limpiar")
        return resumen
    lista = ", ".join(f"'{h}'" for h in hashes)
    if spark.catalog.tableExists(cfg.bronze(dataset)):
        n = spark.table(cfg.bronze(dataset)).where(F.col("_source_file_hash").isin(list(hashes))).count()
        spark.sql(f"DELETE FROM {cfg.bronze(dataset)} WHERE _source_file_hash IN ({lista})")
        resumen["filas_bronze_borradas"] = n
    if spark.catalog.tableExists(cfg.silver("cuarentena")):
        spark.sql(f"UPDATE {cfg.silver('cuarentena')} SET estado_gestion = 'DESCARTADO' "
                  f"WHERE _source_file_hash IN ({lista})")
    append_rows(spark, cfg, "archivos_procesados", [{
        "dataset": dataset, "archivo": nombre, "ruta": ruta, "hash_archivo": h, "tamano_bytes": None,
        "fecha_modificacion": None, "esquema_hash": None, "columnas": None, "registros": 0, "estado": "RETIRADO",
        "mensaje": "Archivo de simulación retirado y revertido", "run_id": cfg.run_id,
        "fecha_corte": cfg.fecha_corte, "fecha_registro": utcnow()} for h in hashes])
    logger.info(f"Limpieza completada: {resumen}")
    return resumen
