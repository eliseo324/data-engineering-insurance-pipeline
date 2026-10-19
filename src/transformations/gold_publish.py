from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import PipelineConfig
from src.common.control import append_rows, utcnow
from src.quality.reconciliation import reconciliar_gold
from src.transformations.enrich import MissingDependencyError
from src.transformations.gold import (
    build_calidad_pipeline,
    build_discrepancias,
    build_poliza_360,
    build_siniestralidad_mensual,
)

CLUSTERING = {
    "poliza_360": ["fecha_corte", "poliza_id"],
    "siniestralidad_mensual": ["fecha_corte", "periodo"],
    "discrepancias": ["fecha_corte", "tipo_discrepancia"],
}


@dataclass
class GoldResult:
    filas: dict = field(default_factory=dict)
    reconciliacion: list[dict] = field(default_factory=list)


def leer_silver(spark: SparkSession, cfg: PipelineConfig) -> dict[str, DataFrame]:
    out = {}
    for ds in ("clientes", "polizas", "recaudos", "siniestros", "movimientos", "cuarentena"):
        fqn = cfg.silver(ds)
        if not spark.catalog.tableExists(fqn):
            raise MissingDependencyError(f"No existe {fqn}. Ejecute la capa Silver antes de Gold.")
        out[ds] = spark.table(fqn)
    return out


def escribir_snapshot(spark: SparkSession, df: DataFrame, nombre: str, cfg: PipelineConfig, logger) -> DataFrame:
    """Escribe el corte de forma idempotente y devuelve lo publicado (leído desde la tabla)."""
    target = cfg.gold(nombre)
    if not spark.catalog.tableExists(target):
        df.write.format("delta").saveAsTable(target)
        cols = CLUSTERING.get(nombre)
        if cols:
            try:
                spark.sql(f"ALTER TABLE {target} CLUSTER BY ({', '.join(cols)})")
            except Exception as exc: 
                logger.warning(f"No se pudo activar liquid clustering en {target}: {exc}")
    else:
        (df.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"fecha_corte = DATE'{cfg.fecha_corte_str}'")
         .option("mergeSchema", "true").saveAsTable(target))
    publicado = spark.table(target).where(F.col("fecha_corte") == F.lit(cfg.fecha_corte))
    logger.info(f"Publicado {target} corte={cfg.fecha_corte_str}")
    return publicado


def publicar_productos(spark: SparkSession, cfg: PipelineConfig, logger) -> GoldResult:
    """Publica póliza 360, siniestralidad mensual y discrepancias, y reconcilia contra Silver."""
    s = leer_silver(spark, cfg)
    res = GoldResult()
    corte = cfg.fecha_corte

    p360 = escribir_snapshot(spark, build_poliza_360(
        s["polizas"], s["clientes"], s["recaudos"], s["siniestros"], s["movimientos"], corte,
        cfg.dias_proximo_vencer), "poliza_360", cfg, logger)
    mensual = escribir_snapshot(spark, build_siniestralidad_mensual(
        s["polizas"], s["siniestros"], s["recaudos"], s["movimientos"], corte), "siniestralidad_mensual", cfg, logger)
    disc = escribir_snapshot(spark, build_discrepancias(
        s["polizas"], s["recaudos"], s["siniestros"], s["movimientos"], s["cuarentena"], p360, corte),
        "discrepancias", cfg, logger)

    res.filas = {"gold_poliza_360": p360.count(), "gold_siniestralidad_mensual": mensual.count(),
                 "gold_discrepancias": disc.count()}
    res.reconciliacion = reconciliar_gold(cfg, s["polizas"], s["recaudos"], s["siniestros"], p360, mensual, utcnow())
    append_rows(spark, cfg, "reconciliacion", res.reconciliacion)
    return res


def publicar_calidad(spark: SparkSession, cfg: PipelineConfig, logger) -> int:
    """gold_calidad_pipeline se reconstruye completo desde las tablas de control (es un consolidado)."""
    df = build_calidad_pipeline(spark.table(cfg.ctl("calidad_resultados")), spark.table(cfg.ctl("reconciliacion")))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.gold("calidad_pipeline"))
    n = spark.table(cfg.gold("calidad_pipeline")).count()
    logger.info(f"Publicado {cfg.gold('calidad_pipeline')} ({n} controles)")
    return n


def verificar_reconciliacion(filas: list[dict]) -> None:
    diferencias = [r for r in filas if r["resultado"] != "OK"]
    if diferencias:
        raise RuntimeError("Reconciliación Silver -> Gold con diferencias: " + "; ".join(
            f"{d['etapa_destino']}.{d['metrica']} {d['valor_origen']} vs {d['valor_destino']}" for d in diferencias))
