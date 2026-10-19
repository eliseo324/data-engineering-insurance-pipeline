from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import PipelineConfig
from src.common.contracts import Contrato
from src.common.control import append_rows, get_watermark, set_watermark, utcnow
from src.common.hashing import row_hash
from src.quality.reconciliation import (
    medir_entrada,
    reconciliar_destinos,
    reconciliar_lote,
    resumen_categorias,
)
from src.quality.rules import (
    ConfigCalidad,
    Regla,
    aplicar_reglas,
    cargar_config_calidad,
    resultado_regla,
    resumir_reglas,
    verificar_bloqueos,
)
from src.transformations import silver_writer as W
from src.transformations.dedup import etiquetar_registros
from src.transformations.enrich import COLUMNAS_DERIVADAS, aplicar_derivaciones, enriquecer_referencias
from src.transformations.standardize import standardize

CAPA = "silver"


@dataclass
class SilverResult:
    dataset: str
    omitido: bool = False
    registros_entrada: int = 0
    aceptados: int = 0
    cuarentena: int = 0
    dup_exactos: int = 0
    dup_clave: int = 0
    resultados_calidad: list[dict] = field(default_factory=list)
    reconciliacion: list[dict] = field(default_factory=list)


def leer_lote_bronze(spark: SparkSession, cfg: PipelineConfig, dataset: str):
    """Modo normal: solo lo ingerido después del último watermark exitoso. Reproceso: todo Bronze."""
    bronze = spark.table(cfg.bronze(dataset))
    if cfg.modo == "reproceso":
        return bronze, None
    wm = get_watermark(spark, cfg, dataset, CAPA)
    return (bronze.where(F.col("_ingestion_ts") > F.lit(wm)) if wm else bronze), wm


def clasificar_lote(df_enriquecido: DataFrame, contrato: Contrato, reglas) -> DataFrame:
    evaluado = aplicar_reglas(df_enriquecido, reglas)
    return etiquetar_registros(evaluado, contrato.clave_negocio, contrato.orden_version)


def columnas_silver(contrato: Contrato, columnas_disponibles: list[str]) -> list[str]:
    derivadas = COLUMNAS_DERIVADAS.get(contrato.dataset, [])
    refs = [c for c in columnas_disponibles if c.startswith("_ref_")]
    return contrato.nombres + derivadas + refs + ["_dq_advertencias"]


def preparar_aceptados(etiquetado: DataFrame, contrato: Contrato, cfg: PipelineConfig, ts) -> DataFrame:
    """Selecciona columnas de negocio + linaje y agrega las columnas de la estrategia de historización."""
    cols = columnas_silver(contrato, etiquetado.columns)
    df = etiquetado.where(F.col("_categoria") == "ACEPTADO").select(
        *cols,
        "_source_file", "_source_file_hash", "_row_hash", "_ingestion_ts",
        F.col("_run_id").alias("_bronze_run_id"),
        F.lit(cfg.run_id).alias("_silver_run_id"),
        F.lit(ts).cast("timestamp").alias("_silver_ts"),
    )
    if contrato.historizacion == "scd2":
        df = (
            df.withColumn("_attr_hash", row_hash(contrato.atributos_scd2))
            .withColumn("vigente_desde", F.col(contrato.orden_version[0]))
            .withColumn("vigente_hasta", F.lit(None).cast("date"))
            .withColumn("es_actual", F.lit(True))
        )
    elif contrato.historizacion == "append_only":
        df = df.withColumn("_event_key", row_hash(contrato.clave_negocio))
    return df


def preparar_cuarentena(etiquetado: DataFrame, contrato: Contrato, reglas, cfg: PipelineConfig, ts) -> DataFrame:
    def mapa(attr):
        return F.create_map(*[x for r in reglas for x in (F.lit(r.id), F.lit(getattr(r, attr)))])

    regla = F.element_at("_dq_cuarentena", 1)
    return etiquetado.where(F.col("_categoria") == "CUARENTENA").select(
        F.lit(contrato.dataset).alias("dataset"),
        F.concat_ws("|", *[F.col(k).cast("string") for k in contrato.clave_negocio]).alias("clave_negocio"),
        regla.alias("regla_id"),
        F.col("_dq_fallas").alias("reglas_incumplidas"),
        F.element_at(mapa("severidad"), regla).alias("severidad"),
        F.element_at(mapa("dimension"), regla).alias("dimension"),
        F.element_at(mapa("descripcion"), regla).alias("descripcion"),
        F.to_json("_raw").alias("registro"),
        "_source_file", "_source_file_hash", "_row_hash",
        F.col("_run_id").alias("_bronze_run_id"),
        F.lit(cfg.run_id).alias("run_id"),
        F.lit(cfg.pipeline).alias("pipeline"),
        F.lit(cfg.fecha_corte).cast("date").alias("fecha_corte"),
        F.lit(ts).cast("timestamp").alias("fecha_cuarentena"),
        F.lit(cfg.run_id).alias("ultima_deteccion_run_id"),
        F.lit(ts).cast("timestamp").alias("ultima_deteccion_ts"),
        F.lit("PENDIENTE").alias("estado_gestion"),
    )


def resultados_calidad(
    etiquetado: DataFrame, contrato: Contrato, cal: ConfigCalidad, resumen: dict, cfg: PipelineConfig, ts
) -> list[dict]:
    """Resultados por regla + métricas de unicidad + umbral global de cuarentena (bloqueo)."""
    sin_dup = etiquetado.where(F.col("_categoria") != "DUP_EXACTO")
    evaluados = sum(v["registros"] for k, v in resumen.items() if k != "DUP_EXACTO")
    filas = resumir_reglas(sin_dup, cal.reglas, evaluados)
    ds = contrato.dataset
    total = evaluados + resumen.get("DUP_EXACTO", {}).get("registros", 0)
    sinteticas = [
        (Regla(f"{ds}.UNQ.duplicados_exactos", ds, "unicidad", "Registros idénticos repetidos en el lote (se descartan)",
               "-", "informativa", "marcar", 100.0), resumen.get("DUP_EXACTO", {}).get("registros", 0), total),
        (Regla(f"{ds}.UNQ.clave_negocio", ds, "unicidad",
               f"Versiones anteriores de la misma clave {list(contrato.clave_negocio)} (prevalece la más reciente)",
               "-", "informativa", "marcar", 100.0), resumen.get("DUP_CLAVE", {}).get("registros", 0), evaluados),
        (Regla(f"{ds}.UMBRAL.cuarentena_total", ds, "validez",
               "Porcentaje del lote enviado a cuarentena", "-", "bloqueo", "cuarentena", cal.umbral_cuarentena_pct),
         resumen.get("CUARENTENA", {}).get("registros", 0), evaluados),
    ]
    for regla, n, base in sinteticas:
        pct, res = resultado_regla(regla, n, base)
        filas.append({"regla_id": regla.id, "dimension": regla.dimension, "descripcion": regla.descripcion,
                      "severidad": regla.severidad, "accion": regla.accion, "registros_evaluados": base,
                      "registros_fallidos": n, "pct_fallo": pct, "umbral_pct": regla.umbral_pct, "resultado": res})
    for f in filas:
        f.update({"run_id": cfg.run_id, "pipeline": cfg.pipeline, "fecha_corte": cfg.fecha_corte, "capa": CAPA,
                  "dataset": ds, "fecha_evaluacion": ts})
    return filas


def merge_silver(spark: SparkSession, cfg: PipelineConfig, contrato: Contrato, aceptados: DataFrame) -> None:
    target = cfg.silver(contrato.dataset)
    W.asegurar_tabla(spark, aceptados, target)
    view = f"_src_silver_{contrato.dataset}"
    if contrato.historizacion == "scd2":
        sql = W.sql_merge_scd2(target, view, contrato.clave_negocio, aceptados.columns)
    elif contrato.historizacion == "append_only":
        sql = W.sql_merge_append_only(target, view)
    else:
        sql = W.sql_merge_tipo1(target, view, contrato.clave_negocio, contrato.orden_version)
    W.ejecutar_merge(spark, aceptados, view, sql)


def procesar_silver(spark: SparkSession, cfg: PipelineConfig, contrato: Contrato, logger) -> SilverResult:
    ds = contrato.dataset
    res = SilverResult(dataset=ds)
    ts = utcnow()

    lote, wm = leer_lote_bronze(spark, cfg, ds)
    entrada = medir_entrada(lote, contrato)
    res.registros_entrada = int(entrada["registros"])
    logger.info(f"Lote Bronze: {res.registros_entrada} registros (modo={cfg.modo}, watermark={wm})")
    if res.registros_entrada == 0:
        res.omitido = True
        logger.info("Sin registros nuevos desde el último watermark. Nada que procesar.")
        return res

    cal = cargar_config_calidad(contrato, cfg.fecha_corte)
    enriquecido = enriquecer_referencias(spark, cfg, aplicar_derivaciones(standardize(lote, contrato), ds), contrato)
    etiquetado = clasificar_lote(enriquecido, contrato, cal.reglas)

    resumen = resumen_categorias(etiquetado, contrato)
    res.aceptados = resumen.get("ACEPTADO", {}).get("registros", 0)
    res.cuarentena = resumen.get("CUARENTENA", {}).get("registros", 0)
    res.dup_exactos = resumen.get("DUP_EXACTO", {}).get("registros", 0)
    res.dup_clave = resumen.get("DUP_CLAVE", {}).get("registros", 0)
    logger.info(f"Clasificación: aceptados={res.aceptados} cuarentena={res.cuarentena} "
                f"dup_exactos={res.dup_exactos} dup_clave={res.dup_clave}")

    q_target = cfg.silver("cuarentena")
    W.asegurar_cuarentena(spark, q_target)
    cuarentena_lote = preparar_cuarentena(etiquetado, contrato, cal.reglas, cfg, ts)
    W.ejecutar_merge(spark, cuarentena_lote, f"_src_q_{ds}", W.sql_merge_cuarentena(q_target, f"_src_q_{ds}"))
    res.resultados_calidad = resultados_calidad(etiquetado, contrato, cal, resumen, cfg, ts)
    append_rows(spark, cfg, "calidad_resultados", res.resultados_calidad)
    res.reconciliacion = reconciliar_lote(cfg, contrato, resumen, entrada, ts)

    for r in res.resultados_calidad:
        if r["resultado"] in ("ADVERTENCIA", "FALLA"):
            logger.warning(f"Regla {r['regla_id']} [{r['severidad']}] {r['registros_fallidos']} fallidos "
                           f"({r['pct_fallo']}% > umbral {r['umbral_pct']}%) -> {r['resultado']}")
    try:
        verificar_bloqueos(res.resultados_calidad, ds)
    finally:
        append_rows(spark, cfg, "reconciliacion", res.reconciliacion)

    aceptados = preparar_aceptados(etiquetado, contrato, cfg, ts)
    merge_silver(spark, cfg, contrato, aceptados)

    destinos = reconciliar_destinos(cfg, contrato, aceptados, spark.table(cfg.silver(ds)),
                                    cuarentena_lote, spark.table(q_target), ts)
    append_rows(spark, cfg, "reconciliacion", destinos)
    res.reconciliacion += destinos
    diferencias = [r for r in res.reconciliacion if r["resultado"] != "OK"]
    if diferencias:
        raise RuntimeError(f"[{ds}] Reconciliación con diferencias: "
                           + "; ".join(f"{d['metrica']} {d['valor_origen']} vs {d['valor_destino']}" for d in diferencias))

    max_ts = lote.agg(F.max("_ingestion_ts")).first()[0]
    set_watermark(spark, cfg, ds, CAPA, max_ts)
    logger.info(f"Silver {ds} actualizado. Watermark -> {max_ts}")
    return res
