from __future__ import annotations

from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.contracts import Contrato


def resumen_categorias(etiquetado: DataFrame, contrato: Contrato) -> dict:
    """Una sola agregación: conteo, claves distintas y totales monetarios por categoría."""
    aggs = [F.count(F.lit(1)).alias("registros")]
    aggs += [F.sum(F.col(m)).alias(f"suma_{m}") for m in contrato.columnas_monetarias]
    filas = etiquetado.groupBy("_categoria").agg(*aggs).collect()
    return {r["_categoria"]: r.asDict() for r in filas}


def _fila(cfg, dataset, origen, destino, metrica, v_origen, v_destino, detalle=None, tolerancia=0) -> dict:
    vo = Decimal(str(v_origen or 0))
    vd = Decimal(str(v_destino or 0))
    dif = vo - vd
    return {
        "run_id": cfg.run_id, "pipeline": cfg.pipeline, "fecha_corte": cfg.fecha_corte, "dataset": dataset,
        "etapa_origen": origen, "etapa_destino": destino, "metrica": metrica,
        "valor_origen": vo, "valor_destino": vd, "diferencia": dif, "tolerancia": Decimal(str(tolerancia)),
        "resultado": "OK" if abs(dif) <= Decimal(str(tolerancia)) else "DIFERENCIA",
        "detalle": detalle,
    }


def medir_entrada(bronze_lote: DataFrame, contrato: Contrato) -> dict:
    """Mide el lote directamente en Bronze, ANTES de estandarizar y enriquecer.

    Se mide por separado para que la reconciliación detecte filas perdidas o multiplicadas
    (p. ej. un join contra una dimensión con claves repetidas).
    """
    aggs = [F.count(F.lit(1)).alias("registros")]
    aggs += [
        F.sum(F.expr(f"try_cast(trim(`{m}`) AS {contrato.columna(m).tipo})")).alias(f"suma_{m}")
        for m in contrato.columnas_monetarias
    ]
    return bronze_lote.agg(*aggs).first().asDict()


def reconciliar_lote(cfg, contrato: Contrato, resumen: dict, entrada: dict, ts) -> list[dict]:
    """Control 1: lo que entró desde Bronze = suma de las categorías clasificadas en Silver."""
    cats = ("ACEPTADO", "CUARENTENA", "DUP_CLAVE", "DUP_EXACTO")
    detalle = ", ".join(f"{k}={resumen.get(k, {}).get('registros', 0)}" for k in cats)
    filas = [_fila(cfg, contrato.dataset, "bronze_lote", "silver_clasificado", "conteo_registros",
                   entrada["registros"], sum(resumen.get(k, {}).get("registros", 0) for k in cats), detalle)]
    for m in contrato.columnas_monetarias:
        col = f"suma_{m}"
        partes = sum(Decimal(str(resumen.get(k, {}).get(col) or 0)) for k in cats)
        filas.append(_fila(cfg, contrato.dataset, "bronze_lote", "silver_clasificado", f"total_{m}",
                           entrada.get(col), partes))
    for f in filas:
        f["fecha_evaluacion"] = ts
    return filas


def reconciliar_destinos(
    cfg, contrato: Contrato, aceptados: DataFrame, silver: DataFrame, cuarentena_lote: DataFrame,
    cuarentena_tabla: DataFrame, ts,
) -> list[dict]:
    """Controles 2 y 3: lo aceptado está en Silver y lo rechazado en la cuarentena."""
    clave = list(contrato.clave_negocio) if contrato.historizacion != "append_only" else ["_event_key"]
    n_acept = aceptados.select(*clave).distinct().count()
    faltantes = aceptados.select(*clave).distinct().join(silver.select(*clave).distinct(), clave, "left_anti").count()
    n_q = cuarentena_lote.select("_row_hash", "_source_file_hash").distinct().count()
    faltan_q = (
        cuarentena_lote.select("_row_hash", "_source_file_hash").distinct()
        .join(cuarentena_tabla.where(F.col("dataset") == contrato.dataset)
              .select("_row_hash", "_source_file_hash"), ["_row_hash", "_source_file_hash"], "left_anti")
        .count()
    )
    filas = [
        _fila(cfg, contrato.dataset, "silver_aceptados_lote", "silver_tabla", "claves_negocio",
              n_acept, n_acept - faltantes, f"claves aceptadas ausentes en Silver: {faltantes}"),
        _fila(cfg, contrato.dataset, "silver_cuarentena_lote", "silver_cuarentena", "registros_rechazados",
              n_q, n_q - faltan_q, f"rechazados no trazados: {faltan_q}"),
    ]
    for f in filas:
        f["fecha_evaluacion"] = ts
    return filas
