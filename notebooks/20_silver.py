# Databricks notebook source
# MAGIC %md
# MAGIC # 20 · Silver: estandarización, calidad, cuarentena y conformación
# MAGIC
# MAGIC Procesa `bronze_<dataset>` → `silver_<dataset>`:
# MAGIC
# MAGIC 1. Lee el lote **incremental** (registros ingeridos después del último watermark) o todo Bronze en modo `reproceso`.
# MAGIC 2. Tipa columnas según el contrato (fechas multiformato, decimales).
# MAGIC 3. Conforma claves foráneas (broadcast join contra las tablas Silver padre).
# MAGIC 4. Evalúa reglas de calidad (automáticas del contrato + `conf/quality_rules.yml`).
# MAGIC 5. Clasifica cada registro: `ACEPTADO`, `CUARENTENA`, `DUP_EXACTO`, `DUP_CLAVE`.
# MAGIC 6. Escribe la cuarentena, los resultados de calidad y la reconciliación (**siempre**, como evidencia).
# MAGIC 7. Si una regla de **bloqueo** supera su umbral, falla sin tocar Silver.
# MAGIC 8. Si no, hace un `MERGE` idempotente (tipo 1, SCD2 o append-only) y avanza el watermark.
# MAGIC
# MAGIC **Orden entre datasets** (dependencias de FK): `clientes` → `polizas` → `recaudos`, `siniestros` → `movimientos`.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from src.common.config import DATASETS, config_from_widgets
from src.common.contracts import cargar_contrato
from src.common.control import TaskRun, ensure_control_tables
from src.common.logging_utils import get_logger
from src.transformations.silver import procesar_silver

ORDEN = ["clientes", "polizas", "recaudos", "siniestros", "movimientos"]
assert set(ORDEN) == set(DATASETS)

cfg = config_from_widgets(dbutils, {"dataset": "todos"})
param = dbutils.widgets.get("dataset").strip().lower()
datasets = ORDEN if param in ("", "todos") else [d.strip() for d in param.split(",")]
ensure_control_tables(spark, cfg)

# COMMAND ----------

resumen = []
for ds in datasets:
    log = get_logger(run_id=cfg.run_id, capa="silver", dataset=ds)
    contrato = cargar_contrato(ds, cfg.contracts_dir)
    with TaskRun(spark, cfg, tarea=f"silver_{ds}", capa="silver", dataset=ds, logger=log) as run:
        res = procesar_silver(spark, cfg, contrato, log)
        run.set(registros_entrada=res.registros_entrada, registros_salida=res.aceptados,
                registros_rechazados=res.cuarentena, registros_duplicados=res.dup_exactos + res.dup_clave)
        if res.omitido:
            run.omitido("Sin registros nuevos en Bronze desde el último watermark")
    resumen.append((ds, res.registros_entrada, res.aceptados, res.cuarentena, res.dup_exactos, res.dup_clave))

display(spark.createDataFrame(
    resumen, "dataset STRING, entrada LONG, aceptados LONG, cuarentena LONG, dup_exactos LONG, dup_clave LONG"))

# COMMAND ----------

# MAGIC %md ## Resultados de calidad de esta ejecución (reglas con hallazgos)

# COMMAND ----------

display(spark.table(cfg.ctl("calidad_resultados"))
        .where(f"run_id = '{cfg.run_id}' AND registros_fallidos > 0")
        .select("dataset", "regla_id", "dimension", "severidad", "accion", "registros_fallidos",
                "pct_fallo", "umbral_pct", "resultado")
        .orderBy("dataset", "regla_id"))

# COMMAND ----------

# MAGIC %md ## Reconciliación Bronze -> Silver

# COMMAND ----------

display(spark.table(cfg.ctl("reconciliacion")).where(f"run_id = '{cfg.run_id}'")
        .select("dataset", "etapa_origen", "etapa_destino", "metrica", "valor_origen", "valor_destino",
                "diferencia", "resultado", "detalle"))

# COMMAND ----------

# MAGIC %md ## Cuarentena por dataset y regla

# COMMAND ----------

display(spark.sql(f"""
  SELECT dataset, regla_id, severidad, count(*) AS registros, min(fecha_cuarentena) AS primera_deteccion
  FROM {cfg.silver('cuarentena')} GROUP BY ALL ORDER BY dataset, registros DESC
"""))