# Databricks notebook source
# MAGIC %md
# MAGIC # 30 · Gold: productos de datos
# MAGIC
# MAGIC Publica, para la `fecha_corte` indicada, los productos orientados a casos de uso:
# MAGIC
# MAGIC | Producto | Grano | Uso |
# MAGIC |---|---|---|
# MAGIC | `gold_poliza_360` | 1 fila por póliza y corte | Estado integral de la póliza: cliente, vigencia, prima, recaudo, siniestros, última actividad |
# MAGIC | `gold_siniestralidad_mensual` | 1 fila por mes, producto, sucursal y corte | Exposición, prima emitida/devengada/recaudada, costo incurrido y razón de siniestralidad |
# MAGIC | `gold_discrepancias` | 1 fila por hallazgo y corte | Pagos sin correspondencia, siniestros sin póliza válida, movimientos fuera de vigencia |
# MAGIC | `gold_calidad_pipeline` | 1 fila por control, dataset y ejecución | Resultados de reglas de calidad y reconciliaciones |
# MAGIC
# MAGIC Escritura idempotente por snapshot (`replaceWhere fecha_corte`): reprocesar un corte lo reemplaza
# MAGIC sin duplicar y conserva los cortes anteriores. Las definiciones están en `docs/productos_gold.md`.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from src.common.config import config_from_widgets
from src.common.control import TaskRun, _schema_ddl, ensure_control_tables
from src.common.logging_utils import get_logger
from src.transformations.gold_publish import publicar_calidad, publicar_productos, verificar_reconciliacion

cfg = config_from_widgets(dbutils)
ensure_control_tables(spark, cfg)
log = get_logger(run_id=cfg.run_id, capa="gold", dataset="productos")
log.info(f"Publicando Gold para fecha_corte={cfg.fecha_corte_str}")

# COMMAND ----------

# MAGIC %md ## 1. Productos de negocio + reconciliación Silver -> Gold

# COMMAND ----------

with TaskRun(spark, cfg, tarea="gold_productos", capa="gold", dataset="productos", logger=log) as run:
    res = publicar_productos(spark, cfg, log)
    run.set(registros_salida=res.filas["gold_poliza_360"])
    run.add_detalle(**res.filas)

display(spark.createDataFrame(list(res.filas.items()), "producto STRING, filas LONG"))
display(spark.createDataFrame(res.reconciliacion, schema=_schema_ddl("reconciliacion")).select(
    "dataset", "etapa_origen", "etapa_destino", "metrica", "valor_origen", "valor_destino", "diferencia", "resultado"))

# COMMAND ----------

# MAGIC %md ## 2. Producto de calidad del pipeline
# MAGIC Se publica **antes** de evaluar la reconciliación, para que una diferencia quede visible en el producto de calidad.
# MAGIC Falla la tarea si Silver y Gold no cuadran.

# COMMAND ----------

log_q = get_logger(run_id=cfg.run_id, capa="gold", dataset="calidad_pipeline")
with TaskRun(spark, cfg, tarea="gold_calidad_pipeline", capa="gold", dataset="calidad_pipeline", logger=log_q) as run:
    run.set(registros_salida=publicar_calidad(spark, cfg, log_q))

verificar_reconciliacion(res.reconciliacion)

# COMMAND ----------

# MAGIC %md ## 3. Vista rápida

# COMMAND ----------

display(spark.table(cfg.gold("poliza_360")).where(f"fecha_corte = DATE'{cfg.fecha_corte_str}'")
        .groupBy("estado_al_corte").count().orderBy("estado_al_corte"))