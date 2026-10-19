# Databricks notebook source
# MAGIC %md
# MAGIC # 92 · Rollback de productos Gold (time travel de Delta)
# MAGIC
# MAGIC Restaura los productos Gold a las versiones Delta registradas en una publicación anterior
# MAGIC (`ctl_publicaciones.versiones_delta`).
# MAGIC
# MAGIC * `run_id_publicacion`: ejecución cuya publicación se quiere recuperar. Vacío = la penúltima publicación.
# MAGIC * `confirmar = no` (por defecto) muestra el plan sin ejecutarlo.
# MAGIC
# MAGIC `RESTORE` crea una versión nueva: queda auditado en `DESCRIBE HISTORY` y también es reversible.
# MAGIC El rollback de **código** se hace redesplegando el tag anterior desde el workflow de CD (ver `docs/cicd.md`).

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from pyspark.sql import functions as F

from src.common.config import config_from_widgets
from src.common.control import TaskRun
from src.common.logging_utils import get_logger
from src.governance.aplicar import ejecutar_plan
from src.orchestration.rollback import plan_rollback

cfg = config_from_widgets(dbutils, {"run_id_publicacion": ""})
dbutils.widgets.dropdown("confirmar", "no", ["no", "si"])
objetivo = dbutils.widgets.get("run_id_publicacion").strip()
modo = "aplicar" if dbutils.widgets.get("confirmar") == "si" else "plan"
log = get_logger(run_id=cfg.run_id, capa="rollback", dataset="gold")

publicaciones = spark.table(cfg.ctl("publicaciones")).orderBy(F.col("ts_publicacion").desc())
display(publicaciones.select("run_id", "fecha_corte", "ts_publicacion", "modo", "tipo_disparo", "versiones_delta").limit(10))

# COMMAND ----------

with TaskRun(spark, cfg, tarea="rollback_gold", capa="rollback", dataset="gold", logger=log) as run:
    filas = (publicaciones.where(F.col("run_id") == objetivo) if objetivo else publicaciones).limit(2).collect()
    if not filas or (not objetivo and len(filas) < 2):
        raise ValueError("No hay una publicación anterior a la cual volver")
    pub = filas[0] if objetivo else filas[1]
    log.info(f"Rollback a la publicación {pub['run_id']} (corte {pub['fecha_corte']}): {pub['versiones_delta']}")
    resultados = ejecutar_plan(spark, plan_rollback(cfg.table, pub["versiones_delta"]), modo, log)
    run.add_detalle(modo=modo, publicacion_restaurada=pub["run_id"], versiones=pub["versiones_delta"])
    if modo == "plan":
        run.omitido("Plan mostrado sin ejecutar (confirmar = no)")

display(spark.createDataFrame(resultados, "accion STRING, estado STRING, detalle STRING"))
