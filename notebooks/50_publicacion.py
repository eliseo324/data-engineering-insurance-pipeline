# Databricks notebook source
# MAGIC %md
# MAGIC # 50 · Publicación
# MAGIC
# MAGIC Última tarea del workflow; solo corre si las validaciones pasaron.
# MAGIC
# MAGIC 1. Reconstruye `gold_calidad_pipeline`, que ya incluye las postcondiciones de esta ejecución.
# MAGIC 2. Registra en `ctl_publicaciones` la versión Delta de cada producto (auditoría y rollback por
# MAGIC    time travel), la frescura de los datos y el cumplimiento del SLA (día 5, 08:00 Colombia;
# MAGIC    solo aplica a ejecuciones programadas).

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from src.common.config import config_from_widgets
from src.common.control import TaskRun, ensure_control_tables
from src.common.logging_utils import get_logger
from src.orchestration.publicacion import registrar_publicacion
from src.transformations.gold_publish import publicar_calidad

cfg = config_from_widgets(dbutils, {"tipo_disparo": ""})
tipo_disparo = dbutils.widgets.get("tipo_disparo").strip() or "MANUAL"
if "{{" in tipo_disparo:  
    tipo_disparo = "MANUAL"
ensure_control_tables(spark, cfg)
log = get_logger(run_id=cfg.run_id, capa="publicacion", dataset="gold")

# COMMAND ----------

with TaskRun(spark, cfg, tarea="publicacion", capa="publicacion", dataset="gold", logger=log) as run:
    run.set(registros_salida=publicar_calidad(spark, cfg, log))
    fila = registrar_publicacion(spark, cfg, tipo_disparo, log)
    run.add_detalle(versiones_delta=fila["versiones_delta"], frescura_dias=fila["frescura_dias"],
                    cumple_sla=fila["cumple_sla"])

display(spark.table(cfg.ctl("publicaciones")).orderBy("ts_publicacion", ascending=False).limit(5))