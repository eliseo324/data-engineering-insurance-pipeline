# Databricks notebook source
# MAGIC %md
# MAGIC # 60 · Observabilidad: tablero de control y alertas
# MAGIC
# MAGIC Se ejecuta como última tarea del job con `run_if: ALL_DONE`: corre **aunque una tarea anterior
# MAGIC haya fallado**, para que el tablero y las alertas reflejen también las ejecuciones fallidas.
# MAGIC
# MAGIC 1. Publica (o actualiza) las vistas `vw_obs_*` en Unity Catalog. Son la base del tablero AI/BI.
# MAGIC 2. Evalúa las alertas (`conf/observabilidad.yml`) y registra las activas en `ctl_alertas`.
# MAGIC 3. Muestra el tablero de control (también sirve como respaldo si no se ha creado el tablero AI/BI).

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
from src.observability.alerts import evaluar_alertas
from src.observability.views import cargar_config_observabilidad, crear_vistas

cfg = config_from_widgets(dbutils)
ensure_control_tables(spark, cfg)
log = get_logger(run_id=cfg.run_id, capa="observabilidad", dataset="pipeline")
T = cfg.table

# COMMAND ----------

with TaskRun(spark, cfg, tarea="observabilidad", capa="observabilidad", dataset="pipeline", logger=log) as run:
    creadas = crear_vistas(spark, cfg, log)
    alertas = evaluar_alertas(spark, cfg, cargar_config_observabilidad(), log)
    run.set(registros_salida=len(alertas))
    run.add_detalle(vistas=creadas, alertas=[a["alerta_id"] for a in alertas])

# COMMAND ----------

# MAGIC %md ## 1. Estado de las ejecuciones

# COMMAND ----------

display(spark.sql(f"""
  SELECT run_id, fecha_corte, modo, tipo_disparo, estado_ejecucion, inicio, duracion_seg, tareas, tareas_fallidas,
         tareas_omitidas, cumple_sla, ultimo_error
  FROM {T('vw_obs_ejecuciones')} ORDER BY inicio DESC LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md ## 2. Duración por tarea (última ejecución)

# COMMAND ----------

display(spark.sql(f"""
  SELECT tarea, estado, duracion_seg FROM {T('vw_obs_tareas')}
  WHERE run_id = (SELECT max_by(run_id, inicio) FROM {T('vw_obs_ejecuciones')}) ORDER BY inicio
"""))

# COMMAND ----------

# MAGIC %md ## 3. Volúmenes: entrada, salida, rechazados y duplicados

# COMMAND ----------

display(spark.sql(f"""
  SELECT run_id, capa, dataset, entrada, salida, rechazados, duplicados, pct_rechazo, pct_duplicados
  FROM {T('vw_obs_volumenes')} ORDER BY inicio DESC, capa, dataset
"""))

# COMMAND ----------

# MAGIC %md ## 4. Calidad por ejecución

# COMMAND ----------

display(spark.sql(f"""
  SELECT run_id, capa, dataset, controles, ok, informativas, advertencias, fallas
  FROM {T('vw_obs_calidad')} ORDER BY fecha_evaluacion DESC, capa, dataset
"""))

# COMMAND ----------

# MAGIC %md ## 5. Frescura y SLA

# COMMAND ----------

display(spark.table(T("vw_obs_frescura")))
display(spark.sql(f"SELECT * FROM {T('vw_obs_sla')} ORDER BY ts_publicacion DESC"))

# COMMAND ----------

# MAGIC %md ## 6. Evolución del esquema y consumo estimado

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {T('vw_obs_esquemas')} ORDER BY fecha_registro DESC"))
display(spark.sql(f"SELECT * FROM {T('vw_obs_consumo')} ORDER BY inicio DESC"))

# COMMAND ----------

# MAGIC %md ## 7. Cuarentena y alertas activas

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {T('vw_obs_cuarentena')} ORDER BY registros DESC"))
display(spark.sql(f"""
  SELECT alerta_id, severidad, nombre, responsable, detalle, fecha_evaluacion
  FROM {cfg.ctl('alertas')} WHERE run_id = '{cfg.run_id}' ORDER BY severidad, alerta_id
"""))
