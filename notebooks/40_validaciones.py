# Databricks notebook source
# MAGIC %md
# MAGIC # 40 · Validaciones (postcondiciones sobre Silver y Gold)
# MAGIC
# MAGIC Pruebas de integración que se ejecutan **sobre las tablas publicadas** en cada corrida del job:
# MAGIC unicidad de la granulidad, completitud Silver -> Gold, coherencia de métricas, historia SCD2 sin solapes y
# MAGIC frescura según el SLA de cada contrato.
# MAGIC
# MAGIC * Severidad **bloqueo** incumplida -. la tarea falla y el job no publica.
# MAGIC * Severidad **advertencia** -> se registra y se alerta en el tablero de observabilidad.
# MAGIC
# MAGIC Los resultados quedan en `ctl_calidad_resultados` con `capa = 'postcondicion'`.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from src.common.config import config_from_widgets
from src.common.control import TaskRun, _schema_ddl, append_rows, ensure_control_tables
from src.common.logging_utils import get_logger
from src.quality.postconditions import ejecutar_postcondiciones, postcondiciones

cfg = config_from_widgets(dbutils)
ensure_control_tables(spark, cfg)
log = get_logger(run_id=cfg.run_id, capa="validacion", dataset="silver_gold")

# COMMAND ----------

with TaskRun(spark, cfg, tarea="validaciones", capa="validacion", dataset="silver_gold", logger=log) as run:
    checks = postcondiciones(cfg.table, cfg.fecha_corte)
    resultados = ejecutar_postcondiciones(spark, checks, cfg.run_id, cfg.pipeline, cfg.fecha_corte, log)
    append_rows(spark, cfg, "calidad_resultados", resultados)
    fallas = [r for r in resultados if r["resultado"] == "FALLA"]
    run.set(registros_entrada=len(resultados), registros_rechazados=len(fallas))
    display(spark.createDataFrame(resultados, schema=_schema_ddl("calidad_resultados")).select(
        "dataset", "regla_id", "dimension", "severidad", "registros_fallidos", "resultado").orderBy("resultado", "dataset"))
    if fallas:
        raise AssertionError(f"{len(fallas)} postcondición(es) de bloqueo incumplida(s): "
                             f"{[f['regla_id'] for f in fallas]}. No se publica el corte {cfg.fecha_corte_str}.")