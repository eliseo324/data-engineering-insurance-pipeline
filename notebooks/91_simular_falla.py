# Databricks notebook source
# MAGIC %md
# MAGIC # 91 · Simulación de fallas y recuperación
# MAGIC
# MAGIC Genera evidencia del **manejo de errores, cuarentena y recuperación** del workflow.
# MAGIC
# MAGIC | Escenario | Qué se inyecta | Qué debe pasar |
# MAGIC |---|---|---|
# MAGIC | `calidad` | Archivo de pólizas con 20 % de filas sin `poliza_id` | Bronze lo ingiere; `silver_polizas` escribe cuarentena y resultados, **falla por bloqueo** y no modifica Silver |
# MAGIC | `esquema` | Archivo de pólizas sin la columna requerida `prima` | `bronze_polizas` lo **rechaza** (`RECHAZADO_ESQUEMA`) y falla; nada se escribe en Bronze |
# MAGIC
# MAGIC **Procedimiento**
# MAGIC 1. `accion = generar` con el escenario elegido.
# MAGIC 2. Ejecutar el job (*Run now with different parameters*, `fecha_corte = 2026-08-31`). Debe fallar en la tarea indicada.
# MAGIC 3. Evidencia: ejecución fallida y su error, `ctl_ejecuciones`, `silver_cuarentena`, `ctl_calidad_resultados`.
# MAGIC 4. `accion = limpiar`. Retira el archivo y revierte sus efectos.
# MAGIC 5. En la ejecución fallida, **Repair run**: se reejecutan solo las tareas fallidas y sus dependientes, y el job termina bien.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from src.common.config import config_from_widgets
from src.common.control import ensure_control_tables
from src.common.logging_utils import get_logger
from src.orchestration.simulacion_fallas import (
    ESCENARIOS,
    archivo_origen,
    generar_claves_nulas,
    generar_esquema_invalido,
    limpiar_simulacion,
)

dbutils.widgets.dropdown("escenario", "calidad", list(ESCENARIOS))
dbutils.widgets.dropdown("accion", "generar", ["generar", "limpiar"])
cfg = config_from_widgets(dbutils)
ensure_control_tables(spark, cfg)
escenario, accion = dbutils.widgets.get("escenario"), dbutils.widgets.get("accion")
log = get_logger(run_id=cfg.run_id, capa="simulacion", dataset="polizas")
landing = cfg.landing_path("polizas")
nombre = ESCENARIOS[escenario]

# COMMAND ----------

if accion == "generar":
    origen, destino = archivo_origen(landing), os.path.join(landing, nombre)
    n = (generar_claves_nulas(origen, destino) if escenario == "calidad" else generar_esquema_invalido(origen, destino))
    log.info(f"Escenario '{escenario}': {n} filas escritas en {destino}. Ahora ejecute el job.")
    displayHTML(f"<p>Archivo <code>{destino}</code> generado ({n} filas). Ejecute el job con "
                f"<b>fecha_corte = 2026-08-31</b>: debe fallar en "
                f"<b>{'silver_polizas' if escenario == 'calidad' else 'bronze_polizas'}</b>.</p>")
else:
    resumen = limpiar_simulacion(spark, cfg, "polizas", nombre, log)
    displayHTML(f"<p>Limpieza: {resumen}. Ahora use <b>Repair run</b> en la ejecución fallida.</p>")

# COMMAND ----------

# MAGIC %md ## Evidencia rápida

# COMMAND ----------

display(spark.sql(f"""
  SELECT run_id, tarea, estado, inicio, duracion_seg, mensaje FROM {cfg.ctl('ejecuciones')}
  WHERE estado = 'FALLIDO' ORDER BY inicio DESC LIMIT 10
"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT dataset, archivo, estado, registros, mensaje, fecha_registro FROM {cfg.ctl('archivos_procesados')}
  WHERE archivo LIKE '%SIMULACION%' ORDER BY fecha_registro DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT regla_id, estado_gestion, count(*) AS registros FROM {cfg.silver('cuarentena')}
  WHERE _source_file LIKE '%SIMULACION%' GROUP BY ALL
"""))
