# Databricks notebook source
# MAGIC %md
# MAGIC # 31 · Preguntas de negocio y trazabilidad
# MAGIC
# MAGIC Responde las preguntas de la sección 4.1 de la prueba sobre los productos Gold, y demuestra la
# MAGIC trazabilidad de un registro desde Gold hasta el archivo fuente y la ejecución que lo procesó.
# MAGIC
# MAGIC Parámetros: `fecha_corte` (vacío = último día del mes anterior es decir 2026-09-30,) y `poliza_trazabilidad`.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.common.config import config_from_widgets

cfg = config_from_widgets(dbutils, {"poliza_trazabilidad": "POL0000002"})
POLIZA = dbutils.widgets.get("poliza_trazabilidad").strip()
C = f"DATE'{cfg.fecha_corte_str}'"
T = cfg.table
displayHTML(f"<b>fecha_corte:</b> {cfg.fecha_corte_str} &nbsp; <b>póliza de trazabilidad:</b> {POLIZA}")

# COMMAND ----------

# MAGIC %md ## 1. ¿Cuáles pólizas están vigentes, vencidas, canceladas o próximas a vencer en la fecha de corte?

# COMMAND ----------

display(spark.sql(f"""
  SELECT estado_al_corte, count(*) AS polizas, sum(prima_emitida) AS prima_emitida
  FROM {T('gold_poliza_360')} WHERE fecha_corte = {C}
  GROUP BY estado_al_corte ORDER BY polizas DESC
"""))

# COMMAND ----------

# MAGIC %md Detalle de las pólizas próximas a vencer (ventana de renovación):

# COMMAND ----------

display(spark.sql(f"""
  SELECT poliza_id, producto, sucursal, segmento_cliente, fecha_fin_vigencia, dias_para_vencer, prima_emitida,
         prima_pendiente
  FROM {T('gold_poliza_360')}
  WHERE fecha_corte = {C} AND estado_al_corte = 'Proxima a vencer'
  ORDER BY dias_para_vencer
"""))

# COMMAND ----------

# MAGIC %md ## 2. ¿Cuál es la prima emitida, recaudada y pendiente por producto, sucursal y mes?

# COMMAND ----------

display(spark.sql(f"""
  SELECT periodo, producto, sucursal, prima_emitida, prima_recaudada,
         prima_emitida - prima_recaudada AS diferencia_mes, prima_pendiente_acumulada
  FROM {T('gold_siniestralidad_mensual')} WHERE fecha_corte = {C}
  ORDER BY periodo DESC, producto, sucursal
"""))

# COMMAND ----------

# MAGIC %md ## 3. ¿Cuál es el costo incurrido y la razón de siniestralidad mensual?

# COMMAND ----------

display(spark.sql(f"""
  SELECT periodo,
         sum(polizas_expuestas)  AS polizas_expuestas,
         sum(prima_devengada)    AS prima_devengada,
         sum(siniestros)         AS siniestros,
         sum(costo_incurrido)    AS costo_incurrido,
         round(try_divide(sum(costo_incurrido), sum(prima_devengada)), 4) AS razon_siniestralidad
  FROM {T('gold_siniestralidad_mensual')} WHERE fecha_corte = {C}
  GROUP BY periodo ORDER BY periodo
"""))

# COMMAND ----------

# MAGIC %md ## 4. ¿Qué pólizas presentan pagos sin correspondencia, siniestros sin póliza válida o movimientos fuera de vigencia?

# COMMAND ----------

display(spark.sql(f"""
  SELECT tipo_discrepancia, severidad, count(*) AS hallazgos, count(DISTINCT poliza_id) AS polizas,
         sum(valor) AS valor_involucrado
  FROM {T('gold_discrepancias')} WHERE fecha_corte = {C}
  GROUP BY tipo_discrepancia, severidad ORDER BY hallazgos DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT * FROM {T('gold_discrepancias')}
  WHERE fecha_corte = {C} AND tipo_discrepancia IN ('PAGO_SIN_POLIZA', 'SINIESTRO_SIN_POLIZA')
  ORDER BY tipo_discrepancia, poliza_id
"""))

# COMMAND ----------

# MAGIC %md ## 5. ¿Qué reglas de calidad se incumplieron y qué registros quedaron en cuarentena?

# COMMAND ----------

display(spark.sql(f"""
  SELECT dataset, control_id, dimension, severidad, accion, registros_fallidos, pct_fallo, umbral_pct, resultado
  FROM {T('gold_calidad_pipeline')}
  WHERE es_ultima_ejecucion AND tipo_control = 'REGLA' AND registros_fallidos > 0
  ORDER BY dataset, registros_fallidos DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT dataset, regla_id, clave_negocio, reglas_incumplidas, registro, _source_file, run_id, fecha_cuarentena,
         estado_gestion
  FROM {T('silver_cuarentena')} ORDER BY dataset, regla_id
"""))

# COMMAND ----------

# MAGIC %md ## 6. ¿Cuál fue la última ejecución exitosa y qué datos fueron procesados?

# COMMAND ----------

display(spark.sql(f"""
  WITH ultima AS (
    SELECT run_id, max(fin) AS fin
    FROM {T('ctl_ejecuciones')}
    GROUP BY run_id
    HAVING count_if(estado = 'FALLIDO') = 0 AND count_if(tarea = 'gold_productos' AND estado = 'EXITOSO') > 0
    ORDER BY fin DESC LIMIT 1
  )
  SELECT e.run_id, e.tarea, e.dataset, e.estado, e.fecha_corte, e.modo, e.inicio, e.fin, e.duracion_seg,
         e.registros_entrada, e.registros_salida, e.registros_rechazados, e.registros_duplicados
  FROM {T('ctl_ejecuciones')} e JOIN ultima u USING (run_id)
  ORDER BY e.inicio
"""))

# COMMAND ----------

# MAGIC %md Archivos fuente ingeridos (todas las ejecuciones):

# COMMAND ----------

display(spark.sql(f"""
  SELECT dataset, archivo, estado, registros, hash_archivo, run_id, fecha_registro
  FROM {T('ctl_archivos_procesados')} ORDER BY fecha_registro DESC
"""))

# COMMAND ----------

# MAGIC %md ## 7. Trazabilidad de un registro: Gold → Silver → Bronze → archivo → ejecución

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'gold_poliza_360' AS capa, g.poliza_id, g.estado_al_corte AS detalle, g._silver_run_id_poliza AS run_id,
         NULL AS archivo, NULL AS hash_archivo
  FROM {T('gold_poliza_360')} g WHERE g.fecha_corte = {C} AND g.poliza_id = '{POLIZA}'
  UNION ALL
  SELECT 'silver_polizas', s.poliza_id, concat('prima=', s.prima, ' actualizado=', s.fecha_actualizacion),
         s._silver_run_id, s._source_file, s._source_file_hash
  FROM {T('silver_polizas')} s WHERE s.poliza_id = '{POLIZA}'
  UNION ALL
  SELECT 'bronze_polizas (todas las versiones recibidas)', b.poliza_id,
         concat('prima=', b.prima, ' actualizado=', b.fecha_actualizacion), b._run_id, b._source_file, b._source_file_hash
  FROM {T('bronze_polizas')} b WHERE b.poliza_id = '{POLIZA}'
"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT a.dataset, a.archivo, a.ruta, a.hash_archivo, a.registros, a.estado, a.run_id,
         e.tarea, e.estado AS estado_tarea, e.inicio, e.fin, e.usuario
  FROM {T('ctl_archivos_procesados')} a
  JOIN {T('silver_polizas')} s ON s._source_file_hash = a.hash_archivo AND s.poliza_id = '{POLIZA}'
  LEFT JOIN {T('ctl_ejecuciones')} e ON e.run_id = a.run_id AND e.tarea = 'bronze_polizas'
  WHERE a.estado = 'INGESTADO'
"""))