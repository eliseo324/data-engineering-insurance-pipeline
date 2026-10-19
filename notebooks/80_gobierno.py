# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 80 · Gobierno, seguridad y privacidad en Unity Catalog
# MAGIC
# MAGIC Aplica de forma **idempotente** el modelo de gobierno declarado en `conf/gobierno.yml` y en los contratos:
# MAGIC
# MAGIC 1. **Documentación y clasificación:** comentarios de tablas y columnas y etiquetas (capa, dominio, owner, `pii`, `clasificacion`).
# MAGIC 2. **Protección de datos personales:** funciones `fn_mask_*`, vistas protegidas y, opcionalmente, column masks nativas.
# MAGIC 3. **Retención de versiones** en tablas con datos personales (time travel limitado).
# MAGIC 4. **Grupos y permisos por rol** con mínimo privilegio, como estado deseado.
# MAGIC
# MAGIC `modo_gobierno = plan` muestra las sentencias sin ejecutarlas (útil para revisión); `aplicar` las ejecuta.
# MAGIC Corre como tarea `gobierno` del job después de la publicación.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from src.common.config import config_from_widgets
from src.common.control import TaskRun
from src.common.logging_utils import get_logger
from src.governance.aplicar import asegurar_grupos, ejecutar_plan
from src.governance.comments import plan_documentacion
from src.governance.grants import cargar_config_gobierno, plan_permisos
from src.governance.masking import plan_proteccion
from src.governance.privacy import plan_retencion

cfg = config_from_widgets(dbutils, {"gestionar_grupos": "true"})
dbutils.widgets.dropdown("modo_gobierno", "aplicar", ["aplicar", "plan"])
dbutils.widgets.dropdown("aplicar_column_masks", "false", ["false", "true"])
modo = dbutils.widgets.get("modo_gobierno")
column_masks = dbutils.widgets.get("aplicar_column_masks") == "true"
gestionar_grupos = dbutils.widgets.get("gestionar_grupos").lower() == "true"
log = get_logger(run_id=cfg.run_id, capa="gobierno", dataset="unity_catalog")
conf = cargar_config_gobierno()
T = cfg.table

# COMMAND ----------

# MAGIC %md
# MAGIC Documentación, protección de PII y retención.
# MAGIC
# MAGIC Grupos (SCIM) y permisos por rol.
# MAGIC
# MAGIC Incluye las vistas y funciones recién creadas.

# COMMAND ----------

def objetos_existentes():
    filas = spark.sql(f"SHOW TABLES IN {cfg.catalog}.{cfg.schema}").where("NOT isTemporary").collect()
    tablas = [r["tableName"] for r in filas]
    funciones = [r[0].split(".")[-1] for r in spark.sql(f"SHOW USER FUNCTIONS IN {cfg.catalog}.{cfg.schema}").collect()]
    return tablas, funciones


resultados = []
with TaskRun(spark, cfg, tarea="gobierno", capa="gobierno", dataset="unity_catalog", logger=log) as run:
    tablas, _ = objetos_existentes()
    columnas = {t: spark.table(T(t)).columns for t in tablas if not t.startswith("vw_")}

    resultados += ejecutar_plan(spark, plan_documentacion(T, columnas), modo, log)
    resultados += ejecutar_plan(spark, plan_proteccion(T, conf["grupo_pii"], column_masks), modo, log)
    resultados += ejecutar_plan(spark, plan_retencion(T, conf["retencion_pii"]), modo, log)

    if gestionar_grupos and modo == "aplicar":
        from databricks.sdk import WorkspaceClient
        resultados += asegurar_grupos(WorkspaceClient(), conf, log)
    tablas, funciones = objetos_existentes() 
    resultados += ejecutar_plan(
        spark, plan_permisos(conf, cfg.catalog, cfg.schema, cfg.volume, tablas, funciones, T), modo, log)

    errores = [r for r in resultados if r["estado"] == "ERROR"]
    run.set(registros_entrada=len(resultados), registros_rechazados=len(errores))
    run.add_detalle(modo=modo, ok=sum(r["estado"] == "OK" for r in resultados), errores=len(errores))

display(spark.createDataFrame(resultados, "accion STRING, estado STRING, detalle STRING").orderBy("estado", "accion"))

# COMMAND ----------

# MAGIC %md ## Verificación

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {cfg.catalog}.{cfg.schema}"))

# COMMAND ----------

display(spark.sql(f"""
  SELECT table_name, column_name, tag_name, tag_value
  FROM {cfg.catalog}.information_schema.column_tags
  WHERE schema_name = '{cfg.schema}' ORDER BY table_name, column_name
"""))

# COMMAND ----------

# MAGIC %md Vista protegida (quien no pertenece a `pii_lectores` ve los datos enmascarados):

# COMMAND ----------

display(spark.sql(f"""
  SELECT cliente_id, numero_documento, correo, telefono, fecha_nacimiento, segmento
  FROM {T('vw_clientes_protegida')} WHERE es_actual LIMIT 5
"""))