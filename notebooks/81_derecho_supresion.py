# Databricks notebook source
# MAGIC %md
# MAGIC # 81 · Derecho de supresión de datos personales (Ley 1581 de 2012)
# MAGIC
# MAGIC Atiende la solicitud de un titular: **suprime sus atributos personales** (documento, fecha de
# MAGIC nacimiento, correo y teléfono) en Bronze, Silver y la cuarentena, y **conserva la clave seudónima**
# MAGIC `cliente_id`, para no romper la integridad de pólizas y siniestros que deben conservarse por
# MAGIC obligación regulatoria.
# MAGIC
# MAGIC * `confirmar = no` (por defecto) muestra el plan sin ejecutarlo.
# MAGIC * La eliminación física ocurre con `VACUUM`, una vez vencida la retención de versiones (7 días en tablas con datos personales).
# MAGIC * La solicitud queda registrada solo con el hash del identificador.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

import hashlib

from src.common.config import config_from_widgets
from src.common.control import TaskRun
from src.common.logging_utils import get_logger
from src.governance.aplicar import ejecutar_plan
from src.governance.privacy import plan_supresion, plan_vacuum

cfg = config_from_widgets(dbutils, {"cliente_id": ""})
dbutils.widgets.dropdown("confirmar", "no", ["no", "si"])
dbutils.widgets.dropdown("ejecutar_vacuum", "no", ["no", "si"])
cliente_id = dbutils.widgets.get("cliente_id").strip()
modo = "aplicar" if dbutils.widgets.get("confirmar") == "si" else "plan"
huella = hashlib.sha256(cliente_id.encode()).hexdigest()[:16]
log = get_logger(run_id=cfg.run_id, capa="privacidad", dataset=f"titular:{huella}")

# COMMAND ----------

with TaskRun(spark, cfg, tarea="derecho_supresion", capa="privacidad", dataset=f"titular:{huella}", logger=log) as run:
    plan = plan_supresion(cfg.table, cliente_id)
    if dbutils.widgets.get("ejecutar_vacuum") == "si":
        plan += plan_vacuum(cfg.table)
    resultados = ejecutar_plan(spark, plan, modo, log)
    run.add_detalle(modo=modo, sentencias=len(plan))
    if modo == "plan":
        run.omitido("Plan mostrado sin ejecutar (confirmar = no)")

display(spark.createDataFrame(resultados, "accion STRING, estado STRING, detalle STRING"))

# COMMAND ----------

# MAGIC %md ## Verificación: el cliente conserva su clave, pero ya no tiene datos personales

# COMMAND ----------

display(spark.sql(f"""
  SELECT cliente_id, numero_documento, fecha_nacimiento, correo, telefono, segmento, es_actual
  FROM {cfg.table('silver_clientes')} WHERE cliente_id = '{cliente_id.replace("'", "")}'
"""))
