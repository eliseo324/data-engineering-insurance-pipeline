# Databricks notebook source
# MAGIC %md
# MAGIC # 90 · Desplegar el workflow (job como código)
# MAGIC
# MAGIC Crea o actualiza el job `insurance-medallion-<ambiente>` a partir de **la misma definición
# MAGIC versionada** que usa Databricks Asset Bundles (`resources/jobs/insurance_pipeline.job.yml`).
# MAGIC
# MAGIC * **Idempotente:** si el job ya existe (por nombre) se reemplaza su configuración (`jobs/reset`); si no, se crea.
# MAGIC * **Valida el DAG antes de desplegar:** dependencias, ausencia de ciclos, orden de FK y schedule.
# MAGIC * **Permisos:** otorga `CAN_VIEW` sobre el job a los usuarios indicados (p. ej. los evaluadores).
# MAGIC
# MAGIC No se usan tokens ni secretos: el SDK se autentica con la identidad del usuario del notebook.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

from src.common.logging_utils import get_logger
from src.orchestration.job_spec import cargar_spec_job, orden_topologico

dbutils.widgets.dropdown("ambiente", "dev", ["dev", "qa", "prod"])
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("volume", "volume1")
dbutils.widgets.dropdown("pause_status", "UNPAUSED", ["UNPAUSED", "PAUSED"])
dbutils.widgets.text("usuarios_lectura", "yesidmontoya@sura.com.co,fjcarvajal@sura.com.co")

log = get_logger(capa="orquestacion", dataset="job")
w = WorkspaceClient()
usuario = w.current_user.me().user_name
variables = {k: dbutils.widgets.get(k) for k in ("ambiente", "catalog", "schema", "volume", "pause_status")}

spec = cargar_spec_job(variables, usuario, REPO_ROOT)
log.info(f"Job '{spec['name']}' validado: {len(spec['tasks'])} tareas, repo={REPO_ROOT}")
for i, nivel in enumerate(orden_topologico(spec["tasks"]), 1):
    log.info(f"Nivel {i}: {', '.join(nivel)}")

# COMMAND ----------

# MAGIC %md ## Crear o actualizar

# COMMAND ----------

existentes = w.api_client.do("GET", "/api/2.2/jobs/list", query={"name": spec["name"]}).get("jobs", [])
if existentes:
    job_id = existentes[0]["job_id"]
    w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": spec})
    accion = "actualizado"
else:
    job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=spec)["job_id"]
    accion = "creado"
log.info(f"Job {accion}: job_id={job_id}")

# COMMAND ----------

# MAGIC %md ## Permisos de solo lectura sobre el job (mínimo privilegio)
# MAGIC El usuario debe existir en el workspace

# COMMAND ----------

lectores = [u.strip() for u in dbutils.widgets.get("usuarios_lectura").split(",") if u.strip()]
if lectores:
    try:
        w.api_client.do("PATCH", f"/api/2.0/permissions/jobs/{job_id}", body={
            "access_control_list": [{"user_name": u, "permission_level": "CAN_VIEW"} for u in lectores]})
        log.info(f"CAN_VIEW otorgado a: {lectores}")
    except Exception as exc: 
        log.warning(f"No se pudieron asignar permisos ({exc}). Verifique que los usuarios existan en el workspace.")

host = w.config.host.rstrip("/")
displayHTML(f"""
<p>Job <b>{spec['name']}</b> {accion}. <a href="{host}/jobs/{job_id}" target="_blank">Abrir job {job_id}</a></p>
<p>Schedule: <code>{spec['schedule']['quartz_cron_expression']}</code> ({spec['schedule']['timezone_id']}),
estado <b>{spec['schedule']['pause_status']}</b>.</p>
""")