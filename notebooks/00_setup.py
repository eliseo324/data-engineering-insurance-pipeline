# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup del entorno
# MAGIC
# MAGIC Prepara, de forma **idempotente**, lo que el pipeline necesita en Unity Catalog:
# MAGIC
# MAGIC 1. Verifica el volumen `workspace.default.volume1`.
# MAGIC 2. Crea la zona de aterrizaje `raw/<dataset>/` dentro del volumen.
# MAGIC 3. Mueve los CSV que se hayan subido a la raíz del volumen hacia su carpeta `raw/<dataset>/`.
# MAGIC 4. Crea las tablas de control (`ctl_*`) usadas para observabilidad, idempotencia y calidad.
# MAGIC
# MAGIC Se puede ejecutar cuantas veces se quiera sin efectos duplicados.

# COMMAND ----------

# MAGIC %md ## Hace importable el paquete `src` del repositorio

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from src.common.config import DATASETS, config_from_widgets
from src.common.control import ensure_control_tables
from src.common.logging_utils import get_logger

cfg = config_from_widgets(dbutils, {"organizar_archivos_raiz": "true"})
organizar = dbutils.widgets.get("organizar_archivos_raiz").lower() == "true"
log = get_logger(run_id=cfg.run_id, capa="setup")
log.info(f"Configuración: {cfg.as_dict()}")

# COMMAND ----------

# MAGIC %md ## 1. Volumen y ruta de guardado

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{cfg.volume} "
          "COMMENT 'Ruta de llegada de archivos fuente de la prueba (CSV/Parquet)'")

for ds in DATASETS:
    os.makedirs(cfg.landing_path(ds), exist_ok=True)
    log.info(f"Carpeta lista: {cfg.landing_path(ds)}")

# COMMAND ----------

# MAGIC %md ## 2. Organizar archivos subidos a la raíz del volumen
# MAGIC Si los CSV se cargaron directamente en `/Volumes/workspace/default/volume1/`, se **mueven** a
# MAGIC `raw/<dataset>/` según el nombre del archivo (p. ej. `polizas.csv` → `raw/polizas/polizas.csv`), para evitar rutas, fechas y nombres rígidos.

# COMMAND ----------

import shutil

if organizar:
    for fname in sorted(os.listdir(cfg.volume_path)):
        src_path = os.path.join(cfg.volume_path, fname)
        if not os.path.isfile(src_path) or not fname.lower().endswith((".csv", ".parquet")):
            continue
        dataset = next((ds for ds in DATASETS if ds in fname.lower()), None)
        if dataset is None:
            log.warning(f"No se reconoce el dataset del archivo {fname}; se deja en la raíz")
            continue
        dst = os.path.join(cfg.landing_path(dataset), fname)
        shutil.move(src_path, dst)
        log.info(f"Movido {fname} -> {dst}")

display(spark.createDataFrame(
    [(ds, f, os.path.getsize(os.path.join(cfg.landing_path(ds), f)))
     for ds in DATASETS for f in sorted(os.listdir(cfg.landing_path(ds)))],
    "dataset STRING, archivo STRING, bytes LONG",
))

# COMMAND ----------

# MAGIC %md ## 3. Tablas de control

# COMMAND ----------

ensure_control_tables(spark, cfg)
display(spark.sql(f"SHOW TABLES IN {cfg.catalog}.{cfg.schema} LIKE 'ctl_*'"))