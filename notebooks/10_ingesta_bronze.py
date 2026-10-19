# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Ingesta Bronze
# MAGIC
# MAGIC Carga los archivos de `raw/<dataset>/` a `bronze_<dataset>` como **copia fiel** (todo texto) con
# MAGIC metadatos de ingesta. Idempotente: un archivo cuyo contenido ya fue ingerido se omite en modo
# MAGIC `normal`; en modo `reproceso` se reescriben sus filas sin duplicar.
# MAGIC
# MAGIC | Parámetro | Descripción |
# MAGIC |---|---|
# MAGIC | `dataset` | Dataset a ingerir o `todos` |
# MAGIC | `fecha_corte` | YYYY-MM-DD. Vacío = último día del mes anterior |
# MAGIC | `modo` | `normal` (incremental por archivo) o `reproceso` |
# MAGIC | `job_run_id` | `{{job.run_id}}` cuando corre en el workflow |

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from src.common.config import DATASETS, config_from_widgets
from src.common.contracts import cargar_contrato
from src.common.control import TaskRun, ensure_control_tables
from src.common.logging_utils import get_logger
from src.ingestion.bronze import ingest_dataset

cfg = config_from_widgets(dbutils, {"dataset": "todos"})
dataset_param = dbutils.widgets.get("dataset").strip().lower()
datasets = list(DATASETS) if dataset_param in ("", "todos") else [d.strip() for d in dataset_param.split(",")]
ensure_control_tables(spark, cfg)

# COMMAND ----------

resultados = []
for ds in datasets:
    log = get_logger(run_id=cfg.run_id, capa="bronze", dataset=ds)
    contrato = cargar_contrato(ds, cfg.contracts_dir)
    with TaskRun(spark, cfg, tarea=f"bronze_{ds}", capa="bronze", dataset=ds, logger=log) as run:
        res = ingest_dataset(spark, cfg, contrato, log)
        run.set(registros_entrada=res.registros_ingeridos, registros_salida=res.registros_ingeridos)
        run.add_detalle(archivos_ingeridos=res.archivos_ingeridos, archivos_omitidos=res.archivos_omitidos,
                        cambios_esquema=res.cambios_esquema)
        if not res.archivos_ingeridos:
            run.omitido("Sin archivos nuevos para ingerir")
    resultados.append((ds, res.archivos_detectados, len(res.archivos_ingeridos), len(res.archivos_omitidos),
                       res.registros_ingeridos))

display(spark.createDataFrame(
    resultados, "dataset STRING, archivos_detectados INT, ingeridos INT, omitidos INT, registros LONG"))

# COMMAND ----------

# MAGIC %md ## Verificación: conteos en Bronze y reporte de archivos

# COMMAND ----------

display(spark.sql(" UNION ALL ".join(
    f"SELECT '{ds}' AS dataset, count(*) AS registros, count(DISTINCT _source_file_hash) AS archivos, "
    f"max(_ingestion_ts) AS ultima_ingesta FROM {cfg.bronze(ds)}" for ds in datasets
)))

# COMMAND ----------

display(spark.table(cfg.ctl("archivos_procesados")).where(f"run_id = '{cfg.run_id}'")
        .select("dataset", "archivo", "estado", "registros", "mensaje", "hash_archivo"))