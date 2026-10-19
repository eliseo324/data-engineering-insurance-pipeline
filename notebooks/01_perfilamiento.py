# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Perfilamiento y entendimiento de las fuentes
# MAGIC
# MAGIC Perfila los archivos **tal como llegan** (todo como texto) para decidir las reglas de Silver.
# MAGIC Todo el cálculo es distribuido (PySpark); no se usa pandas ni se traen datasets al driver.
# MAGIC
# MAGIC **Salidas**
# MAGIC * `ctl_perfilamiento`: métricas por dataset y columna (formato largo, consultable en SQL).
# MAGIC * Chequeos cruzados de integridad referencial y consistencia temporal.
# MAGIC * Las conclusiones se documentan en `docs/decisiones.md`.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from functools import reduce

from pyspark.sql import functions as F

from src.common.config import DATASETS, config_from_widgets
from src.common.contracts import cargar_contrato
from src.common.dates import parse_date
from src.common.logging_utils import get_logger
from src.quality.profiling import orphan_count, profile_dataframe

cfg = config_from_widgets(dbutils)
log = get_logger(run_id=cfg.run_id, capa="perfilamiento")
contratos = {ds: cargar_contrato(ds) for ds in DATASETS}


def leer_landing(ds: str):
    c = contratos[ds]
    return (
        spark.read.format("csv")
        .option("header", True).option("sep", c.fuente["separador"]).option("encoding", "UTF-8")
        .option("quote", '"').option("escape", '"').option("inferSchema", False)
        .load(cfg.landing_path(c.fuente["subcarpeta"]))
    )


raw = {ds: leer_landing(ds) for ds in DATASETS}

# COMMAND ----------

# MAGIC %md ## 1. Métricas por columna

# COMMAND ----------

perfil = reduce(lambda a, b: a.unionByName(b), [profile_dataframe(raw[ds], contratos[ds]) for ds in DATASETS])
perfil = perfil.withColumn("run_id", F.lit(cfg.run_id)).withColumn("fecha_perfilamiento", F.current_timestamp())
perfil.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.ctl("perfilamiento"))
log.info("Perfil guardado en ctl_perfilamiento")

# COMMAND ----------

# MAGIC %md ### Resumen por dataset: volumen y duplicados

# COMMAND ----------

display(spark.sql(f"""
  SELECT dataset,
         max(CASE WHEN metrica = 'registros' THEN CAST(valor AS INT) END)               AS registros,
         max(CASE WHEN metrica = 'columnas' THEN CAST(valor AS INT) END)                AS columnas,
         max(CASE WHEN metrica = 'duplicados_exactos' THEN CAST(valor AS INT) END)      AS dup_exactos,
         max(CASE WHEN metrica = 'duplicados_clave_negocio' THEN CAST(valor AS INT) END) AS dup_clave
  FROM {cfg.ctl('perfilamiento')} GROUP BY dataset ORDER BY dataset
"""))

# COMMAND ----------

# MAGIC %md ### Hallazgos de calidad por columna (solo métricas con problemas)

# COMMAND ----------

display(spark.sql(f"""
  SELECT dataset, columna, metrica, valor
  FROM {cfg.ctl('perfilamiento')}
  WHERE (metrica IN ('nulos_o_vacios','negativos','no_numericos','fuera_de_dominio','no_cumple_patron')
         AND CAST(valor AS DOUBLE) > 0)
     OR (metrica IN ('formato_dd/MM/yyyy','formato_yyyy/MM/dd','formato_OTRO') AND CAST(valor AS INT) > 0)
  ORDER BY dataset, columna, metrica
"""))

# COMMAND ----------

# MAGIC %md ## 2. Integridad referencial (huérfanos)

# COMMAND ----------

checks = [
    ("polizas sin cliente", raw["polizas"], "cliente_id", raw["clientes"], "cliente_id"),
    ("recaudos sin poliza", raw["recaudos"], "poliza_id", raw["polizas"], "poliza_id"),
    ("siniestros sin poliza", raw["siniestros"], "poliza_id", raw["polizas"], "poliza_id"),
]
mov = raw["movimientos"]
for entidad, ds, key in [("POLIZA", "polizas", "poliza_id"), ("SINIESTRO", "siniestros", "siniestro_id"),
                         ("RECAUDO", "recaudos", "recaudo_id"), ("CLIENTE", "clientes", "cliente_id")]:
    checks.append((f"movimientos {entidad} sin entidad", mov.where(F.col("entidad") == entidad), "id_entidad", raw[ds], key))

ri = spark.createDataFrame(
    [(name, child.count(), orphan_count(child, cc, parent, pc).count()) for name, child, cc, parent, pc in checks],
    "chequeo STRING, registros_hijo LONG, huerfanos LONG",
)
display(ri)

# COMMAND ----------

# MAGIC %md ### Ejemplos de claves huérfanas

# COMMAND ----------

display(orphan_count(raw["polizas"], "cliente_id", raw["clientes"], "cliente_id").select("poliza_id", "cliente_id").limit(10))
display(orphan_count(raw["siniestros"], "poliza_id", raw["polizas"], "poliza_id").select("siniestro_id", "poliza_id").limit(10))

# COMMAND ----------

# MAGIC %md ## 3. Consistencia temporal y de negocio

# COMMAND ----------

pol = raw["polizas"].select(
    "poliza_id", "estado",
    parse_date("fecha_inicio_vigencia").alias("ini"), parse_date("fecha_fin_vigencia").alias("fin"))
sin = raw["siniestros"].select(
    "siniestro_id", "poliza_id", parse_date("fecha_ocurrencia").alias("ocu"), parse_date("fecha_aviso").alias("avi"))
rec = raw["recaudos"].select("recaudo_id", "poliza_id", "estado_transaccion",
                             parse_date("fecha_pago").alias("fpago"), F.expr("try_cast(valor AS DOUBLE)").alias("valor"))
mv = mov.select("evento_id", parse_date("fecha_evento").alias("fev"), parse_date("fecha_ingesta").alias("fing"))
corte = F.lit(cfg.fecha_corte)

consistencia = [
    ("polizas con fin_vigencia < inicio_vigencia", pol.where(F.col("fin") < F.col("ini")).count()),
    ("polizas 'Vigente' con fin_vigencia < fecha_corte", pol.where((F.col("estado") == "Vigente") & (F.col("fin") < corte)).count()),
    ("siniestros con aviso < ocurrencia", sin.where(F.col("avi") < F.col("ocu")).count()),
    ("siniestros con aviso posterior a la fecha de corte", sin.where(F.col("avi") > corte).count()),
    ("siniestros con ocurrencia fuera de vigencia",
     sin.join(pol, "poliza_id").where((F.col("ocu") < F.col("ini")) | (F.col("ocu") > F.col("fin"))).count()),
    ("recaudos con pago fuera de vigencia",
     rec.join(pol, "poliza_id").where((F.col("fpago") < F.col("ini")) | (F.col("fpago") > F.col("fin"))).count()),
    ("recaudos negativos con estado Aplicado", rec.where((F.col("valor") < 0) & (F.col("estado_transaccion") == "Aplicado")).count()),
    ("movimientos con fecha_ingesta < fecha_evento", mv.where(F.col("fing") < F.col("fev")).count()),
    ("movimientos tardíos (> 30 días entre evento e ingesta)", mv.where(F.datediff("fing", "fev") > 30).count()),
]
display(spark.createDataFrame(consistencia, "chequeo STRING, registros LONG"))

# COMMAND ----------

# MAGIC %md ## 4. Duplicados por clave de negocio en movimientos (reenvíos con distinto evento_id)

# COMMAND ----------

k = contratos["movimientos"].clave_negocio
dups = mov.groupBy(*k).agg(F.count(F.lit(1)).alias("n"), F.collect_set("evento_id").alias("eventos")).where("n > 1")
display(dups)

# COMMAND ----------

# MAGIC %md ## 5. Mismo timestamp para la misma entidad (para poder dar un desempate)

# COMMAND ----------

display(mov.groupBy("entidad", "id_entidad", "fecha_evento").agg(
    F.count(F.lit(1)).alias("eventos"), F.collect_list("tipo_evento").alias("tipos"),
    F.collect_list("secuencia").alias("secuencias")).where("eventos > 1"))

# COMMAND ----------

# MAGIC %md ## 6. Estructura del payload JSON de movimientos

# COMMAND ----------

display(mov.select(F.explode(F.json_object_keys("payload")).alias("clave")).groupBy("clave").count())
display(mov.select("entidad", F.get_json_object("payload", "$.estado_destino").alias("estado_destino"))
        .groupBy("entidad", "estado_destino").count().orderBy("entidad", "estado_destino"))