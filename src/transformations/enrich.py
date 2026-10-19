from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import PipelineConfig
from src.common.contracts import Contrato, Referencia, ReferenciaPolimorfica

PAYLOAD_SCHEMA = "estado_origen STRING, estado_destino STRING, usuario_proceso STRING"


class MissingDependencyError(RuntimeError):
    """La tabla padre de una referencia aún no existe (orden de ejecución incorrecto)."""


def agregar_referencia(
    df: DataFrame, ref: Referencia, padre: DataFrame, claves_cuarentena: DataFrame | None = None
) -> DataFrame:
    """Agrega las columnas de integridad referencial de `ref`.

    `padre` = tabla Silver del padre (versión actual); `claves_cuarentena` = DataFrame con una
    columna `clave_negocio` de las claves del padre que están en cuarentena.
    """
    k = f"__k_{ref.alias}"
    cols = [F.col(ref.columna_ref).alias(k)] + [
        F.col(c).alias(f"_refcol_{ref.alias}_{c}") for c in ref.traer
    ]
    lookup = padre.select(*cols).dropDuplicates([k]).withColumn(f"_ref_{ref.alias}", F.lit(True))
    out = df.join(F.broadcast(lookup), df[ref.columna] == lookup[k], "left").drop(k)
    out = out.withColumn(f"_ref_{ref.alias}", F.coalesce(F.col(f"_ref_{ref.alias}"), F.lit(False)))

    flag_q = f"_ref_{ref.alias}_cuarentena"
    if claves_cuarentena is None:
        return out.withColumn(flag_q, F.lit(False))
    kq = f"__kq_{ref.alias}"
    q = claves_cuarentena.select(F.col("clave_negocio").alias(kq)).distinct().withColumn(flag_q, F.lit(True))
    out = out.join(F.broadcast(q), out[ref.columna] == q[kq], "left").drop(kq)
    return out.withColumn(flag_q, F.coalesce(F.col(flag_q), F.lit(False)) & ~F.col(f"_ref_{ref.alias}"))


def agregar_referencia_polimorfica(
    df: DataFrame, ref: ReferenciaPolimorfica, claves_por_tipo: DataFrame, claves_cuarentena: DataFrame | None
) -> DataFrame:
    """`claves_por_tipo` y `claves_cuarentena`: columnas (`tipo`, `clave`)."""
    flag, flag_q = f"_ref_{ref.alias}", f"_ref_{ref.alias}_cuarentena"
    ok = claves_por_tipo.select(F.col("tipo").alias("__t"), F.col("clave").alias("__c")).distinct() \
        .withColumn(flag, F.lit(True))
    cond = (df[ref.columna_tipo] == ok["__t"]) & (df[ref.columna_id] == ok["__c"])
    out = df.join(F.broadcast(ok), cond, "left").drop("__t", "__c")
    out = out.withColumn(flag, F.coalesce(F.col(flag), F.lit(False)))
    if claves_cuarentena is None:
        return out.withColumn(flag_q, F.lit(False))
    q = claves_cuarentena.select(F.col("tipo").alias("__tq"), F.col("clave").alias("__cq")).distinct() \
        .withColumn(flag_q, F.lit(True))
    condq = (out[ref.columna_tipo] == q["__tq"]) & (out[ref.columna_id] == q["__cq"])
    out = out.join(F.broadcast(q), condq, "left").drop("__tq", "__cq")
    return out.withColumn(flag_q, F.coalesce(F.col(flag_q), F.lit(False)) & ~F.col(flag))


def _tabla_padre(spark: SparkSession, cfg: PipelineConfig, tabla: str, dataset_hijo: str) -> DataFrame:
    fqn = cfg.table(tabla)
    if not spark.catalog.tableExists(fqn):
        raise MissingDependencyError(
            f"[{dataset_hijo}] La tabla padre {fqn} no existe. Ejecute primero su tarea Silver "
            f"(orden: clientes -> polizas -> recaudos/siniestros -> movimientos)."
        )
    df = spark.table(fqn)
    return df.where("es_actual") if "es_actual" in df.columns else df


def _claves_cuarentena(spark: SparkSession, cfg: PipelineConfig, dataset: str) -> DataFrame | None:
    fqn = cfg.silver("cuarentena")
    if not spark.catalog.tableExists(fqn):
        return None
    return spark.table(fqn).where(F.col("dataset") == dataset).select("clave_negocio")


def enriquecer_referencias(spark: SparkSession, cfg: PipelineConfig, df: DataFrame, contrato: Contrato) -> DataFrame:
    for ref in contrato.referencias:
        padre = _tabla_padre(spark, cfg, ref.tabla, contrato.dataset)
        df = agregar_referencia(df, ref, padre, _claves_cuarentena(spark, cfg, ref.dataset_ref))
    poli = contrato.referencia_polimorfica
    if poli:
        claves, claves_q = [], []
        for tipo, dest in poli.destinos.items():
            padre = _tabla_padre(spark, cfg, dest["tabla"], contrato.dataset)
            claves.append(padre.select(F.lit(tipo).alias("tipo"), F.col(dest["columna_ref"]).alias("clave")))
            q = _claves_cuarentena(spark, cfg, dest["tabla"].removeprefix("silver_"))
            if q is not None:
                claves_q.append(q.select(F.lit(tipo).alias("tipo"), F.col("clave_negocio").alias("clave")))
        union = claves[0]
        for c in claves[1:]:
            union = union.unionByName(c)
        union_q = None
        if claves_q:
            union_q = claves_q[0]
            for c in claves_q[1:]:
                union_q = union_q.unionByName(c)
        df = agregar_referencia_polimorfica(df, poli, union, union_q)
    return df


def derivar_movimientos(df: DataFrame) -> DataFrame:
    """Expande el payload JSON. Un JSON inválido deja los campos en NULL (regla movimientos.VAL.payload)."""
    parsed = F.from_json(F.col("payload"), PAYLOAD_SCHEMA)
    return (
        df.withColumn("__p", parsed)
        .withColumn("estado_origen", F.col("__p.estado_origen"))
        .withColumn("estado_destino", F.col("__p.estado_destino"))
        .withColumn("usuario_proceso", F.col("__p.usuario_proceso"))
        .drop("__p")
    )


DERIVACIONES = {"movimientos": derivar_movimientos}
COLUMNAS_DERIVADAS = {"movimientos": ["estado_origen", "estado_destino", "usuario_proceso"]}


def aplicar_derivaciones(df: DataFrame, dataset: str) -> DataFrame:
    fn = DERIVACIONES.get(dataset)
    return fn(df) if fn else df
