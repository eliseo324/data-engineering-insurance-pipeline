from __future__ import annotations

from dataclasses import dataclass, field
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import PipelineConfig
from src.common.contracts import Contrato
from src.common.control import append_rows, utcnow
from src.common.hashing import row_hash, schema_hash
from src.ingestion.file_discovery import SourceFile, discover_files, select_files_to_process

METADATA_COLUMNS = [
    "_source_file",
    "_source_file_name",
    "_source_file_hash",
    "_source_file_mtime",
    "_ingestion_ts",
    "_run_id",
    "_fecha_corte",
    "_schema_hash",
    "_row_hash",
]


class SchemaContractError(RuntimeError):
    """Uno o más archivos no cumplen las columnas requeridas del contrato (severidad: bloqueo)."""


@dataclass
class IngestResult:
    dataset: str
    archivos_detectados: int = 0
    archivos_ingeridos: list[str] = field(default_factory=list)
    archivos_omitidos: list[str] = field(default_factory=list)
    archivos_rechazados: list[str] = field(default_factory=list)
    registros_ingeridos: int = 0
    cambios_esquema: list[dict] = field(default_factory=list)


def read_source_file(spark: SparkSession, source: SourceFile, contrato: Contrato) -> DataFrame:
    """Lee un archivo como texto, respetando el formato declarado en el contrato."""
    fuente = contrato.fuente
    fmt = fuente.get("formato", "csv").lower()
    if fmt == "parquet":
        df = spark.read.parquet(source.path)
        return df.select([F.col(c).cast("string").alias(c.strip()) for c in df.columns])
    if fmt != "csv":
        raise ValueError(f"Formato no soportado para {contrato.dataset}: {fmt}")
    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("sep", fuente.get("separador", ","))
        .option("encoding", fuente.get("encoding", "UTF-8"))
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", False)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .load(source.path)
    )
    return df.toDF(*[c.strip() for c in df.columns])


def add_ingestion_metadata(
    df: DataFrame, source: SourceFile, cfg: PipelineConfig, ingestion_ts, data_columns: list[str]
) -> DataFrame:
    """Agrega columnas técnicas de trazabilidad. `data_columns` define el orden del hash de fila."""
    return (
        df.withColumn("_source_file", F.lit(source.path))
        .withColumn("_source_file_name", F.lit(source.name))
        .withColumn("_source_file_hash", F.lit(source.sha256))
        .withColumn("_source_file_mtime", F.lit(source.modified_at).cast("timestamp"))
        .withColumn("_ingestion_ts", F.lit(ingestion_ts).cast("timestamp"))
        .withColumn("_run_id", F.lit(cfg.run_id))
        .withColumn("_fecha_corte", F.lit(cfg.fecha_corte).cast("date"))
        .withColumn("_schema_hash", F.lit(schema_hash(df.columns)))
        .withColumn("_row_hash", row_hash(data_columns))
    )


def _ordered_data_columns(contrato: Contrato, received: list[str]) -> list[str]:
    """Columnas del contrato presentes (en orden del contrato) + columnas nuevas (orden alfabético)."""
    known = [c for c in contrato.nombres if c in received]
    extra = sorted(c for c in received if c not in contrato.nombres)
    return known + extra


def _processed_hashes(spark: SparkSession, cfg: PipelineConfig, dataset: str) -> set[str]:
    rows = (
        spark.table(cfg.ctl("archivos_procesados"))
        .where(f"dataset = '{dataset}' AND estado = 'INGESTADO'")
        .select("hash_archivo")
        .distinct()
        .collect()
    )
    return {r[0] for r in rows}


def _previous_schema_hash(spark: SparkSession, cfg: PipelineConfig, dataset: str) -> str | None:
    row = (
        spark.table(cfg.ctl("esquemas"))
        .where(f"dataset = '{dataset}'")
        .orderBy(F.col("fecha_registro").desc())
        .select("esquema_hash")
        .first()
    )
    return row[0] if row else None


def write_bronze(spark: SparkSession, df: DataFrame, table: str, file_hashes: list[str]) -> None:
    """Escritura idempotente: reemplaza solo las filas de los archivos del lote."""
    if not spark.catalog.tableExists(table):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table)
        )
        return
    in_list = ", ".join(f"'{h}'" for h in file_hashes)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_source_file_hash IN ({in_list})")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )


def ingest_dataset(spark: SparkSession, cfg: PipelineConfig, contrato: Contrato, logger) -> IngestResult:
    """Ingiere a Bronze los archivos pendientes de un dataset."""
    dataset = contrato.dataset
    result = IngestResult(dataset=dataset)
    landing = cfg.landing_path(contrato.fuente.get("subcarpeta", dataset))
    files = discover_files(landing, contrato.fuente.get("patron_archivo", "*.csv"))
    result.archivos_detectados = len(files)
    logger.info(f"{len(files)} archivo(s) detectado(s) en {landing}")
    if not files:
        logger.warning(f"No hay archivos en {landing}. Nada que ingerir.")
        return result

    to_process, skipped = select_files_to_process(files, _processed_hashes(spark, cfg, dataset), cfg.modo)
    ingestion_ts = utcnow()
    control_rows, schema_rows, frames, rejected = [], [], [], []
    prev_schema = _previous_schema_hash(spark, cfg, dataset)

    for f in skipped:
        logger.info(f"Omitido (contenido ya ingerido, hash={f.sha256[:12]}): {f.name}")
        result.archivos_omitidos.append(f.name)
        control_rows.append(_control_row(cfg, dataset, f, None, None, 0, "OMITIDO_DUPLICADO",
                                         "Contenido idéntico a un archivo ya ingerido", ingestion_ts))

    for f in to_process:
        raw = read_source_file(spark, f, contrato)
        cmp = contrato.comparar_esquema(raw.columns)
        s_hash = schema_hash(raw.columns)
        cambio = prev_schema is not None and s_hash != prev_schema
        schema_rows.append({
            "dataset": dataset, "archivo": f.name, "hash_archivo": f.sha256, "esquema_hash": s_hash,
            "columnas": raw.columns, "columnas_nuevas": cmp["nuevas"], "columnas_faltantes": cmp["faltantes"],
            "cambio_detectado": bool(cambio or cmp["nuevas"] or cmp["faltantes"]),
            "run_id": cfg.run_id, "fecha_registro": ingestion_ts,
        })
        if cmp["faltantes_requeridas"]:
            msg = f"Faltan columnas requeridas {cmp['faltantes_requeridas']}"
            logger.error(f"Archivo rechazado {f.name}: {msg}")
            rejected.append(f.name)
            control_rows.append(_control_row(cfg, dataset, f, s_hash, raw.columns, 0, "RECHAZADO_ESQUEMA", msg, ingestion_ts))
            continue
        if cmp["nuevas"] or cmp["faltantes"]:
            logger.warning(f"Evolución de esquema en {f.name}: nuevas={cmp['nuevas']} faltantes={cmp['faltantes']}")
            result.cambios_esquema.append({"archivo": f.name, **cmp})
        prev_schema = s_hash
        data_cols = _ordered_data_columns(contrato, raw.columns)
        frames.append((f, s_hash, raw.columns, add_ingestion_metadata(raw, f, cfg, ingestion_ts, data_cols)))

    if frames:
        batch = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [fr[3] for fr in frames])
        write_bronze(spark, batch, cfg.bronze(dataset), [fr[0].sha256 for fr in frames])
        counts = {
            r["_source_file_hash"]: r["n"]
            for r in spark.table(cfg.bronze(dataset))
            .where(F.col("_source_file_hash").isin([fr[0].sha256 for fr in frames]))
            .groupBy("_source_file_hash").agg(F.count(F.lit(1)).alias("n"))
            .collect()
        }
        for f, s_hash, cols, _ in frames:
            n = int(counts.get(f.sha256, 0))
            result.archivos_ingeridos.append(f.name)
            result.registros_ingeridos += n
            estado_msg = "Archivo vacío" if n == 0 else None
            if n == 0:
                logger.warning(f"Archivo sin registros: {f.name}")
            control_rows.append(_control_row(cfg, dataset, f, s_hash, cols, n, "INGESTADO", estado_msg, ingestion_ts))
            logger.info(f"Ingerido {f.name}: {n} registros")

    append_rows(spark, cfg, "archivos_procesados", control_rows)
    append_rows(spark, cfg, "esquemas", schema_rows)
    result.archivos_rechazados = rejected
    if rejected:
        raise SchemaContractError(
            f"[{dataset}] {len(rejected)} archivo(s) rechazado(s) por incumplir el contrato: {rejected}. "
            f"Se ingirieron {len(result.archivos_ingeridos)} archivo(s) válidos. Revise ctl_archivos_procesados."
        )
    return result


def _control_row(cfg, dataset, f: SourceFile, s_hash, cols, n, estado, mensaje, ts) -> dict:
    return {
        "dataset": dataset, "archivo": f.name, "ruta": f.path, "hash_archivo": f.sha256,
        "tamano_bytes": f.size_bytes, "fecha_modificacion": f.modified_at, "esquema_hash": s_hash,
        "columnas": list(cols) if cols else None, "registros": n, "estado": estado, "mensaje": mensaje,
        "run_id": cfg.run_id, "fecha_corte": cfg.fecha_corte, "fecha_registro": ts,
    }
