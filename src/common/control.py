from __future__ import annotations

import json
import time
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from src.common.config import PipelineConfig

CONTROL_DDL: dict[str, str] = {
    "ejecuciones": """
        run_id STRING, pipeline STRING, tarea STRING, capa STRING, dataset STRING,
        fecha_corte DATE, modo STRING, ambiente STRING, estado STRING,
        inicio TIMESTAMP, fin TIMESTAMP, duracion_seg DOUBLE,
        registros_entrada BIGINT, registros_salida BIGINT, registros_rechazados BIGINT,
        registros_duplicados BIGINT, mensaje STRING, detalle STRING, usuario STRING
    """,
    "archivos_procesados": """
        dataset STRING, archivo STRING, ruta STRING, hash_archivo STRING, tamano_bytes BIGINT,
        fecha_modificacion TIMESTAMP, esquema_hash STRING, columnas ARRAY<STRING>, registros BIGINT,
        estado STRING, mensaje STRING, run_id STRING, fecha_corte DATE, fecha_registro TIMESTAMP
    """,
    "esquemas": """
        dataset STRING, archivo STRING, hash_archivo STRING, esquema_hash STRING,
        columnas ARRAY<STRING>, columnas_nuevas ARRAY<STRING>, columnas_faltantes ARRAY<STRING>,
        cambio_detectado BOOLEAN, run_id STRING, fecha_registro TIMESTAMP
    """,
    "watermarks": """
        dataset STRING, capa STRING, ultimo_ingestion_ts TIMESTAMP, run_id STRING, actualizado TIMESTAMP
    """,
    "calidad_resultados": """
        run_id STRING, pipeline STRING, fecha_corte DATE, capa STRING, dataset STRING,
        regla_id STRING, dimension STRING, descripcion STRING, severidad STRING, accion STRING,
        registros_evaluados BIGINT, registros_fallidos BIGINT, pct_fallo DOUBLE, umbral_pct DOUBLE,
        resultado STRING, fecha_evaluacion TIMESTAMP
    """,
    "reconciliacion": """
        run_id STRING, pipeline STRING, fecha_corte DATE, dataset STRING, etapa_origen STRING,
        etapa_destino STRING, metrica STRING, valor_origen DECIMAL(24,2), valor_destino DECIMAL(24,2),
        diferencia DECIMAL(24,2), tolerancia DECIMAL(24,2), resultado STRING, detalle STRING,
        fecha_evaluacion TIMESTAMP
    """,
    "publicaciones": """
        run_id STRING, pipeline STRING, fecha_corte DATE, ambiente STRING, modo STRING, tipo_disparo STRING,
        estado STRING, inicio_ejecucion TIMESTAMP, ts_publicacion TIMESTAMP, duracion_total_seg DOUBLE,
        sla_objetivo TIMESTAMP, aplica_sla BOOLEAN, cumple_sla BOOLEAN, fecha_max_datos DATE,
        frescura_dias INT, versiones_delta STRING, usuario STRING
    """,
    "alertas": """
        run_id STRING, alerta_id STRING, nombre STRING, severidad STRING, responsable STRING,
        descripcion STRING, detalle STRING, fecha_evaluacion TIMESTAMP, estado STRING
    """,
}

CONTROL_COMMENTS = {
    "ejecuciones": "Reporte de ejecución por tarea: estado, duración, volúmenes y errores.",
    "archivos_procesados": "Archivos ingeridos a Bronze identificados por hash de contenido (idempotencia).",
    "esquemas": "Huella del esquema recibido por archivo para detectar evolución de esquema.",
    "watermarks": "Marca de agua de la última ingesta procesada por dataset y capa (carga incremental).",
    "calidad_resultados": "Resultados de reglas de calidad por ejecución, dataset y regla.",
    "reconciliacion": "Reconciliación de conteos, totales monetarios y claves entre etapas.",
    "publicaciones": "Publicaciones de productos Gold: versión Delta, frescura y cumplimiento del SLA.",
    "alertas": "Alertas operativas y de datos disparadas por ejecución, con severidad y responsable.",
}


def _schema_ddl(name: str) -> str:
    return " ".join(CONTROL_DDL[name].split())


def ensure_control_tables(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Crea (si no existen) las tablas de control. Idempotente."""
    for name, ddl in CONTROL_DDL.items():
        comment = CONTROL_COMMENTS[name].replace("'", "")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {cfg.ctl(name)} ({' '.join(ddl.split())}) "
            f"USING DELTA COMMENT '{comment}'"
        )


def append_rows(spark: SparkSession, cfg: PipelineConfig, name: str, rows: list[dict]) -> None:
    """Agrega filas de metadatos a una tabla de control."""
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=_schema_ddl(name))
    df.write.format("delta").mode("append").saveAsTable(cfg.ctl(name))


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def get_watermark(spark: SparkSession, cfg: PipelineConfig, dataset: str, capa: str):
    """Último `_ingestion_ts` procesado exitosamente para dataset/capa (None si nunca)."""
    row = (
        spark.table(cfg.ctl("watermarks"))
        .where(f"dataset = '{dataset}' AND capa = '{capa}'")
        .agg({"ultimo_ingestion_ts": "max"})
        .first()
    )
    return row[0] if row else None


def set_watermark(spark: SparkSession, cfg: PipelineConfig, dataset: str, capa: str, valor) -> None:
    if valor is None:
        return
    append_rows(
        spark,
        cfg,
        "watermarks",
        [{"dataset": dataset, "capa": capa, "ultimo_ingestion_ts": valor, "run_id": cfg.run_id, "actualizado": utcnow()}],
    )


class TaskRun:
    """Context manager que registra una tarea en `ctl_ejecuciones`, exitosa o fallida.

    Uso:
        with TaskRun(spark, cfg, tarea="bronze_polizas", capa="bronze", dataset="polizas") as run:
            ...
            run.set(registros_entrada=100, registros_salida=98)
    """

    def __init__(self, spark, cfg: PipelineConfig, tarea: str, capa: str, dataset: str | None = None, logger=None):
        self.spark, self.cfg, self.tarea, self.capa, self.dataset = spark, cfg, tarea, capa, dataset
        self.logger = logger
        self.metrics: dict = {}
        self.detalle: dict = {}
        self.estado = "EXITOSO"
        self.mensaje = ""

    def set(self, **metrics) -> None:
        self.metrics.update({k: v for k, v in metrics.items() if v is not None})

    def add_detalle(self, **kwargs) -> None:
        self.detalle.update(kwargs)

    def omitido(self, mensaje: str) -> None:
        self.estado, self.mensaje = "OMITIDO", mensaje

    def __enter__(self) -> TaskRun:
        self._t0 = time.monotonic()
        self.inicio = utcnow()
        if self.logger:
            self.logger.info(f"Inicio tarea {self.tarea}")
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None:
            self.estado = "FALLIDO"
            self.mensaje = f"{exc_type.__name__}: {exc}"[:4000]
        row = {
            "run_id": self.cfg.run_id,
            "pipeline": self.cfg.pipeline,
            "tarea": self.tarea,
            "capa": self.capa,
            "dataset": self.dataset,
            "fecha_corte": self.cfg.fecha_corte,
            "modo": self.cfg.modo,
            "ambiente": self.cfg.ambiente,
            "estado": self.estado,
            "inicio": self.inicio,
            "fin": utcnow(),
            "duracion_seg": round(time.monotonic() - self._t0, 3),
            "registros_entrada": self.metrics.get("registros_entrada"),
            "registros_salida": self.metrics.get("registros_salida"),
            "registros_rechazados": self.metrics.get("registros_rechazados"),
            "registros_duplicados": self.metrics.get("registros_duplicados"),
            "mensaje": self.mensaje or None,
            "detalle": json.dumps(self.detalle, default=str, ensure_ascii=False) if self.detalle else None,
            "usuario": _current_user(self.spark),
        }
        try:
            append_rows(self.spark, self.cfg, "ejecuciones", [row])
        except Exception as log_exc:  
            if self.logger:
                self.logger.error(f"No se pudo registrar la ejecución en ctl_ejecuciones: {log_exc}")
        if self.logger:
            log = self.logger.error if self.estado == "FALLIDO" else self.logger.info
            log(f"Fin tarea {self.tarea} estado={self.estado} duracion={row['duracion_seg']}s {self.mensaje}")
        return False  


def _current_user(spark) -> str | None:
    try:
        return spark.sql("SELECT current_user()").first()[0]
    except Exception:  # noqa: BLE001
        return None