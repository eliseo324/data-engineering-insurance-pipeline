from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import TZ_COLOMBIA, PipelineConfig
from src.common.control import append_rows, utcnow

HORA_SLA = time(8, 0)
DIA_SLA = 5
PRODUCTOS = ("poliza_360", "siniestralidad_mensual", "discrepancias", "calidad_pipeline")
FECHAS_FRESCURA = {"polizas": "fecha_actualizacion", "recaudos": "fecha_pago",
                   "siniestros": "fecha_actualizacion", "movimientos": "fecha_evento"}


def sla_objetivo(corte: date) -> datetime:
    """Día 5 del mes siguiente al corte, 08:00 hora Colombia, expresado en UTC (sin zona)."""
    primero_mes_siguiente = (corte.replace(day=1) + timedelta(days=32)).replace(day=1)
    local = datetime.combine(primero_mes_siguiente.replace(day=DIA_SLA), HORA_SLA, tzinfo=TZ_COLOMBIA)
    return local.astimezone(UTC).replace(tzinfo=None)


def evaluar_sla(corte: date, ts_publicacion: datetime, tipo_disparo: str | None) -> dict:
    objetivo = sla_objetivo(corte)
    aplica = (tipo_disparo or "").upper() == "PERIODIC"
    return {"sla_objetivo": objetivo, "aplica_sla": aplica,
            "cumple_sla": (ts_publicacion <= objetivo) if aplica else None}


def registrar_publicacion(spark: SparkSession, cfg: PipelineConfig, tipo_disparo: str | None, logger) -> dict:
    ts = utcnow()

    versiones = {}
    for p in PRODUCTOS:
        h = spark.sql(f"DESCRIBE HISTORY {cfg.gold(p)} LIMIT 1").first()
        versiones[f"gold_{p}"] = int(h["version"])

    maximos = {ds: spark.table(cfg.silver(ds)).agg(F.max(col)).first()[0] for ds, col in FECHAS_FRESCURA.items()}
    fecha_max = min(v for v in maximos.values() if v is not None) 
    frescura = (cfg.fecha_corte - fecha_max).days

    inicio = (spark.table(cfg.ctl("ejecuciones")).where(F.col("run_id") == cfg.run_id)
              .agg(F.min("inicio")).first()[0])
    fila = {
        "run_id": cfg.run_id, "pipeline": cfg.pipeline, "fecha_corte": cfg.fecha_corte, "ambiente": cfg.ambiente,
        "modo": cfg.modo, "tipo_disparo": tipo_disparo, "estado": "PUBLICADO", "inicio_ejecucion": inicio,
        "ts_publicacion": ts, "duracion_total_seg": (ts - inicio).total_seconds() if inicio else None,
        **evaluar_sla(cfg.fecha_corte, ts, tipo_disparo),
        "fecha_max_datos": fecha_max, "frescura_dias": frescura,
        "versiones_delta": json.dumps(versiones), "usuario": None,
    }
    append_rows(spark, cfg, "publicaciones", [fila])
    logger.info(f"Publicación registrada: corte={cfg.fecha_corte_str} frescura={frescura} días "
                f"SLA aplica={fila['aplica_sla']} cumple={fila['cumple_sla']} versiones={versiones}")
    return fila
