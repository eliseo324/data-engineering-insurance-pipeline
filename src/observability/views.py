from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from src.common.contracts import REPO_ROOT, cargar_contrato

FECHAS_DATO = {
    "clientes": "fecha_actualizacion",
    "polizas": "fecha_actualizacion",
    "recaudos": "fecha_pago",
    "siniestros": "fecha_actualizacion",
    "movimientos": "fecha_evento",
}


@dataclass(frozen=True)
class Vista:
    nombre: str
    comentario: str
    sql: str


def cargar_config_observabilidad(path: str = "conf/observabilidad.yml") -> dict:
    import yaml

    p = Path(path) if Path(path).is_absolute() else REPO_ROOT / path
    return yaml.safe_load(p.read_text(encoding="utf-8"))


def vistas(T: Callable[[str], str], consumo: dict | None = None) -> list[Vista]:
    c = consumo or {"dbu_por_hora_tarea": 2.0, "usd_por_dbu": 0.35}
    frescura = " UNION ALL ".join(
        f"""SELECT '{ds}' AS dataset, max({col}) AS fecha_max_dato, max(_silver_ts) AS ultima_actualizacion_silver,
                   {int(cargar_contrato(ds).sla.get('frescura_max_dias', 5))} AS frescura_max_dias
            FROM {T(f'silver_{ds}')}"""
        for ds, col in FECHAS_DATO.items()
    )
    return [
        Vista("vw_obs_ejecuciones", "Una fila por ejecución del pipeline: estado final, duración, reintentos y publicación", f"""
            WITH intentos AS (  -- un Repair run conserva el run_id: el estado final es el del último intento
              SELECT run_id, tarea, max(fecha_corte) AS fecha_corte, max(modo) AS modo, max(ambiente) AS ambiente,
                     min(inicio) AS inicio, max(fin) AS fin, max_by(estado, fin) AS estado_final,
                     count(*) - 1 AS reintentos, sum(duracion_seg) AS segundos,
                     max_by(mensaje, CASE WHEN estado = 'FALLIDO' THEN fin END) AS ultimo_error
              FROM {T('ctl_ejecuciones')} GROUP BY run_id, tarea),
            t AS (
              SELECT run_id, max(fecha_corte) AS fecha_corte, max(modo) AS modo, max(ambiente) AS ambiente,
                     min(inicio) AS inicio, max(fin) AS fin, count(*) AS tareas,
                     count_if(estado_final = 'FALLIDO') AS tareas_fallidas,
                     count_if(estado_final = 'OMITIDO') AS tareas_omitidas,
                     sum(reintentos) AS reintentos, sum(segundos) AS segundos_computo,
                     max_by(ultimo_error, CASE WHEN ultimo_error IS NOT NULL THEN fin END) AS ultimo_error
              FROM intentos GROUP BY run_id)
            SELECT t.*, timestampdiff(SECOND, t.inicio, t.fin) AS duracion_seg,
                   CASE WHEN t.tareas_fallidas > 0 THEN 'FALLIDO'
                        WHEN p.run_id IS NOT NULL THEN 'PUBLICADO' ELSE 'PARCIAL' END AS estado_ejecucion,
                   p.tipo_disparo, p.cumple_sla
            FROM t LEFT JOIN {T('ctl_publicaciones')} p ON p.run_id = t.run_id"""),

        Vista("vw_obs_tareas", "Detalle por tarea: estado, duración y volúmenes (tendencia por tarea)", f"""
            SELECT run_id, fecha_corte, capa, dataset, tarea, estado, inicio, fin, duracion_seg,
                   registros_entrada, registros_salida, registros_rechazados, registros_duplicados, mensaje
            FROM {T('ctl_ejecuciones')}"""),

        Vista("vw_obs_volumenes", "Volumen de entrada y salida, aceptados, rechazados y duplicados por capa y dataset", f"""
            SELECT run_id, fecha_corte, capa, dataset, max(inicio) AS inicio,
                   sum(registros_entrada) AS entrada, sum(registros_salida) AS salida,
                   sum(registros_rechazados) AS rechazados, sum(registros_duplicados) AS duplicados,
                   round(100 * try_divide(sum(registros_rechazados), sum(registros_entrada)), 3) AS pct_rechazo,
                   round(100 * try_divide(sum(registros_duplicados), sum(registros_entrada)), 3) AS pct_duplicados
            FROM {T('ctl_ejecuciones')}
            WHERE capa IN ('bronze', 'silver') AND estado <> 'OMITIDO'
            GROUP BY run_id, fecha_corte, capa, dataset"""),

        Vista("vw_obs_calidad", "Resumen de reglas de calidad y postcondiciones por ejecución, capa y dataset", f"""
            SELECT run_id, fecha_corte, capa, dataset, max(fecha_evaluacion) AS fecha_evaluacion,
                   count(*) AS controles, count_if(resultado IN ('OK', 'OK_DENTRO_UMBRAL')) AS ok,
                   count_if(resultado = 'INFORMATIVA') AS informativas,
                   count_if(resultado = 'ADVERTENCIA') AS advertencias, count_if(resultado = 'FALLA') AS fallas,
                   sum(registros_fallidos) AS registros_con_hallazgos
            FROM {T('ctl_calidad_resultados')} GROUP BY run_id, fecha_corte, capa, dataset"""),

        Vista("vw_obs_frescura", "Frescura por fuente: dato más reciente frente a la fecha de corte publicada y al SLA", f"""
            WITH d AS ({frescura}),
                 corte AS (SELECT max(fecha_corte) AS fecha_corte FROM {T('ctl_publicaciones')}),
                 ingesta AS (SELECT dataset, max(fecha_registro) AS ultima_ingesta
                             FROM {T('ctl_archivos_procesados')} WHERE estado = 'INGESTADO' GROUP BY dataset)
            SELECT d.dataset, corte.fecha_corte AS ultima_fecha_corte_publicada, d.fecha_max_dato,
                   datediff(corte.fecha_corte, d.fecha_max_dato) AS rezago_dias_vs_corte, d.frescura_max_dias,
                   i.ultima_ingesta, datediff(current_date(), CAST(i.ultima_ingesta AS DATE)) AS dias_desde_ultima_ingesta,
                   d.ultima_actualizacion_silver,
                   CASE WHEN corte.fecha_corte IS NULL THEN 'SIN_PUBLICACION'
                        WHEN datediff(corte.fecha_corte, d.fecha_max_dato) > d.frescura_max_dias THEN 'ALERTA'
                        ELSE 'OK' END AS estado_frescura
            FROM d CROSS JOIN corte LEFT JOIN ingesta i ON i.dataset = d.dataset"""),

        Vista("vw_obs_esquemas", "Evolución del esquema recibido por archivo (columnas nuevas o faltantes)", f"""
            SELECT dataset, archivo, fecha_registro, run_id, size(columnas) AS num_columnas, columnas_nuevas,
                   columnas_faltantes, cambio_detectado, esquema_hash,
                   lag(esquema_hash) OVER (PARTITION BY dataset ORDER BY fecha_registro) AS esquema_hash_anterior
            FROM {T('ctl_esquemas')}"""),

        Vista("vw_obs_sla", "Cumplimiento del SLA de publicación (día 5, 08:00 Colombia) por corte", f"""
            SELECT fecha_corte, run_id, ambiente, modo, tipo_disparo, ts_publicacion, sla_objetivo, aplica_sla,
                   cumple_sla, timestampdiff(MINUTE, ts_publicacion, sla_objetivo) AS minutos_de_margen,
                   duracion_total_seg, frescura_dias, versiones_delta
            FROM {T('ctl_publicaciones')}"""),

        Vista("vw_obs_consumo", "Consumo ESTIMADO por ejecución (duración x DBU/hora x USD/DBU, supuestos en conf/observabilidad.yml)", f"""
            SELECT run_id, max(fecha_corte) AS fecha_corte, min(inicio) AS inicio,
                   round(sum(duracion_seg) / 3600, 4) AS horas_computo,
                   round(sum(duracion_seg) / 3600 * {c['dbu_por_hora_tarea']}, 4) AS dbu_estimados,
                   round(sum(duracion_seg) / 3600 * {c['dbu_por_hora_tarea']} * {c['usd_por_dbu']}, 4) AS usd_estimados
            FROM {T('ctl_ejecuciones')} GROUP BY run_id"""),

        Vista("vw_obs_cuarentena", "Registros en cuarentena por dataset, regla y estado de gestión", f"""
            SELECT dataset, regla_id, severidad, estado_gestion, count(*) AS registros,
                   min(fecha_cuarentena) AS primera_deteccion, max(ultima_deteccion_ts) AS ultima_deteccion
            FROM {T('silver_cuarentena')} GROUP BY dataset, regla_id, severidad, estado_gestion"""),
    ]


SQL_CONSUMO_REAL = """
    SELECT u.usage_metadata.job_run_id AS job_run_id, u.usage_metadata.job_id AS job_id,
           min(u.usage_start_time) AS inicio, max(u.usage_end_time) AS fin,
           sum(u.usage_quantity) AS dbus,
           sum(u.usage_quantity * p.pricing.effective_list.default) AS usd_lista
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices p
      ON p.sku_name = u.sku_name AND u.usage_start_time >= p.price_start_time
     AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
    WHERE u.usage_metadata.job_run_id IS NOT NULL
    GROUP BY 1, 2
"""


def crear_vistas(spark, cfg, logger) -> list[str]:
    """CREATE OR REPLACE VIEW de cada vista (idempotente). Devuelve los nombres creados."""
    conf = cargar_config_observabilidad()
    creadas = []
    for v in vistas(cfg.table, conf.get("consumo")):
        comentario = v.comentario.replace("'", "")
        spark.sql(f"CREATE OR REPLACE VIEW {cfg.table(v.nombre)} COMMENT '{comentario}' AS {v.sql}")
        creadas.append(v.nombre)
    try:  
        spark.sql("SELECT 1 FROM system.billing.usage LIMIT 1").collect()
        spark.sql(f"CREATE OR REPLACE VIEW {cfg.table('vw_obs_consumo_real')} "
                  f"COMMENT 'Consumo real por ejecución de job desde system.billing' AS {SQL_CONSUMO_REAL}")
        creadas.append("vw_obs_consumo_real")
    except Exception as exc: 
        logger.warning(f"system.billing no disponible ({type(exc).__name__}); se usa vw_obs_consumo (estimado)")
    logger.info(f"Vistas de observabilidad: {creadas}")
    return creadas
