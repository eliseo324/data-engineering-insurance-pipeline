from __future__ import annotations

from collections.abc import Callable

from src.common.control import append_rows, utcnow


def consultas_alertas(T: Callable[[str], str], conf_alertas: dict[str, dict]) -> dict[str, str]:
    ultimo_run = f"(SELECT max_by(run_id, inicio) FROM {T('vw_obs_ejecuciones')})"
    factor = conf_alertas.get("OBS-07", {}).get("factor", 2.0)
    umbral_q = conf_alertas.get("OBS-08", {}).get("umbral_registros", 50)
    return {
        "OBS-01": f"""SELECT concat('run ', run_id, ' (corte ', fecha_corte, '): ', tareas_fallidas,
                             ' tarea(s) fallida(s). ', coalesce(ultimo_error, '')) AS detalle
                      FROM {T('vw_obs_ejecuciones')} WHERE run_id = {ultimo_run} AND estado_ejecucion = 'FALLIDO'""",
        "OBS-02": f"""SELECT concat('Corte ', fecha_corte, ' publicado ', -minutos_de_margen,
                             ' min después del SLA (', sla_objetivo, ' UTC)') AS detalle
                      FROM (SELECT * FROM {T('vw_obs_sla')} WHERE aplica_sla ORDER BY ts_publicacion DESC LIMIT 1)
                      WHERE NOT cumple_sla""",
        "OBS-03": f"""SELECT concat('No hay publicación para el corte ', last_day(add_months(current_date(), -1)),
                             ' y el SLA venció') AS detalle
                      WHERE current_timestamp() > make_timestamp(year(current_date()), month(current_date()), 5, 13, 0, 0)
                        AND NOT EXISTS (SELECT 1 FROM {T('vw_obs_sla')}
                                        WHERE fecha_corte = last_day(add_months(current_date(), -1)))""",
        "OBS-04": f"""SELECT concat(dataset, ': dato más reciente ', fecha_max_dato, ', rezago ', rezago_dias_vs_corte,
                             ' días frente al corte (máximo ', frescura_max_dias, ')') AS detalle
                      FROM {T('vw_obs_frescura')} WHERE estado_frescura = 'ALERTA'""",
        "OBS-05": f"""SELECT concat(capa, '.', dataset, ': ', advertencias, ' regla(s) en advertencia') AS detalle
                      FROM {T('vw_obs_calidad')} WHERE run_id = {ultimo_run} AND advertencias > 0""",
        "OBS-06": f"""SELECT concat(dataset, '/', archivo, ': nuevas=', to_json(columnas_nuevas),
                             ' faltantes=', to_json(columnas_faltantes)) AS detalle
                      FROM {T('vw_obs_esquemas')}
                      WHERE cambio_detectado AND fecha_registro >= current_timestamp() - INTERVAL 35 DAYS""",
        "OBS-07": f"""WITH r AS (SELECT run_id, inicio, duracion_seg FROM {T('vw_obs_ejecuciones')}),
                           u AS (SELECT * FROM r ORDER BY inicio DESC LIMIT 1),
                           h AS (SELECT percentile_approx(duracion_seg, 0.5) AS mediana, count(*) AS n
                                 FROM r WHERE run_id <> (SELECT run_id FROM u))
                      SELECT concat('run ', u.run_id, ': ', u.duracion_seg, ' s vs mediana ', h.mediana, ' s') AS detalle
                      FROM u CROSS JOIN h WHERE h.n >= 3 AND u.duracion_seg > {factor} * h.mediana""",
        "OBS-08": f"""SELECT concat(sum(registros), ' registros en cuarentena pendientes de gestión') AS detalle
                      FROM {T('vw_obs_cuarentena')} WHERE estado_gestion = 'PENDIENTE'
                      HAVING sum(registros) > {umbral_q}""",
    }


def evaluar_alertas(spark, cfg, conf: dict, logger, T: Callable[[str], str] | None = None) -> list[dict]:
    T = T or cfg.table
    definiciones = {a["id"]: a for a in conf["alertas"]}
    responsables = conf.get("responsables", {})
    ts = utcnow()
    filas = []
    for alerta_id, sql in consultas_alertas(T, definiciones).items():
        d = definiciones[alerta_id]
        if d.get("solo_ambientes") and cfg.ambiente not in d["solo_ambientes"]:
            continue
        for r in spark.sql(sql).collect():  
            filas.append({
                "run_id": cfg.run_id, "alerta_id": alerta_id, "nombre": d["nombre"], "severidad": d["severidad"],
                "responsable": responsables.get(d["responsable"], d["responsable"]), "descripcion": d["descripcion"],
                "detalle": r["detalle"], "fecha_evaluacion": ts, "estado": "ABIERTA",
            })
            (logger.error if d["severidad"] == "P1" else logger.warning)(
                f"ALERTA {alerta_id} [{d['severidad']}] {d['nombre']}: {r['detalle']}")
    if filas:
        append_rows(spark, cfg, "alertas", filas)
    else:
        logger.info("Sin alertas activas")
    return filas
