from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

from pyspark.sql import SparkSession

from src.common.contracts import cargar_contrato
from src.common.control import utcnow

ESTADOS_POLIZA = ("Vigente", "Proxima a vencer", "Vencida", "Renovada", "Cancelada", "No iniciada")


@dataclass(frozen=True)
class Postcondicion:
    id: str
    tabla: str
    dimension: str
    descripcion: str
    sql_violaciones: str
    severidad: str = "bloqueo" 


def postcondiciones(T: Callable[[str], str], corte: date) -> list[Postcondicion]:
    C = f"DATE'{corte.isoformat()}'"
    checks: list[Postcondicion] = []

    for ds, clave in [("polizas", "poliza_id"), ("recaudos", "recaudo_id"), ("siniestros", "siniestro_id"),
                      ("movimientos", "_event_key")]:
        checks.append(Postcondicion(
            f"silver_{ds}.unicidad_{clave}", f"silver_{ds}", "unicidad", f"{clave} es único en silver_{ds}",
            f"SELECT {clave} FROM {T(f'silver_{ds}')} GROUP BY {clave} HAVING count(*) > 1"))
        checks.append(Postcondicion(
            f"silver_{ds}.clave_no_nula", f"silver_{ds}", "completitud", f"{clave} nunca es nulo en silver_{ds}",
            f"SELECT * FROM {T(f'silver_{ds}')} WHERE {clave} IS NULL"))

    checks += [
        Postcondicion(
            "silver_clientes.scd2_una_version_actual", "silver_clientes", "unicidad",
            "Cada cliente tiene exactamente una versión actual",
            f"SELECT cliente_id FROM {T('silver_clientes')} GROUP BY cliente_id HAVING count_if(es_actual) <> 1"),
        Postcondicion(
            "silver_clientes.scd2_sin_solapes", "silver_clientes", "consistencia",
            "Las versiones de un cliente no se solapan en el tiempo",
            f"""SELECT a.cliente_id FROM {T('silver_clientes')} a JOIN {T('silver_clientes')} b
                ON a.cliente_id = b.cliente_id AND a.vigente_desde < b.vigente_desde
               AND (a.vigente_hasta IS NULL OR a.vigente_hasta > b.vigente_desde)"""),
    ]

    g360 = f"(SELECT * FROM {T('gold_poliza_360')} WHERE fecha_corte = {C})"
    checks += [
        Postcondicion("gold_poliza_360.grano", "gold_poliza_360", "unicidad", "Una fila por póliza en el corte",
                      f"SELECT poliza_id FROM {g360} GROUP BY poliza_id HAVING count(*) > 1"),
        Postcondicion("gold_poliza_360.completitud", "gold_poliza_360", "completitud",
                      "Toda póliza aceptada en Silver está publicada en el corte",
                      f"SELECT poliza_id FROM {T('silver_polizas')} EXCEPT SELECT poliza_id FROM {g360}"),
        Postcondicion("gold_poliza_360.estado_dominio", "gold_poliza_360", "validez",
                      "El estado al corte pertenece al dominio definido",
                      f"SELECT poliza_id FROM {g360} WHERE estado_al_corte IS NULL OR estado_al_corte NOT IN "
                      f"({', '.join(repr(e) for e in ESTADOS_POLIZA)})"),
        Postcondicion("gold_poliza_360.incurrido", "gold_poliza_360", "consistencia",
                      "costo_incurrido = valor pagado + reserva",
                      f"SELECT poliza_id FROM {g360} "
                      f"WHERE costo_incurrido <> valor_pagado_siniestros + valor_reserva_siniestros"),
        Postcondicion("gold_poliza_360.pendiente", "gold_poliza_360", "consistencia",
                      "prima_pendiente = prima emitida - prima recaudada",
                      f"SELECT poliza_id FROM {g360} WHERE prima_pendiente <> prima_emitida - prima_recaudada"),
    ]

    gm = f"(SELECT * FROM {T('gold_siniestralidad_mensual')} WHERE fecha_corte = {C})"
    checks += [
        Postcondicion("gold_siniestralidad_mensual.grano", "gold_siniestralidad_mensual", "unicidad",
                      "Una fila por periodo, producto y sucursal",
                      f"SELECT periodo, producto, sucursal FROM {gm} GROUP BY ALL HAVING count(*) > 1"),
        Postcondicion("gold_siniestralidad_mensual.no_negativos", "gold_siniestralidad_mensual", "validez",
                      "Exposición, prima devengada y razón de siniestralidad no son negativas",
                      f"SELECT * FROM {gm} WHERE polizas_expuestas < 0 OR prima_devengada < 0 "
                      f"OR razon_siniestralidad < 0"),
        Postcondicion("gold_siniestralidad_mensual.periodo_hasta_corte", "gold_siniestralidad_mensual",
                      "validez", "No hay periodos posteriores a la fecha de corte",
                      f"SELECT * FROM {gm} WHERE periodo > {C}"),
        Postcondicion("gold_siniestralidad_mensual.devengada_menor_emitida", "gold_siniestralidad_mensual",
                      "consistencia", "La prima devengada total no supera la prima emitida total",
                      f"SELECT 1 FROM (SELECT sum(prima_devengada) d, sum(prima_emitida) e FROM {gm}) WHERE d > e + 1"),
    ]

    for ds, col in [("polizas", "fecha_actualizacion"), ("recaudos", "fecha_pago"),
                    ("siniestros", "fecha_actualizacion"), ("movimientos", "fecha_evento")]:
        dias = int(cargar_contrato(ds).sla.get("frescura_max_dias", 5))
        checks.append(Postcondicion(
            f"silver_{ds}.frescura", f"silver_{ds}", "oportunidad",
            f"El dato más reciente de {ds} tiene a lo sumo {dias} días de antigüedad respecto al corte",
            f"SELECT max({col}) AS ultimo FROM {T(f'silver_{ds}')} "
            f"HAVING max({col}) IS NULL OR max({col}) < date_sub({C}, {dias})",
            severidad="advertencia"))
    return checks


def ejecutar_postcondiciones(
    spark: SparkSession, checks: list[Postcondicion], run_id: str, pipeline: str, corte: date, logger=None
) -> list[dict]:
    """Ejecuta cada postcondición y devuelve filas con el formato de ctl_calidad_resultados."""
    filas = []
    ts = utcnow()
    for chk in checks:
        n = spark.sql(f"SELECT count(*) FROM ({chk.sql_violaciones}) v").first()[0]
        resultado = "OK" if n == 0 else ("FALLA" if chk.severidad == "bloqueo" else "ADVERTENCIA")
        if n and logger:
            muestra = [r.asDict() for r in spark.sql(chk.sql_violaciones).limit(3).collect()]
            (logger.error if resultado == "FALLA" else logger.warning)(
                f"Postcondición {chk.id} [{chk.severidad}]: {n} violaciones. Muestra: {muestra}")
        filas.append({
            "run_id": run_id, "pipeline": pipeline, "fecha_corte": corte, "capa": "postcondicion",
            "dataset": chk.tabla, "regla_id": chk.id, "dimension": chk.dimension, "descripcion": chk.descripcion,
            "severidad": chk.severidad, "accion": "validar", "registros_evaluados": None,
            "registros_fallidos": int(n), "pct_fallo": None, "umbral_pct": 0.0, "resultado": resultado,
            "fecha_evaluacion": ts,
        })
    return filas
