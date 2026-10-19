from datetime import date, datetime
from decimal import Decimal

import pytest

from src.common.config import PipelineConfig
from src.common.control import _schema_ddl
from src.quality.reconciliation import reconciliar_gold
from src.transformations.gold import (
    build_calidad_pipeline,
    build_discrepancias,
    build_poliza_360,
    build_siniestralidad_mensual,
    clientes_al_corte,
    estado_al_corte,
    exposicion_mensual,
    polizas_con_fin_efectivo,
)

D = Decimal
POL = ("poliza_id STRING, cliente_id STRING, producto STRING, sucursal STRING, canal STRING, "
       "fecha_inicio_vigencia DATE, fecha_fin_vigencia DATE, estado STRING, prima DECIMAL(18,2), "
       "fecha_actualizacion DATE, _ref_cliente BOOLEAN, _dq_advertencias ARRAY<STRING>, _silver_run_id STRING")
CLI = ("cliente_id STRING, segmento STRING, ciudad STRING, tipo_documento STRING, vigente_desde DATE, "
       "vigente_hasta DATE, es_actual BOOLEAN")
REC = ("recaudo_id STRING, poliza_id STRING, fecha_pago DATE, valor DECIMAL(18,2), estado_transaccion STRING, "
       "_ref_poliza_cuarentena BOOLEAN, _dq_advertencias ARRAY<STRING>")
SIN = ("siniestro_id STRING, poliza_id STRING, fecha_ocurrencia DATE, estado STRING, valor_pagado DECIMAL(18,2), "
       "valor_reserva DECIMAL(18,2), fecha_actualizacion DATE, _ref_poliza_cuarentena BOOLEAN, "
       "_dq_advertencias ARRAY<STRING>")
MOV = "evento_id STRING, entidad STRING, id_entidad STRING, tipo_evento STRING, fecha_evento DATE, secuencia INT"
QUA = "dataset STRING, reglas_incumplidas ARRAY<STRING>, registro STRING"


@pytest.fixture
def silver(spark):
    pol = spark.createDataFrame([
        ("P1", "C1", "Autos", "Cali", "Directo", date(2026, 1, 1), date(2026, 12, 31), "Vigente", D("365.00"),
         date(2026, 1, 1), True, [], "s1"),
        ("P2", "C1", "Autos", "Cali", "Directo", date(2026, 1, 1), date(2026, 9, 15), "Vigente", D("100.00"),
         date(2026, 1, 1), True, [], "s1"),  
        ("P3", "CX", "Vida", "Costa", "Asesor", date(2025, 1, 1), date(2025, 12, 31), "Cancelada", D("200.00"),
         date(2025, 6, 1), False, ["polizas.RI.cliente"], "s1"), 
        ("P4", "C2", "Vida", "Costa", "Asesor", date(2024, 1, 1), date(2024, 12, 31), "Renovada", D("50.00"),
         date(2024, 1, 1), True, [], "s1"),
    ], POL)
    cli = spark.createDataFrame([
        ("C1", "Personas", "Cali", "CC", date(2024, 1, 1), date(2026, 6, 1), False),
        ("C1", "Pyme", "Cali", "CC", date(2026, 6, 1), None, True), 
        ("C2", "Corporativo", "Bogota", "NIT", date(2024, 1, 1), None, True),
    ], CLI)
    rec = spark.createDataFrame([
        ("R1", "P1", date(2026, 2, 1), D("100.00"), "Aplicado", False, []),
        ("R2", "P1", date(2026, 3, 1), D("50.00"), "Pendiente", False, []),
        ("R3", "P1", date(2026, 3, 5), D("30.00"), "Reversado", False, []),
        ("R4", "P2", date(2026, 9, 10), D("999.00"), "Aplicado", False, []), 
        ("R5", "P2", date(2026, 1, 15), D("150.00"), "Aplicado", False, []), 
    ], REC)
    sin = spark.createDataFrame([
        ("S1", "P1", date(2026, 2, 10), "Abierto", D("10.00"), D("20.00"), date(2026, 8, 1), False, []),
        ("S2", "P1", date(2026, 3, 1), "Cerrado", D("5.00"), D("0.00"), date(2026, 3, 9), False, []),
        ("S3", "P3", date(2025, 3, 1), "Cerrado", D("7.00"), D("0.00"), date(2025, 3, 2), False,
         ["siniestros.CON.ocurrencia_en_vigencia"]),
    ], SIN)
    mov = spark.createDataFrame([
        ("E1", "POLIZA", "P1", "CAMBIO_ESTADO", date(2026, 8, 20), 1),
        ("E2", "POLIZA", "P3", "CANCELACION", date(2025, 3, 31), 2),
        ("E3", "POLIZA", "P4", "CAMBIO_ESTADO", date(2026, 2, 1), 3), 
    ], MOV)
    qua = spark.createDataFrame([
        ("recaudos", ["recaudos.RI.poliza"], '{"recaudo_id":"R9","poliza_id":"POL_ORF1","valor":"12.50","fecha_pago":"05/03/2026"}'),
    ], QUA)
    return {"polizas": pol, "clientes": cli, "recaudos": rec, "siniestros": sin, "movimientos": mov, "cuarentena": qua}


CORTE = date(2026, 8, 31)


def test_estado_al_corte(spark, silver):
    pol = polizas_con_fin_efectivo(silver["polizas"], silver["movimientos"])
    est = {r["poliza_id"]: r["e"] for r in pol.select("poliza_id", estado_al_corte(CORTE, 30).alias("e")).collect()}
    assert est == {"P1": "Vigente", "P2": "Proxima a vencer", "P3": "Cancelada", "P4": "Renovada"}
    est_pasado = {r["poliza_id"]: r["e"] for r in pol.select("poliza_id", estado_al_corte(date(2025, 2, 28), 30)
                                                              .alias("e")).collect()}
    assert est_pasado["P3"] == "Vigente" and est_pasado["P1"] == "No iniciada"


def test_cancelacion_usa_primer_evento_en_vigencia(spark, silver):
    r = {x["poliza_id"]: x for x in polizas_con_fin_efectivo(silver["polizas"], silver["movimientos"]).collect()}
    assert r["P3"]["fecha_cancelacion"] == date(2025, 3, 31)
    assert r["P3"]["fecha_fin_efectiva"] == date(2025, 3, 31)
    assert r["P1"]["fecha_cancelacion"] is None and r["P1"]["fecha_fin_efectiva"] == date(2026, 12, 31)


def test_prima_devengada_proporcional_a_los_dias(spark, silver):
    pol = polizas_con_fin_efectivo(silver["polizas"].where("poliza_id = 'P1'"), silver["movimientos"])
    meses = {r["periodo"]: r for r in exposicion_mensual(pol, date(2026, 3, 31)).collect()}
    assert sorted(meses) == [date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1)] 
    assert meses[date(2026, 2, 1)]["dias_expuestos"] == 28
    assert float(meses[date(2026, 2, 1)]["prima_devengada"]) == pytest.approx(28.0)


def test_clientes_scd2_al_corte(spark, silver):
    seg = {r["cliente_id"]: r["segmento"] for r in clientes_al_corte(silver["clientes"], date(2026, 3, 31)).collect()}
    assert seg["C1"] == "Personas" 
    seg2 = {r["cliente_id"]: r["segmento"] for r in clientes_al_corte(silver["clientes"], CORTE).collect()}
    assert seg2["C1"] == "Pyme"


def test_poliza_360(spark, silver):
    s = silver
    out = {r["poliza_id"]: r for r in build_poliza_360(
        s["polizas"], s["clientes"], s["recaudos"], s["siniestros"], s["movimientos"], CORTE).collect()}
    assert len(out) == 4  
    p1 = out["P1"]
    assert p1["prima_recaudada"] == D("100.00") 
    assert p1["recaudo_pendiente_confirmar"] == D("50.00") and p1["recaudo_reversado"] == D("30.00")
    assert p1["prima_pendiente"] == D("265.00")
    assert p1["num_siniestros"] == 2 and p1["num_siniestros_abiertos"] == 1
    assert p1["costo_incurrido"] == D("35.00")
    assert p1["fecha_ultima_actividad"] == date(2026, 8, 20)
    assert p1["tipo_ultima_actividad"] == "MOVIMIENTO CAMBIO_ESTADO"
    assert p1["segmento_cliente"] == "Pyme"
    assert out["P2"]["prima_recaudada"] == D("150.00") 
    assert out["P3"]["segmento_cliente"] == "NO_IDENTIFICADO" and not out["P3"]["cliente_identificado"]
    assert out["P3"]["num_siniestros_fuera_vigencia"] == 1


def test_siniestralidad_mensual(spark, silver):
    s = silver
    m = build_siniestralidad_mensual(s["polizas"], s["siniestros"], s["recaudos"], s["movimientos"], CORTE)
    feb = m.where("periodo = DATE'2026-02-01' AND producto = 'Autos'").first()
    assert feb["polizas_expuestas"] == 2
    assert feb["siniestros"] == 1 and feb["costo_incurrido"] == D("30.00")
    assert feb["razon_siniestralidad"] == pytest.approx(30.0 / float(feb["prima_devengada"]), rel=1e-3)
    ene = m.where("periodo = DATE'2026-01-01' AND producto = 'Autos'").first()
    assert ene["prima_emitida"] == D("465.00") and ene["prima_recaudada"] == D("150.00")
    assert ene["prima_pendiente_acumulada"] == D("315.00")
    assert m.select("periodo", "producto", "sucursal").distinct().count() == m.count() 


def test_discrepancias(spark, silver):
    s = silver
    p360 = build_poliza_360(s["polizas"], s["clientes"], s["recaudos"], s["siniestros"], s["movimientos"], CORTE)
    d = build_discrepancias(s["polizas"], s["recaudos"], s["siniestros"], s["movimientos"], s["cuarentena"], p360, CORTE)
    tipos = {(r["tipo_discrepancia"], r["id_registro"]): r for r in d.collect()}
    pago = tipos[("PAGO_SIN_POLIZA", "R9")]
    assert pago["poliza_id"] == "POL_ORF1" and pago["valor"] == D("12.50") and pago["fecha_referencia"] == date(2026, 3, 5)
    assert ("SINIESTRO_FUERA_DE_VIGENCIA", "S3") in tipos
    assert ("MOVIMIENTO_FUERA_DE_VIGENCIA", "E3") in tipos
    assert ("POLIZA_SIN_CLIENTE", "P3") in tipos
    assert ("RECAUDO_EXCEDE_PRIMA", "P2") in tipos


def test_reconciliacion_silver_gold_cuadra(spark, silver):
    s = silver
    cfg = PipelineConfig(fecha_corte=CORTE, run_id="t")
    p360 = build_poliza_360(s["polizas"], s["clientes"], s["recaudos"], s["siniestros"], s["movimientos"], CORTE)
    m = build_siniestralidad_mensual(s["polizas"], s["siniestros"], s["recaudos"], s["movimientos"], CORTE)
    filas = reconciliar_gold(cfg, s["polizas"], s["recaudos"], s["siniestros"], p360, m, datetime(2026, 9, 5))
    assert [f["resultado"] for f in filas] == ["OK"] * len(filas), filas
    spark.createDataFrame(filas, schema=_schema_ddl("reconciliacion")).count()


def test_calidad_pipeline_marca_ultima_ejecucion(spark):
    base = {"pipeline": "p", "fecha_corte": CORTE, "capa": "silver", "dataset": "polizas", "dimension": "validez",
            "descripcion": "d", "severidad": "advertencia", "accion": "cuarentena", "registros_evaluados": 10,
            "registros_fallidos": 1, "pct_fallo": 10.0, "umbral_pct": 1.0, "resultado": "ADVERTENCIA"}
    cal = spark.createDataFrame([
        {**base, "run_id": "r1", "regla_id": "polizas.X", "fecha_evaluacion": datetime(2026, 9, 5, 6)},
        {**base, "run_id": "r2", "regla_id": "polizas.X", "fecha_evaluacion": datetime(2026, 10, 5, 6)},
    ], schema=_schema_ddl("calidad_resultados"))
    rec = spark.createDataFrame([], schema=_schema_ddl("reconciliacion"))
    out = {r["run_id"]: r["es_ultima_ejecucion"] for r in build_calidad_pipeline(cal, rec).collect()}
    assert out == {"r1": False, "r2": True}
