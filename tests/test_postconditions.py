from datetime import date
from decimal import Decimal as D

import pytest

from src.quality.postconditions import ejecutar_postcondiciones, postcondiciones

CORTE = date(2026, 8, 31)


@pytest.fixture
def tablas(spark):
    def vista(nombre, filas, esquema):
        spark.createDataFrame(filas, esquema).createOrReplaceTempView(nombre)

    vista("silver_polizas", [("P1", date(2026, 8, 30)), ("P2", date(2026, 8, 1))],
          "poliza_id STRING, fecha_actualizacion DATE")
    vista("silver_recaudos", [("R1", date(2026, 8, 31))], "recaudo_id STRING, fecha_pago DATE")
    vista("silver_siniestros", [("S1", date(2026, 8, 29))], "siniestro_id STRING, fecha_actualizacion DATE")
    vista("silver_movimientos", [("k1", date(2026, 8, 31))], "_event_key STRING, fecha_evento DATE")
    vista("silver_clientes", [("C1", date(2024, 1, 1), date(2026, 6, 1), False), ("C1", date(2026, 6, 1), None, True)],
          "cliente_id STRING, vigente_desde DATE, vigente_hasta DATE, es_actual BOOLEAN")
    p360 = ("fecha_corte DATE, poliza_id STRING, estado_al_corte STRING, costo_incurrido DECIMAL(18,2), "
            "valor_pagado_siniestros DECIMAL(18,2), valor_reserva_siniestros DECIMAL(18,2), "
            "prima_pendiente DECIMAL(18,2), prima_emitida DECIMAL(18,2), prima_recaudada DECIMAL(18,2)")
    vista("gold_poliza_360", [
        (CORTE, "P1", "Vigente", D("30"), D("10"), D("20"), D("60"), D("100"), D("40")),
        (CORTE, "P2", "Vencida", D("0"), D("0"), D("0"), D("50"), D("50"), D("0")),
    ], p360)
    vista("gold_siniestralidad_mensual", [(CORTE, date(2026, 8, 1), "Autos", "Cali", 2, D("12"), D("150"), 0.5)],
          "fecha_corte DATE, periodo DATE, producto STRING, sucursal STRING, polizas_expuestas INT, "
          "prima_devengada DECIMAL(18,2), prima_emitida DECIMAL(18,2), razon_siniestralidad DOUBLE")
    return vista


def _resultado(spark):
    filas = ejecutar_postcondiciones(spark, postcondiciones(lambda t: t, CORTE), "r", "p", CORTE)
    return {f["regla_id"]: (f["resultado"], f["registros_fallidos"]) for f in filas}


def test_tablas_correctas_cumplen_todas_las_postcondiciones(spark, tablas):
    res = _resultado(spark)
    assert len(res) >= 20
    assert {k: v for k, v in res.items() if v[0] != "OK"} == {}


def test_violaciones_se_detectan_con_su_severidad(spark, tablas):
    tablas("gold_poliza_360", [(CORTE, "P1", "Vigente", D("30"), D("10"), D("20"), D("60"), D("100"), D("40"))] * 2,
           "fecha_corte DATE, poliza_id STRING, estado_al_corte STRING, costo_incurrido DECIMAL(18,2), "
           "valor_pagado_siniestros DECIMAL(18,2), valor_reserva_siniestros DECIMAL(18,2), "
           "prima_pendiente DECIMAL(18,2), prima_emitida DECIMAL(18,2), prima_recaudada DECIMAL(18,2)")
    tablas("silver_clientes", [("C1", date(2024, 1, 1), None, True), ("C1", date(2026, 6, 1), None, True)],
           "cliente_id STRING, vigente_desde DATE, vigente_hasta DATE, es_actual BOOLEAN")
    tablas("silver_recaudos", [("R1", date(2026, 7, 1))], "recaudo_id STRING, fecha_pago DATE")
    res = _resultado(spark)
    assert res["gold_poliza_360.grano"] == ("FALLA", 1)
    assert res["gold_poliza_360.completitud"] == ("FALLA", 1)  
    assert res["silver_clientes.scd2_una_version_actual"][0] == "FALLA"
    assert res["silver_clientes.scd2_sin_solapes"][0] == "FALLA"
    assert res["silver_recaudos.frescura"] == ("ADVERTENCIA", 1)
