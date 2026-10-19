from datetime import date

import pytest

from src.common.contracts import cargar_contrato
from src.quality.rules import (
    DataQualityError,
    Regla,
    RuleConfigError,
    aplicar_reglas,
    cargar_config_calidad,
    reglas_automaticas,
    resultado_regla,
    verificar_bloqueos,
)

CORTE = date(2026, 8, 31)


def test_reglas_automaticas_derivadas_del_contrato():
    ids = {r.id: r for r in reglas_automaticas(cargar_contrato("polizas"))}
    assert ids["polizas.COMP.poliza_id"].severidad == "bloqueo"
    assert ids["polizas.COMP.producto"].severidad == "advertencia"
    assert "polizas.COMP.canal" not in ids
    assert "polizas.FMT.prima" in ids and "polizas.FMT.fecha_inicio_vigencia" in ids
    assert "polizas.DOM.estado" in ids and "polizas.PAT.poliza_id" in ids
    assert "polizas.FMT.producto" not in ids


def test_config_combina_negocio_overrides_y_placeholders():
    cal = cargar_config_calidad(cargar_contrato("clientes"), CORTE)
    reglas = {r.id: r for r in cal.reglas}
    assert reglas["clientes.COMP.numero_documento"].accion == "marcar"
    assert "DATE'2026-08-31'" in reglas["clientes.OPO.fecha_actualizacion"].expresion
    assert cal.umbral_cuarentena_pct == 5.0


@pytest.mark.parametrize("dataset", ["clientes", "polizas", "recaudos", "siniestros", "movimientos"])
def test_yaml_de_reglas_valido_para_todos_los_datasets(dataset):
    cal = cargar_config_calidad(cargar_contrato(dataset), CORTE)
    assert all(r.id.startswith(f"{dataset}.") for r in cal.reglas)
    assert "${" not in " ".join(r.expresion for r in cal.reglas)


def test_regla_invalida_y_override_inexistente():
    with pytest.raises(RuleConfigError):
        Regla("x.y", "x", "validez", "d", "1=1", "critica", "cuarentena", 0)
    raw = {"datasets": {"polizas": {"overrides": {"polizas.NO.existe": {"accion": "marcar"}}}}}
    with pytest.raises(RuleConfigError, match="inexistente"):
        cargar_config_calidad(cargar_contrato("polizas"), CORTE, raw=raw)


def test_aplicar_reglas_null_se_considera_cumplida(spark):
    reglas = [
        Regla("t.VAL.prima", "t", "validez", "prima > 0", "prima > 0", "advertencia", "cuarentena", 1),
        Regla("t.CON.x", "t", "consistencia", "fin >= ini", "fin >= ini", "advertencia", "marcar", 1),
    ]
    df = spark.createDataFrame([(10, 2, 1), (-5, 1, 2), (None, None, 1)], "prima INT, fin INT, ini INT")
    rows = aplicar_reglas(df, reglas).orderBy("ini", "prima").collect()
    assert [(list(r["_dq_cuarentena"]), list(r["_dq_advertencias"])) for r in rows] == [
        ([], []), ([], []), (["t.VAL.prima"], ["t.CON.x"])]


def test_resultado_segun_severidad_y_umbral():
    adv = Regla("a.b", "a", "validez", "d", "1=1", "advertencia", "cuarentena", 2.0)
    blo = Regla("a.c", "a", "completitud", "d", "1=1", "bloqueo", "cuarentena", 1.0)
    inf = Regla("a.d", "a", "consistencia", "d", "1=1", "informativa", "marcar", 0)
    assert resultado_regla(adv, 0, 100) == (0.0, "OK")
    assert resultado_regla(adv, 2, 100) == (2.0, "OK_DENTRO_UMBRAL")
    assert resultado_regla(adv, 3, 100)[1] == "ADVERTENCIA"
    assert resultado_regla(blo, 2, 100)[1] == "FALLA"
    assert resultado_regla(inf, 90, 100)[1] == "INFORMATIVA"


def test_bloqueo_detiene_el_pipeline():
    verificar_bloqueos([{"regla_id": "a", "resultado": "ADVERTENCIA"}], "polizas")
    with pytest.raises(DataQualityError, match="polizas.COMP.poliza_id"):
        verificar_bloqueos([{"regla_id": "polizas.COMP.poliza_id", "resultado": "FALLA", "pct_fallo": 3.0,
                             "umbral_pct": 1.0}], "polizas")
