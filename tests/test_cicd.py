"""Controles de CI/CD: compatibilidad de contratos, rollback y utilidades de ejecución."""

import copy
import logging

import pytest

from src.common.contract_compat import comparar_contratos, incremento_version, validar_cambio
from src.common.contracts import REPO_ROOT
from src.common.logging_utils import get_logger
from src.governance.aplicar import ejecutar_plan
from src.orchestration.rollback import RollbackError, plan_rollback


@pytest.fixture
def contrato():
    import yaml

    return yaml.safe_load((REPO_ROOT / "conf/contracts/polizas.yml").read_text(encoding="utf-8"))


def _cambiar(c, **kw):
    n = copy.deepcopy(c)
    for k, v in kw.items():
        n[k] = v
    return n


def test_sin_cambios_no_exige_version(contrato):
    assert comparar_contratos(contrato, copy.deepcopy(contrato)) == ("ninguno", [])
    assert validar_cambio(contrato, copy.deepcopy(contrato)) == []


def test_eliminar_columna_es_mayor_y_exige_version_mayor(contrato):
    nuevo = _cambiar(contrato, columnas=[c for c in contrato["columnas"] if c["nombre"] != "canal"])
    nivel, cambios = comparar_contratos(contrato, nuevo)
    assert nivel == "mayor" and any("columna eliminada: canal" in c for c in cambios)
    assert validar_cambio(contrato, _cambiar(nuevo, version="1.1.0"))  # menor no alcanza
    assert validar_cambio(contrato, _cambiar(nuevo, version="2.0.0")) == []


def test_cambios_compatibles_y_de_documentacion(contrato):
    con_col = _cambiar(contrato, columnas=contrato["columnas"] + [{"nombre": "descuento", "tipo": "decimal(18,2)"}])
    assert comparar_contratos(contrato, con_col)[0] == "menor"
    assert validar_cambio(contrato, _cambiar(con_col, version="1.0.1"))  # parche no alcanza
    assert validar_cambio(contrato, _cambiar(con_col, version="1.1.0")) == []

    doc = copy.deepcopy(contrato)
    doc["columnas"][0]["descripcion"] = "Otra descripción"
    assert comparar_contratos(contrato, doc)[0] == "parche"
    assert validar_cambio(contrato, doc)  # cualquier cambio exige subir versión


@pytest.mark.parametrize("mutacion, esperado", [
    (lambda c: c["columnas"][7].update(tipo="double"), "mayor"),                    # prima: tipo
    (lambda c: c["columnas"][9].update(requerido=True), "mayor"),                    # canal: pasa a requerida
    (lambda c: c["columnas"][6]["valores_permitidos"].remove("Renovada"), "mayor"),  # estado: dominio reducido
    (lambda c: c["columnas"][6]["valores_permitidos"].append("Suspendida"), "menor"),
    (lambda c: c.update(clave_negocio=["poliza_id", "cliente_id"]), "mayor"),
])
def test_clasificacion_de_cambios(contrato, mutacion, esperado):
    nuevo = copy.deepcopy(contrato)
    mutacion(nuevo)
    assert comparar_contratos(contrato, nuevo)[0] == esperado


def test_incremento_semver():
    assert incremento_version("1.0.0", "2.0.0") == "mayor"
    assert incremento_version("1.0.0", "1.1.0") == "menor"
    assert incremento_version("1.0.0", "1.0.1") == "parche"
    assert incremento_version("1.2.0", "1.1.9") == "ninguno"


def test_plan_rollback(contrato):
    plan = plan_rollback(lambda t: f"w.d.{t}", '{"gold_poliza_360": 7, "gold_discrepancias": 3}')
    assert plan == [("restaurar gold_discrepancias a v3", "RESTORE TABLE w.d.gold_discrepancias TO VERSION AS OF 3"),
                    ("restaurar gold_poliza_360 a v7", "RESTORE TABLE w.d.gold_poliza_360 TO VERSION AS OF 7")]
    with pytest.raises(RollbackError):
        plan_rollback(lambda t: t, {"silver_polizas": 2})  # solo productos Gold
    with pytest.raises(RollbackError):
        plan_rollback(lambda t: t, "{}")


def test_ejecutar_plan_continua_ante_errores():
    class FakeSpark:
        def __init__(self):
            self.ejecutadas = []

        def sql(self, s):
            if "FALLA" in s:
                raise RuntimeError("grupo inexistente\ndetalle largo")
            self.ejecutadas.append(s)

    spark, log = FakeSpark(), logging.getLogger("test")
    plan = [("a", "GRANT X TO `g`"), ("b", "GRANT FALLA"), ("c", "COMMENT ON TABLE t IS 'x'")]
    res = ejecutar_plan(spark, plan, "aplicar", log)
    assert [r["estado"] for r in res] == ["OK", "ERROR", "OK"] and len(spark.ejecutadas) == 2
    assert "grupo inexistente" in res[1]["detalle"] and "detalle largo" not in res[1]["detalle"]
    assert [r["estado"] for r in ejecutar_plan(FakeSpark(), plan, "plan", log)] == ["PLAN"] * 3


def test_logger_con_contexto_e_idempotente(capsys):
    log = get_logger("prueba_ctx", run_id="r-1", capa="silver", dataset="polizas")
    get_logger("prueba_ctx")  # no duplica handlers al reejecutar una celda
    log.info("mensaje de prueba")
    salida = capsys.readouterr().out
    assert salida.count("mensaje de prueba") == 1
    assert "run_id=r-1" in salida and "capa=silver" in salida and "dataset=polizas" in salida
