import copy
import csv
from datetime import date, datetime

import pytest

from src.orchestration.job_spec import JobSpecError, ancestros, cargar_spec_job, orden_topologico, validar_dag
from src.orchestration.publicacion import evaluar_sla, sla_objetivo
from src.orchestration.simulacion_fallas import generar_claves_nulas, generar_esquema_invalido

VARS = {"ambiente": "dev", "catalog": "workspace", "schema": "default", "volume": "volume1", "pause_status": "UNPAUSED"}
USER = "yo@ejemplo.com"
ROOT = "/Workspace/Users/yo@ejemplo.com/data-engineering-insurance-pipeline"


@pytest.fixture(scope="module")
def spec():
    return cargar_spec_job(VARS, USER, ROOT)


def test_job_programado_dia_5_a_las_6_hora_colombia(spec):
    assert spec["schedule"] == {"quartz_cron_expression": "0 0 6 5 * ?", "timezone_id": "America/Bogota",
                                "pause_status": "UNPAUSED"}
    assert spec["name"] == "insurance-medallion-dev"
    assert spec["max_concurrent_runs"] == 1 
    assert spec["email_notifications"]["on_failure"] == [USER]


def test_parametros_y_rutas_resueltas(spec):
    params = {p["name"]: p["default"] for p in spec["parameters"]}
    assert params["modo"] == "normal" and params["fecha_corte"] == ""
    assert params["job_run_id"] == "{{job.run_id}}" 
    assert params["catalog"] == "workspace"
    t = {x["task_key"]: x for x in spec["tasks"]}
    nb = t["silver_polizas"]["notebook_task"]
    assert nb["notebook_path"] == "/Users/yo@ejemplo.com/data-engineering-insurance-pipeline/notebooks/20_silver"
    assert nb["source"] == "WORKSPACE" and nb["base_parameters"] == {"dataset": "polizas"}


def test_dag_respeta_dependencias_de_datos(spec):
    niveles = orden_topologico(spec["tasks"])
    assert niveles[0] == ["preparar_entorno"]
    assert set(niveles[1]) == {f"bronze_{d}" for d in ("clientes", "polizas", "recaudos", "siniestros", "movimientos")}
    assert niveles[-3:] == [["publicacion"], ["gobierno"], ["observabilidad"]]
    obs = next(t for t in spec["tasks"] if t["task_key"] == "observabilidad")
    assert obs["run_if"] == "ALL_DONE" 
    assert {"bronze_movimientos", "silver_recaudos", "silver_siniestros"} <= ancestros(spec["tasks"], "silver_movimientos")
    assert "validaciones" in ancestros(spec["tasks"], "publicacion")
    assert max(len(n) for n in niveles) <= 5  


def test_validacion_detecta_errores_de_definicion(spec):
    ciclo = copy.deepcopy(spec)
    next(t for t in ciclo["tasks"] if t["task_key"] == "preparar_entorno")["depends_on"] = [{"task_key": "publicacion"}]
    with pytest.raises(JobSpecError, match="Ciclo"):
        validar_dag(ciclo)
    sin_orden = copy.deepcopy(spec)
    next(t for t in sin_orden["tasks"] if t["task_key"] == "silver_recaudos")["depends_on"] = [
        {"task_key": "bronze_recaudos"}]
    with pytest.raises(JobSpecError, match="silver_recaudos debe ejecutarse después"):
        validar_dag(sin_orden)
    cron = copy.deepcopy(spec)
    cron["schedule"]["timezone_id"] = "UTC"
    with pytest.raises(JobSpecError, match="schedule"):
        validar_dag(cron)


def test_variable_sin_valor_falla():
    with pytest.raises(JobSpecError, match="var.catalog"):
        cargar_spec_job({k: v for k, v in VARS.items() if k != "catalog"}, USER, ROOT)


def test_sla_dia_5_a_las_8_hora_colombia():
    assert sla_objetivo(date(2026, 8, 31)) == datetime(2026, 9, 5, 13, 0)
    assert sla_objetivo(date(2026, 12, 31)) == datetime(2027, 1, 5, 13, 0)
    a_tiempo = evaluar_sla(date(2026, 8, 31), datetime(2026, 9, 5, 11, 40), "PERIODIC")
    tarde = evaluar_sla(date(2026, 8, 31), datetime(2026, 9, 5, 14, 0), "PERIODIC")
    manual = evaluar_sla(date(2026, 8, 31), datetime(2026, 10, 19, 5, 0), "ONE_TIME")
    assert a_tiempo["cumple_sla"] is True and tarde["cumple_sla"] is False
    assert manual["aplica_sla"] is False and manual["cumple_sla"] is None  


def _csv(path, filas):
    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        csv.writer(fh, delimiter=";").writerows(filas)


def test_generadores_de_falla(tmp_path):
    origen = tmp_path / "polizas.csv"
    _csv(origen, [["poliza_id", "cliente_id", "prima"]] + [[f"POL{i:07d}", "CLI000001", "10.00"] for i in range(10)])
    n = generar_esquema_invalido(str(origen), str(tmp_path / "esq.csv"), n=5)
    with open(tmp_path / "esq.csv", encoding="utf-8") as fh:
        filas = list(csv.reader(fh, delimiter=";"))
    assert n == 5 and filas[0] == ["poliza_id", "cliente_id"]  

    generar_claves_nulas(str(origen), str(tmp_path / "cal.csv"), pct=0.2, n=10)
    with open(tmp_path / "cal.csv", encoding="utf-8") as fh:
        filas = list(csv.reader(fh, delimiter=";"))[1:]
    assert sum(1 for f in filas if f[0] == "") == 2 


def test_spec_aceptada_por_el_modelo_oficial_de_la_api(spec):
    jobs = pytest.importorskip("databricks.sdk.service.jobs")

    def claves(d, p=""):
        if isinstance(d, dict):
            return {p + k for k in d} | {x for k, v in d.items() for x in claves(v, f"{p}{k}.")}
        if isinstance(d, list):
            return {x for v in d for x in claves(v, f"{p}[].")}
        return set()

    ida_vuelta = jobs.JobSettings.from_dict(spec).as_dict()
    assert claves(spec) - claves(ida_vuelta) == set()