from datetime import date, datetime

import pytest

from src.common.config import PipelineConfig
from src.common.control import _schema_ddl
from src.observability.alerts import consultas_alertas, evaluar_alertas
from src.observability.views import cargar_config_observabilidad, vistas

CORTE = date(2026, 8, 31)


def _ejec(run, tarea, capa, ds, estado, ini, seg, ent=None, sal=None, rech=None, dup=None, msg=None):
    return {"run_id": run, "pipeline": "p", "tarea": tarea, "capa": capa, "dataset": ds, "fecha_corte": CORTE,
            "modo": "normal", "ambiente": "dev", "estado": estado, "inicio": ini,
            "fin": datetime.fromtimestamp(ini.timestamp() + seg), "duracion_seg": float(seg),
            "registros_entrada": ent, "registros_salida": sal, "registros_rechazados": rech,
            "registros_duplicados": dup, "mensaje": msg, "detalle": None, "usuario": "u"}


@pytest.fixture
def entorno(spark):
    def tabla(nombre, filas, esquema):
        spark.createDataFrame(filas, esquema).createOrReplaceTempView(nombre)

    t0, t1 = datetime(2026, 9, 5, 11, 0), datetime(2026, 10, 5, 11, 0)
    tabla("ctl_ejecuciones", [
        _ejec("r1", "bronze_polizas", "bronze", "polizas", "EXITOSO", t0, 30, 1502, 1502),
        _ejec("r1", "silver_polizas", "silver", "polizas", "EXITOSO", t0.replace(minute=1), 60, 1502, 1486, 15, 1),
        _ejec("r2", "bronze_polizas", "bronze", "polizas", "EXITOSO", t1, 30, 200, 200),
        _ejec("r2", "silver_polizas", "silver", "polizas", "FALLIDO", t1.replace(minute=1), 20, msg="DataQualityError: x"),
        _ejec("r3", "silver_polizas", "silver", "polizas", "FALLIDO", datetime(2026, 8, 1, 11), 20, msg="Timeout"),
        _ejec("r3", "silver_polizas", "silver", "polizas", "EXITOSO", datetime(2026, 8, 1, 12), 40, 10, 10),
    ], _schema_ddl("ejecuciones"))
    tabla("ctl_publicaciones", [{
        "run_id": "r1", "pipeline": "p", "fecha_corte": CORTE, "ambiente": "dev", "modo": "normal",
        "tipo_disparo": "PERIODIC", "estado": "PUBLICADO", "inicio_ejecucion": t0, "ts_publicacion": datetime(2026, 9, 5, 14),
        "duracion_total_seg": 90.0, "sla_objetivo": datetime(2026, 9, 5, 13), "aplica_sla": True, "cumple_sla": False,
        "fecha_max_datos": date(2026, 8, 20), "frescura_dias": 11, "versiones_delta": "{}", "usuario": None,
    }], _schema_ddl("publicaciones"))
    tabla("ctl_calidad_resultados", [{
        "run_id": "r2", "pipeline": "p", "fecha_corte": CORTE, "capa": "silver", "dataset": "polizas",
        "regla_id": "polizas.COMP.producto", "dimension": "completitud", "descripcion": "d", "severidad": "advertencia",
        "accion": "cuarentena", "registros_evaluados": 200, "registros_fallidos": 10, "pct_fallo": 5.0,
        "umbral_pct": 2.0, "resultado": "ADVERTENCIA", "fecha_evaluacion": t1}], _schema_ddl("calidad_resultados"))
    tabla("ctl_esquemas", [{
        "dataset": "polizas", "archivo": "p2.csv", "hash_archivo": "h", "esquema_hash": "e2", "columnas": ["a", "b", "c"],
        "columnas_nuevas": ["c"], "columnas_faltantes": [], "cambio_detectado": True, "run_id": "r2",
        "fecha_registro": datetime.now()}], _schema_ddl("esquemas"))
    tabla("ctl_archivos_procesados", [{
        "dataset": "polizas", "archivo": "p.csv", "ruta": "/x", "hash_archivo": "h", "tamano_bytes": 1,
        "fecha_modificacion": t0, "esquema_hash": "e", "columnas": ["a"], "registros": 1502, "estado": "INGESTADO",
        "mensaje": None, "run_id": "r1", "fecha_corte": CORTE, "fecha_registro": t0}], _schema_ddl("archivos_procesados"))
    for ds, col, fecha in [("clientes", "fecha_actualizacion", date(2026, 8, 30)),
                           ("polizas", "fecha_actualizacion", date(2026, 8, 31)),
                           ("recaudos", "fecha_pago", date(2026, 8, 1)), 
                           ("siniestros", "fecha_actualizacion", date(2026, 8, 29)),
                           ("movimientos", "fecha_evento", date(2026, 8, 31))]:
        tabla(f"silver_{ds}", [(fecha, t0)], f"{col} DATE, _silver_ts TIMESTAMP")
    tabla("silver_cuarentena", [("recaudos", "recaudos.RI.poliza", "advertencia", "PENDIENTE", t0, t0)] * 60,
          "dataset STRING, regla_id STRING, severidad STRING, estado_gestion STRING, fecha_cuarentena TIMESTAMP, "
          "ultima_deteccion_ts TIMESTAMP")
    conf = cargar_config_observabilidad()
    for v in vistas(lambda t: t, conf["consumo"]):
        spark.sql(v.sql).createOrReplaceTempView(v.nombre)
    return conf


def test_vistas_de_ejecucion_volumen_y_consumo(spark, entorno):
    ej = {r["run_id"]: r for r in spark.table("vw_obs_ejecuciones").collect()}
    assert ej["r1"]["estado_ejecucion"] == "PUBLICADO" and ej["r1"]["cumple_sla"] is False
    assert ej["r2"]["estado_ejecucion"] == "FALLIDO" and ej["r2"]["tareas_fallidas"] == 1
    assert "DataQualityError" in ej["r2"]["ultimo_error"]
    assert ej["r3"]["tareas_fallidas"] == 0 and ej["r3"]["reintentos"] == 1
    assert ej["r3"]["estado_ejecucion"] == "PARCIAL"  
    vol = spark.table("vw_obs_volumenes").where("run_id = 'r1' AND capa = 'silver'").first()
    assert (vol["entrada"], vol["salida"], vol["rechazados"], vol["duplicados"]) == (1502, 1486, 15, 1)
    assert vol["pct_rechazo"] == pytest.approx(0.999, abs=1e-3)
    consumo = spark.table("vw_obs_consumo").where("run_id = 'r1'").first()
    assert consumo["horas_computo"] == pytest.approx(0.025)  
    assert consumo["usd_estimados"] == pytest.approx(0.025 * 2.0 * 0.35, abs=1e-4)


def test_vistas_de_frescura_esquema_y_sla(spark, entorno):
    fr = {r["dataset"]: r for r in spark.table("vw_obs_frescura").collect()}
    assert fr["recaudos"]["estado_frescura"] == "ALERTA" and fr["recaudos"]["rezago_dias_vs_corte"] == 30
    assert fr["polizas"]["estado_frescura"] == "OK"
    sla = spark.table("vw_obs_sla").first()
    assert sla["minutos_de_margen"] == -60  
    assert spark.table("vw_obs_esquemas").first()["cambio_detectado"] is True


def test_alertas_disparadas(spark, entorno, monkeypatch):
    import src.observability.alerts as mod

    capturadas = []
    monkeypatch.setattr(mod, "append_rows", lambda spark, cfg, name, rows: capturadas.extend(rows))

    class Log:
        def __getattr__(self, _):
            return lambda *a, **k: None

    cfg = PipelineConfig(fecha_corte=CORTE, run_id="obs-test", ambiente="dev")
    filas = evaluar_alertas(spark, cfg, entorno, Log(), T=lambda t: t)
    ids = {f["alerta_id"] for f in filas}
    assert {"OBS-01", "OBS-02", "OBS-04", "OBS-05", "OBS-06", "OBS-08"} <= ids
    assert "OBS-03" not in ids  
    assert "OBS-07" not in ids  
    assert capturadas == filas
    sla = next(f for f in filas if f["alerta_id"] == "OBS-02")
    assert sla["severidad"] == "P1" and "60 min" in sla["detalle"]
    spark.createDataFrame(filas, schema=_schema_ddl("alertas")).count()


def test_todas_las_alertas_tienen_definicion():
    conf = cargar_config_observabilidad()
    definidas = {a["id"] for a in conf["alertas"]}
    assert set(consultas_alertas(lambda t: t, {a["id"]: a for a in conf["alertas"]})) == definidas
    assert all(a["responsable"] in conf["responsables"] for a in conf["alertas"])
