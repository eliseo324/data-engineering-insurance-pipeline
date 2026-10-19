from datetime import date, datetime

from src.common.config import PipelineConfig
from src.common.contracts import cargar_contrato
from src.common.control import _schema_ddl
from src.common.hashing import row_hash
from src.quality.reconciliation import medir_entrada, reconciliar_lote, resumen_categorias
from src.quality.rules import cargar_config_calidad
from src.transformations.enrich import agregar_referencia
from src.transformations.silver import (
    clasificar_lote,
    preparar_aceptados,
    preparar_cuarentena,
    resultados_calidad,
)
from src.transformations.silver_writer import condicion_mas_reciente, sql_merge_scd2, sql_merge_tipo1
from src.transformations.standardize import standardize

CFG = PipelineConfig(fecha_corte=date(2026, 8, 31), run_id="test-run")
COLS = ["poliza_id", "cliente_id", "producto", "sucursal", "fecha_inicio_vigencia", "fecha_fin_vigencia",
        "estado", "prima", "moneda", "canal", "fecha_actualizacion", "origen"]
BASE = ["POL0000001", "CLI000001", "Autos", "Cali", "2026-01-01", "2026-12-31", "Vigente", "100.00", "COP",
        "Directo", "2026-03-01", "CORE"]


def _p(**kw):
    r = dict(zip(COLS, BASE, strict=True))
    r.update(kw)
    return [r[c] for c in COLS]


def _bronze(spark):
    rows = [
        _p(),                                                     
        _p(),                                                     
        _p(poliza_id="POL0000002", fecha_actualizacion="2026-02-01", prima="50.00"),
        _p(poliza_id="POL0000002", fecha_actualizacion="01/04/2026", prima="60.00"),
        _p(poliza_id="POL0000003", prima="-10.00"),               
        _p(poliza_id="POL0000004", producto=""),                  
        _p(poliza_id="POL0000005", fecha_fin_vigencia="2022-01-01"),
        _p(poliza_id="POL0000006", cliente_id="CLI_ORF001"),      
    ]
    df = spark.createDataFrame(rows, ", ".join(f"{c} STRING" for c in COLS))
    from pyspark.sql import functions as F
    return (df.withColumn("_row_hash", row_hash(COLS))
              .withColumn("_ingestion_ts", F.lit(datetime(2026, 9, 5, 11)).cast("timestamp"))
              .withColumn("_source_file", F.lit("raw/polizas/polizas.csv"))
              .withColumn("_source_file_hash", F.lit("fh1"))
              .withColumn("_run_id", F.lit("bronze-run"))
              .withColumn("_fecha_corte", F.lit(date(2026, 8, 31))))


def test_cadena_silver_polizas(spark):
    contrato = cargar_contrato("polizas")
    bronze = _bronze(spark)
    clientes = spark.createDataFrame([("CLI000001",)], "cliente_id STRING")
    cal = cargar_config_calidad(contrato, CFG.fecha_corte)

    enriquecido = agregar_referencia(standardize(bronze, contrato), contrato.referencias[0], clientes, None)
    etiquetado = clasificar_lote(enriquecido, contrato, cal.reglas)
    cats = {(r["poliza_id"], str(r["prima"])): r["_categoria"] for r in etiquetado.collect()}

    assert cats[("POL0000002", "60.00")] == "ACEPTADO" and cats[("POL0000002", "50.00")] == "DUP_CLAVE"
    assert cats[("POL0000003", "-10.00")] == "CUARENTENA"
    assert cats[("POL0000004", "100.00")] == "CUARENTENA"
    assert cats[("POL0000005", "100.00")] == "CUARENTENA"
    assert cats[("POL0000006", "100.00")] == "ACEPTADO"  

    resumen = resumen_categorias(etiquetado, contrato)
    recon = reconciliar_lote(CFG, contrato, resumen, medir_entrada(bronze, contrato), datetime(2026, 9, 5))
    assert all(r["resultado"] == "OK" for r in recon), recon
    assert resumen["DUP_EXACTO"]["registros"] == 1 and resumen["CUARENTENA"]["registros"] == 3

    acc = {r["poliza_id"]: r for r in preparar_aceptados(etiquetado, contrato, CFG, datetime(2026, 9, 5)).collect()}
    assert set(acc) == {"POL0000001", "POL0000002", "POL0000006"}
    assert "polizas.RI.cliente" in acc["POL0000006"]["_dq_advertencias"]
    assert acc["POL0000002"]["fecha_actualizacion"] == date(2026, 4, 1)
    assert acc["POL0000001"]["_silver_run_id"] == "test-run" and acc["POL0000001"]["_bronze_run_id"] == "bronze-run"

    q = {r["clave_negocio"]: r for r in preparar_cuarentena(etiquetado, contrato, cal.reglas, CFG, datetime(2026, 9, 5)).collect()}
    assert q["POL0000003"]["regla_id"] == "polizas.VAL.prima_positiva"
    assert q["POL0000004"]["regla_id"] == "polizas.COMP.producto"
    assert q["POL0000005"]["regla_id"] == "polizas.CON.vigencia"
    assert '"prima":"-10.00"' in q["POL0000003"]["registro"] and q["POL0000003"]["_source_file_hash"] == "fh1"

    filas = resultados_calidad(etiquetado, contrato, cal, resumen, CFG, datetime(2026, 9, 5))
    df = spark.createDataFrame(filas, schema=_schema_ddl("calidad_resultados"))
    umbral = df.where("regla_id = 'polizas.UMBRAL.cuarentena_total'").first()
    assert umbral["registros_fallidos"] == 3 and umbral["resultado"] == "FALLA"
    spark.createDataFrame(recon, schema=_schema_ddl("reconciliacion")).count()


def test_scd2_clientes_columnas(spark):
    contrato = cargar_contrato("clientes")
    assert contrato.historizacion == "scd2"
    sql = sql_merge_scd2("cat.sch.silver_clientes", "src", ["cliente_id"], ["cliente_id", "_attr_hash"])
    assert "cur.es_actual" in sql and "t.es_actual = false" in sql and "u.vigente_desde >= t.vigente_desde" in sql


def test_merge_tipo1_solo_actualiza_si_cambia_y_es_mas_reciente():
    sql = sql_merge_tipo1("t1", "src", ["poliza_id"], ["fecha_actualizacion"])
    assert "t._row_hash <> s._row_hash" in sql
    cond = condicion_mas_reciente(["fecha_actualizacion"])
    assert "s.`_ingestion_ts` > t.`_ingestion_ts`" in cond
    assert "coalesce(s.`fecha_actualizacion`, DATE'1900-01-01') > coalesce(t.`fecha_actualizacion`" in cond
