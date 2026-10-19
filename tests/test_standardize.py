from datetime import date
from decimal import Decimal

from src.common.contracts import cargar_contrato
from src.transformations.standardize import standardize

POL_COLS = ("poliza_id STRING, cliente_id STRING, producto STRING, sucursal STRING, fecha_inicio_vigencia STRING, "
            "fecha_fin_vigencia STRING, estado STRING, prima STRING, moneda STRING, fecha_actualizacion STRING, "
            "origen STRING, _row_hash STRING, _ingestion_ts TIMESTAMP, _source_file STRING, "
            "_source_file_hash STRING, _run_id STRING, _fecha_corte DATE")


def _row(**kw):
    base = dict(poliza_id="POL0000001", cliente_id="CLI000001", producto="Autos", sucursal="Cali",
                fecha_inicio_vigencia="2026-01-27", fecha_fin_vigencia="27/01/2027", estado="Vigente",
                prima="13991560.54", moneda="COP", fecha_actualizacion="2026/03/18", origen="CORE",
                _row_hash="h", _ingestion_ts=None, _source_file="f", _source_file_hash="fh", _run_id="r",
                _fecha_corte=None)
    base.update(kw)
    return tuple(base.values())


def test_tipado_por_contrato_y_columna_opcional_ausente(spark):
    df = spark.createDataFrame([_row()], POL_COLS)
    out = standardize(df, cargar_contrato("polizas"))
    tipos = dict(out.dtypes)
    assert tipos["fecha_inicio_vigencia"] == "date"
    assert tipos["prima"] == "decimal(18,2)"
    r = out.first()
    assert r["fecha_fin_vigencia"] == date(2027, 1, 27)
    assert r["fecha_actualizacion"] == date(2026, 3, 18)
    assert r["prima"] == Decimal("13991560.54")
    assert r["canal"] is None
    assert r["_raw"]["fecha_fin_vigencia"] == "27/01/2027"


def test_limpieza_vacios_y_valores_invalidos_quedan_nulos(spark):
    df = spark.createDataFrame([_row(producto="  ", sucursal="  Cali ", prima="abc", fecha_inicio_vigencia="31-31-2026")],
                               POL_COLS)
    r = standardize(df, cargar_contrato("polizas")).first()
    assert r["producto"] is None  
    assert r["sucursal"] == "Cali"
    assert r["prima"] is None and r["_raw"]["prima"] == "abc"
    assert r["fecha_inicio_vigencia"] is None
