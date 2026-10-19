from src.common.contracts import cargar_contrato
from src.quality.profiling import orphan_count, profile_dataframe

COLS = "recaudo_id STRING, poliza_id STRING, fecha_pago STRING, valor STRING, medio_pago STRING, " \
       "estado_transaccion STRING, referencia_bancaria STRING, fecha_ingesta STRING, origen STRING"


def _metric(perfil, columna, metrica):
    return perfil.where(f"columna = '{columna}' AND metrica = '{metrica}'").first()["valor"]


def test_perfil_detecta_duplicados_negativos_formatos_y_dominio(spark):
    rows = [
        ("REC00000001", "POL0000001", "2025-01-30", "100.50", "PSE", "Aplicado", "RB1", "2025-02-01", "PAGOS"),
        ("REC00000001", "POL0000001", "2025-01-30", "100.50", "PSE", "Aplicado", "RB1", "2025-02-01", "PAGOS"),
        ("REC00000002", "POL0000002", "30/01/2025", "-20.00", "PSE", "Aprobadito", "RB2", "2025-02-01", "PAGOS"),
        ("REC00000003", "POL0000003", "", "abc", "PSE", "Pendiente", "RB3", "2025-02-01", "PAGOS"),
    ]
    perfil = profile_dataframe(spark.createDataFrame(rows, COLS), cargar_contrato("recaudos"))
    assert _metric(perfil, "*", "registros") == "4"
    assert _metric(perfil, "*", "duplicados_exactos") == "1"
    assert _metric(perfil, "recaudo_id", "duplicados_clave_negocio") == "1"
    assert _metric(perfil, "valor", "negativos") == "1"
    assert _metric(perfil, "valor", "no_numericos") == "1"
    assert _metric(perfil, "fecha_pago", "formato_dd/MM/yyyy") == "1"
    assert _metric(perfil, "fecha_pago", "nulos_o_vacios") == "1"
    assert _metric(perfil, "estado_transaccion", "fuera_de_dominio") == "1"


def test_orphan_count(spark):
    hijos = spark.createDataFrame([("S1", "P1"), ("S2", "P9")], "siniestro_id STRING, poliza_id STRING")
    padres = spark.createDataFrame([("P1",)], "poliza_id STRING")
    assert [r["siniestro_id"] for r in orphan_count(hijos, "poliza_id", padres, "poliza_id").collect()] == ["S2"]
