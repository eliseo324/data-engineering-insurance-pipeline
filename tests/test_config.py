from datetime import date

import pytest

from src.common.config import ConfigError, PipelineConfig, parse_fecha_corte, ultimo_dia_mes_anterior


def test_fecha_corte_por_defecto_es_ultimo_dia_mes_anterior():
    assert ultimo_dia_mes_anterior(date(2026, 9, 5)) == date(2026, 8, 31)
    assert ultimo_dia_mes_anterior(date(2026, 3, 5)) == date(2026, 2, 28)
    assert ultimo_dia_mes_anterior(date(2026, 1, 5)) == date(2025, 12, 31)
    assert parse_fecha_corte("", hoy=date(2026, 9, 5)) == date(2026, 8, 31)


def test_fecha_corte_explicita_e_invalida():
    assert parse_fecha_corte("2026-08-31") == date(2026, 8, 31)
    with pytest.raises(ConfigError):
        parse_fecha_corte("31/08/2026")


def test_from_params_genera_run_id_si_no_se_resolvio_el_valor_dinamico():
    cfg = PipelineConfig.from_params({"fecha_corte": "2026-08-31", "job_run_id": "{{job.run_id}}", "modo": ""})
    assert cfg.run_id.startswith("manual-")
    assert cfg.modo == "normal" 
    cfg2 = PipelineConfig.from_params({"fecha_corte": "2026-08-31", "job_run_id": "123456"})
    assert cfg2.run_id == "123456"


def test_nombres_calificados_y_rutas_parametrizadas():
    cfg = PipelineConfig(fecha_corte=date(2026, 8, 31), catalog="cat", schema="sch", volume="vol")
    assert cfg.bronze("polizas") == "cat.sch.bronze_polizas"
    assert cfg.gold("poliza_360") == "cat.sch.gold_poliza_360"
    assert cfg.landing_path("polizas") == "/Volumes/cat/sch/vol/raw/polizas"


def test_modo_invalido_falla_con_mensaje_util():
    with pytest.raises(ConfigError, match="modo"):
        PipelineConfig(fecha_corte=date(2026, 8, 31), modo="full")
