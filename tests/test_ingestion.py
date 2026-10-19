# import shutil
# import uuid
from datetime import date, datetime
# from pathlib import Path

# import pytest

from src.common.config import PipelineConfig
from src.common.contracts import cargar_contrato
from src.ingestion.bronze import (
    METADATA_COLUMNS,
    _ordered_data_columns,
    add_ingestion_metadata,
    read_source_file,
)
from src.ingestion.file_discovery import discover_files, select_files_to_process

# @pytest.fixture
# def tmp_path():
#     """tmp_path en UC Volume para que Spark pueda leer los archivos en Serverless."""
#     base = Path("/Volumes/workspace/default/volume1/_test_tmp")
#     path = base / uuid.uuid4().hex
#     path.mkdir(parents=True, exist_ok=True)
#     yield path
#     shutil.rmtree(str(path), ignore_errors=True)


HEADER = "evento_id;entidad;id_entidad;tipo_evento;fecha_evento;fecha_ingesta;payload;secuencia;origen"
ROW1 = 'EVT1;POLIZA;POL0000001;CREACION;2024-01-05;2024-01-06;"{""estado_origen"":""NA"",""estado_destino"":""Vigente""}";1;BUS'
ROW2 = 'EVT2;POLIZA;POL0000001;CANCELACION;05/02/2024;2024-02-06;"{""estado_origen"":""Vigente""}";2;BUS'


def _write(path, text, bom=True):
    path.write_bytes(("_" if bom else "").encode() + text.replace("\n", "\r\n").encode())


def test_discovery_y_reenvio_con_otro_nombre_se_omite(tmp_path):
    _write(tmp_path / "mov_2026_08.csv", f"{HEADER}\n{ROW1}\n")
    _write(tmp_path / "mov_2026_08_copia.csv", f"{HEADER}\n{ROW1}\n")
    _write(tmp_path / "mov_2026_09.csv", f"{HEADER}\n{ROW2}\n")
    (tmp_path / "_SUCCESS").write_text("")
    files = discover_files(str(tmp_path), "*.csv")
    assert len(files) == 3

    procesar, omitidos = select_files_to_process(files, already_processed_hashes=set(), modo="normal")
    assert len(procesar) == 2 and len(omitidos) == 1

    ya = {f.sha256 for f in procesar}
    procesar2, omitidos2 = select_files_to_process(files, already_processed_hashes=ya, modo="normal")
    assert procesar2 == [] and len(omitidos2) == 3

    procesar3, _ = select_files_to_process(files, already_processed_hashes=ya, modo="reproceso")
    assert len(procesar3) == 2


def test_lectura_csv_con_bom_separador_y_json_escapado(spark, tmp_path):
    _write(tmp_path / "mov.csv", f"{HEADER}\n{ROW1}\n{ROW2}\n")
    contrato = cargar_contrato("movimientos")
    f = discover_files(str(tmp_path))[0]
    df = read_source_file(spark, f, contrato)
    assert df.columns[0] == "evento_id"
    assert dict(df.dtypes)["fecha_evento"] == "string"
    rows = df.orderBy("evento_id").collect()
    assert rows[0]["payload"] == '{"estado_origen":"NA","estado_destino":"Vigente"}'
    assert rows[1]["fecha_evento"] == "05/02/2024"


def test_metadatos_de_ingesta(spark, tmp_path):
    _write(tmp_path / "mov.csv", f"{HEADER}\n{ROW1}\n{ROW1}\n")
    contrato = cargar_contrato("movimientos")
    f = discover_files(str(tmp_path))[0]
    raw = read_source_file(spark, f, contrato)
    cfg = PipelineConfig(fecha_corte=date(2026, 8, 31), run_id="run-test")
    out = add_ingestion_metadata(raw, f, cfg, datetime(2026, 9, 5, 11, 0), _ordered_data_columns(contrato, raw.columns))
    assert set(METADATA_COLUMNS) <= set(out.columns)
    rows = out.collect()
    assert {r["_run_id"] for r in rows} == {"run-test"}
    assert {r["_fecha_corte"] for r in rows} == {date(2026, 8, 31)}
    assert rows[0]["_source_file_hash"] == f.sha256
    assert rows[0]["_row_hash"] == rows[1]["_row_hash"]


def test_orden_columnas_hash_incluye_columnas_nuevas_al_final():
    contrato = cargar_contrato("movimientos")
    recibidas = ["zeta_nueva"] + list(reversed(contrato.nombres))
    assert _ordered_data_columns(contrato, recibidas) == contrato.nombres + ["zeta_nueva"]
