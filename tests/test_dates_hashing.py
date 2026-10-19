from datetime import date

from pyspark.sql import functions as F

from src.common.dates import detect_date_format, parse_date
from src.common.hashing import row_hash, schema_hash


def test_parse_date_multiples_formatos(spark):
    df = spark.createDataFrame(
        [("2024-11-22",), ("22/11/2024",), ("2024/11/22",), (" 2024-11-22 ",), ("13/02/2024",), ("basura",), (None,), ("",)],
        "f STRING",
    )
    out = [r[0] for r in df.select(parse_date("f")).collect()]
    assert out[:4] == [date(2024, 11, 22)] * 4
    assert out[4] == date(2024, 2, 13)
    assert out[5:] == [None, None, None]


def test_detect_date_format(spark):
    df = spark.createDataFrame([("2024-01-05",), ("05/01/2024",), ("2024/01/05",), ("ene-2024",), (None,)], "f STRING")
    assert [r[0] for r in df.select(detect_date_format("f")).collect()] == [
        "yyyy-MM-dd", "dd/MM/yyyy", "yyyy/MM/dd", "OTRO", "NULO"]


def test_row_hash_deterministico_y_distingue_nulo_de_vacio(spark):
    df = spark.createDataFrame([("a", "b"), ("a", "b"), ("a", None), ("a", "")], "x STRING, y STRING")
    h = [r[0] for r in df.select(row_hash(["x", "y"])).collect()]
    assert h[0] == h[1] 
    assert h[2] != h[3]
    assert len(h[0]) == 64
    h_inv = df.select(row_hash(["y", "x"])).first()[0]
    assert h_inv != h[0]


def test_schema_hash_detecta_cambio_de_columnas():
    assert schema_hash(["a", "b"]) == schema_hash(["a", "b"])
    assert schema_hash(["a", "b"]) != schema_hash(["a", "b", "c"])


def test_row_hash_estable_ante_tipos(spark):
    df = spark.createDataFrame([(1, "x")], "n INT, s STRING").withColumn("n_str", F.col("n").cast("string"))
    assert df.select(row_hash(["n", "s"])).first()[0] == df.select(row_hash(["n_str", "s"])).first()[0]
