import pytest

from src.common.contracts import REPO_ROOT, ContractError, cargar_contrato, contrato_desde_dict

DATASETS = tuple(p.stem for p in (REPO_ROOT / "conf" / "contracts").glob("*.yml"))

# from src.common.config import DATASETS
# from src.common.contracts import ContractError, cargar_contrato, contrato_desde_dict


@pytest.mark.parametrize("dataset", DATASETS)
def test_todos_los_contratos_cargan_y_son_coherentes(dataset):
    c = cargar_contrato(dataset)
    assert c.dataset == dataset
    assert set(c.clave_negocio) <= set(c.nombres)
    assert set(c.columnas_monetarias) <= set(c.nombres)
    for m in c.columnas_monetarias:
        assert c.columna(m).es_monetaria
    assert all(o in c.nombres for o in c.orden_version)


def test_columnas_pii_clasificadas_en_clientes():
    pii = {c.nombre: c.pii for c in cargar_contrato("clientes").columnas_pii}
    assert pii["numero_documento"] == "alta"
    assert {"correo", "telefono", "fecha_nacimiento"} <= set(pii)


def test_comparar_esquema_detecta_faltantes_y_nuevas():
    c = cargar_contrato("polizas")
    recibidas = [n for n in c.nombres if n not in ("prima", "canal")] + ["descuento"]
    cmp = c.comparar_esquema(recibidas)
    assert set(cmp["faltantes"]) == {"prima", "canal"}
    assert cmp["faltantes_requeridas"] == ["prima"]
    assert cmp["nuevas"] == ["descuento"]


def test_contrato_invalido_falla():
    with pytest.raises(ContractError, match="clave"):
        contrato_desde_dict({"dataset": "x", "clave_negocio": ["id"], "columnas": [{"nombre": "otro"}]})
    with pytest.raises(ContractError, match="tipo no soportado"):
        contrato_desde_dict({"dataset": "x", "clave_negocio": ["id"], "columnas": [{"nombre": "id", "tipo": "uuid"}]})
