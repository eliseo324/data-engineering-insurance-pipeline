from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

TIPOS_SOPORTADOS = ("string", "date", "int", "bigint", "double")


class ContractError(ValueError):
    """Contrato mal definido o esquema recibido incompatible con el contrato."""


@dataclass(frozen=True)
class Columna:
    nombre: str
    tipo: str = "string"
    requerido: bool = False
    descripcion: str = ""
    pii: str | None = None
    valores_permitidos: tuple[str, ...] | None = None
    patron: str | None = None

    @property
    def es_monetaria(self) -> bool:
        return self.tipo.startswith("decimal")


@dataclass(frozen=True)
class Referencia:
    """FK simple: `columna` debe existir en `tabla.columna_ref`. `traer` = columnas del padre útiles
    para reglas (p. ej. la vigencia de la póliza para validar pagos fuera de vigencia)."""

    alias: str
    columna: str
    tabla: str
    columna_ref: str
    traer: tuple[str, ...] = ()

    @property
    def dataset_ref(self) -> str:
        return self.tabla.removeprefix("silver_")


@dataclass(frozen=True)
class ReferenciaPolimorfica:
    """FK cuyo destino depende del valor de otra columna (movimientos.entidad -> tabla)."""

    alias: str
    columna_tipo: str
    columna_id: str
    destinos: dict


@dataclass(frozen=True)
class Contrato:
    dataset: str
    version: str
    owner: str
    descripcion: str
    fuente: dict
    clave_negocio: tuple[str, ...]
    orden_version: tuple[str, ...]
    columnas: tuple[Columna, ...]
    historizacion: str = "tipo1"
    columnas_monetarias: tuple[str, ...] = ()
    referencias: tuple[Referencia, ...] = ()
    atributos_scd2: tuple[str, ...] = ()
    referencia_polimorfica: ReferenciaPolimorfica | None = None
    sla: dict = field(default_factory=dict)

    @property
    def nombres(self) -> list[str]:
        return [c.nombre for c in self.columnas]

    @property
    def requeridas(self) -> list[str]:
        return [c.nombre for c in self.columnas if c.requerido]

    @property
    def columnas_pii(self) -> list[Columna]:
        return [c for c in self.columnas if c.pii]

    def columna(self, nombre: str) -> Columna:
        for c in self.columnas:
            if c.nombre == nombre:
                return c
        raise KeyError(nombre)

    def comparar_esquema(self, columnas_recibidas: list[str]) -> dict:
        """Compara el encabezado recibido contra el contrato.

        Devuelve faltantes (columnas del contrato ausentes), faltantes_requeridas y nuevas
        (columnas no declaradas: evolución de esquema que se conserva en Bronze).
        """
        recibidas = [c.strip() for c in columnas_recibidas]
        faltantes = [c for c in self.nombres if c not in recibidas]
        return {
            "faltantes": faltantes,
            "faltantes_requeridas": [c for c in faltantes if c in self.requeridas],
            "nuevas": [c for c in recibidas if c not in self.nombres and not c.startswith("_")],
        }


def _validar(raw: dict, origen: str) -> None:
    for key in ("dataset", "clave_negocio", "columnas"):
        if key not in raw:
            raise ContractError(f"Contrato {origen}: falta la clave '{key}'")
    nombres = [c["nombre"] for c in raw["columnas"]]
    if len(nombres) != len(set(nombres)):
        raise ContractError(f"Contrato {origen}: columnas duplicadas")
    for c in raw["columnas"]:
        tipo = c.get("tipo", "string")
        if not (tipo in TIPOS_SOPORTADOS or tipo.startswith("decimal(")):
            raise ContractError(f"Contrato {origen}: tipo no soportado '{tipo}' en {c['nombre']}")
    for k in raw["clave_negocio"]:
        if k not in nombres:
            raise ContractError(f"Contrato {origen}: clave '{k}' no está en columnas")


def contrato_desde_dict(raw: dict, origen: str = "<dict>") -> Contrato:
    _validar(raw, origen)
    columnas = tuple(
        Columna(
            nombre=c["nombre"],
            tipo=c.get("tipo", "string"),
            requerido=bool(c.get("requerido", False)),
            descripcion=c.get("descripcion", ""),
            pii=c.get("pii"),
            valores_permitidos=tuple(c["valores_permitidos"]) if c.get("valores_permitidos") else None,
            patron=c.get("patron"),
        )
        for c in raw["columnas"]
    )
    return Contrato(
        dataset=raw["dataset"],
        version=str(raw.get("version", "0.0.0")),
        owner=raw.get("owner", ""),
        descripcion=" ".join(str(raw.get("descripcion", "")).split()),
        fuente=raw.get("fuente", {}),
        clave_negocio=tuple(raw["clave_negocio"]),
        orden_version=tuple(raw.get("orden_version", [])),
        columnas=columnas,
        historizacion=raw.get("historizacion", "tipo1"),
        columnas_monetarias=tuple(raw.get("columnas_monetarias", [])),
        referencias=tuple(
            Referencia(**{**r, "traer": tuple(r.get("traer", []))}) for r in raw.get("referencias", [])
        ),
        atributos_scd2=tuple(raw.get("atributos_scd2", [])),
        referencia_polimorfica=(
            ReferenciaPolimorfica(**raw["referencia_polimorfica"]) if raw.get("referencia_polimorfica") else None
        ),
        sla=raw.get("sla", {}),
    )


def cargar_contrato(dataset: str, contracts_dir: str | Path = "conf/contracts") -> Contrato:
    """Carga el contrato YAML de un dataset (ruta relativa a la raíz del repositorio)."""
    try:
        import yaml
    except ImportError as exc:
        raise ContractError("PyYAML no está disponible. Instale requirements.txt") from exc
    base = Path(contracts_dir)
    if not base.is_absolute():
        base = REPO_ROOT / base
    path = base / f"{dataset}.yml"
    if not path.exists():
        raise ContractError(f"No existe el contrato para '{dataset}' en {path}")
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if raw.get("dataset") != dataset:
        raise ContractError(f"{path}: dataset '{raw.get('dataset')}' no coincide con '{dataset}'")
    return contrato_desde_dict(raw, str(path))