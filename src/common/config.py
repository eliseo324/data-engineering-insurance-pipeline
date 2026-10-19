from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

TZ_COLOMBIA = ZoneInfo("America/Bogota")

MODOS_VALIDOS = ("normal", "reproceso")
AMBIENTES_VALIDOS = ("dev", "qa", "prod")
DATASETS = ("clientes", "polizas", "recaudos", "siniestros", "movimientos")


class ConfigError(ValueError):
    """Parámetro de ejecución inválido."""


def ultimo_dia_mes_anterior(hoy: date) -> date:
    """Fecha de corte por defecto: último día del mes anterior a `hoy`.

    El job corre el día 5 de cada mes y procesa el mes cerrado anterior.
    """
    return hoy.replace(day=1) - timedelta(days=1)


def parse_fecha_corte(valor: str | None, hoy: date | None = None) -> date:
    """Convierte el parámetro `fecha_corte` (YYYY-MM-DD) o calcula el valor por defecto."""
    if valor is None or str(valor).strip() == "":
        hoy = hoy or datetime.now(TZ_COLOMBIA).date()
        return ultimo_dia_mes_anterior(hoy)
    try:
        return datetime.strptime(str(valor).strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ConfigError(f"fecha_corte inválida '{valor}'. Formato esperado YYYY-MM-DD") from exc


@dataclass(frozen=True)
class PipelineConfig:
    """Parámetros resueltos de una ejecución."""

    fecha_corte: date
    modo: str = "normal"
    ambiente: str = "dev"
    catalog: str = "workspace"
    schema: str = "default"
    volume: str = "volume1"
    landing_dir: str = "raw"
    run_id: str = field(default_factory=lambda: f"manual-{uuid.uuid4().hex[:12]}")
    pipeline: str = "insurance_medallion"
    dias_proximo_vencer: int = 30
    contracts_dir: str = "conf/contracts"

    def __post_init__(self) -> None:
        if self.modo not in MODOS_VALIDOS:
            raise ConfigError(f"modo '{self.modo}' inválido. Valores: {MODOS_VALIDOS}")
        if self.ambiente not in AMBIENTES_VALIDOS:
            raise ConfigError(f"ambiente '{self.ambiente}' inválido. Valores: {AMBIENTES_VALIDOS}")

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    def landing_path(self, dataset: str) -> str:
        return f"{self.volume_path}/{self.landing_dir}/{dataset}"

    def table(self, name: str) -> str:
        """Nombre totalmente calificado catalog.schema.objeto en Unity Catalog."""
        return f"{self.catalog}.{self.schema}.{name}"

    def bronze(self, dataset: str) -> str:
        return self.table(f"bronze_{dataset}")

    def silver(self, dataset: str) -> str:
        return self.table(f"silver_{dataset}")

    def gold(self, name: str) -> str:
        return self.table(f"gold_{name}")

    def ctl(self, name: str) -> str:
        return self.table(f"ctl_{name}")

    @property
    def fecha_corte_str(self) -> str:
        return self.fecha_corte.isoformat()

    def as_dict(self) -> dict:
        return {
            "fecha_corte": self.fecha_corte_str,
            "modo": self.modo,
            "ambiente": self.ambiente,
            "catalog": self.catalog,
            "schema": self.schema,
            "volume": self.volume,
            "run_id": self.run_id,
            "pipeline": self.pipeline,
        }

    
    @classmethod
    def from_params(cls, params: dict, hoy: date | None = None) -> PipelineConfig:
        """Construye la configuración desde un diccionario de parámetros (job o widgets).
        Los valores vacíos se ignoran para conservar los defaults.
        Cuando el valor dinámico `{{job.run_id}}` no se resolvió (ejecución interactiva) se genera uno.
        """
        p = {k: v for k, v in params.items() if v is not None and str(v).strip() != ""}
        run_id = p.get("run_id") or p.get("job_run_id")
        if not run_id or "{{" in str(run_id):
            run_id = f"manual-{uuid.uuid4().hex[:12]}"
        kwargs = {
            "fecha_corte": parse_fecha_corte(p.get("fecha_corte"), hoy),
            "run_id": str(run_id),
        }
        for key in ("modo", "ambiente", "catalog", "schema", "volume", "landing_dir", "pipeline", "contracts_dir"):
            if key in p:
                kwargs[key] = str(p[key]).strip()
        if "dias_proximo_vencer" in p:
            kwargs["dias_proximo_vencer"] = int(p["dias_proximo_vencer"])
        return cls(**kwargs)


WIDGETS_DEFAULT = {
    "fecha_corte": "",
    "modo": "normal",
    "ambiente": "dev",
    "catalog": "workspace",
    "schema": "default",
    "volume": "volume1",
    "job_run_id": "",
}


def config_from_widgets(dbutils, extra_widgets: dict | None = None) -> PipelineConfig:
    """Crea los widgets estándar (si no existen) y devuelve la configuración resuelta.

    En un job, los parámetros del job sobrescriben los valores de los widgets.
    """
    widgets = {**WIDGETS_DEFAULT, **(extra_widgets or {})}
    for name, default in widgets.items():
        try:
            dbutils.widgets.text(name, default)
        except Exception:  # noqa: BLE001 - el widget ya existe
            pass
    params = {name: dbutils.widgets.get(name) for name in widgets}
    return PipelineConfig.from_params(params)
