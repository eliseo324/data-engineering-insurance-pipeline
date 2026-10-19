from __future__ import annotations

import fnmatch
from collections.abc import Callable
from pathlib import Path

from src.common.contracts import REPO_ROOT

PRIVILEGIOS_VALIDOS = {
    "USE CATALOG", "USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE", "CREATE FUNCTION", "EXECUTE",
    "READ VOLUME", "WRITE VOLUME", "BROWSE",
}


class GovernanceConfigError(ValueError):
    """Configuración de gobierno inválida."""


def cargar_config_gobierno(path: str = "conf/gobierno.yml") -> dict:
    import yaml

    p = Path(path) if Path(path).is_absolute() else REPO_ROOT / path
    return yaml.safe_load(p.read_text(encoding="utf-8"))


def _resolver(patrones: list[str], disponibles: list[str], excluir: list[str]) -> list[str]:
    elegidos = {n for n in disponibles for p in patrones if fnmatch.fnmatch(n, p)}
    return sorted(elegidos - set(excluir))


def plan_permisos(
    conf: dict, catalog: str, schema: str, volume: str, tablas: list[str], funciones: list[str],
    T: Callable[[str], str], revocar_previos: bool = True,
) -> list[tuple[str, str]]:
    """Sentencias para llevar los permisos al estado declarado.

    `tablas` incluye tablas y vistas existentes; `funciones`, las funciones de usuario del esquema.
    """
    plan: list[tuple[str, str]] = []
    for grupo, reglas in conf["permisos"].items():
        if grupo not in conf["grupos"]:
            raise GovernanceConfigError(f"Grupo no declarado: {grupo}")
        g = f"`{grupo}`"
        if revocar_previos:
            plan.append((f"revocar esquema {grupo}", f"REVOKE ALL PRIVILEGES ON SCHEMA {catalog}.{schema} FROM {g}"))
            plan.append((f"revocar volumen {grupo}", f"REVOKE ALL PRIVILEGES ON VOLUME {catalog}.{schema}.{volume} FROM {g}"))
        for regla in reglas:
            privs = regla["privilegios"]
            invalidos = set(privs) - PRIVILEGIOS_VALIDOS
            if invalidos:
                raise GovernanceConfigError(f"{grupo}: privilegios no soportados {sorted(invalidos)}")
            p = ", ".join(privs)
            excluir = conf.get(regla["excluir"], []) if isinstance(regla.get("excluir"), str) else regla.get("excluir", [])
            if regla.get("objeto") == "catalogo":
                plan.append((f"{grupo}: {p} catálogo", f"GRANT {p} ON CATALOG {catalog} TO {g}"))
            elif regla.get("objeto") == "esquema":
                plan.append((f"{grupo}: {p} esquema", f"GRANT {p} ON SCHEMA {catalog}.{schema} TO {g}"))
            elif regla.get("objeto") == "volumen":
                plan.append((f"{grupo}: {p} volumen", f"GRANT {p} ON VOLUME {catalog}.{schema}.{volume} TO {g}"))
            elif "tablas" in regla:
                for t in _resolver(regla["tablas"], tablas, excluir):
                    if grupo != "pii_lectores" and grupo != "ingenieria_datos" and t in conf.get("tablas_pii", []):
                        raise GovernanceConfigError(f"{grupo} no puede recibir acceso directo a la tabla PII {t}")
                    plan.append((f"{grupo}: {p} {t}", f"GRANT {p} ON TABLE {T(t)} TO {g}"))
            elif "funciones" in regla:
                for f in _resolver(regla["funciones"], funciones, excluir):
                    plan.append((f"{grupo}: {p} {f}", f"GRANT {p} ON FUNCTION {T(f)} TO {g}"))
            else:
                raise GovernanceConfigError(f"{grupo}: regla sin objeto reconocible {regla}")
    return plan
