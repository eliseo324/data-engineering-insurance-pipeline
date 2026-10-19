from __future__ import annotations

import re
from collections.abc import Callable

COLUMNAS_PERSONALES = ("numero_documento", "fecha_nacimiento", "correo", "telefono")
_ID_VALIDO = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")


class SolicitudInvalida(ValueError):
    """Identificador de titular inválido."""


def plan_supresion(T: Callable[[str], str], cliente_id: str) -> list[tuple[str, str]]:
    """Sentencias que suprimen los datos personales de un cliente en todas las capas."""
    if not _ID_VALIDO.match(cliente_id or ""):
        raise SolicitudInvalida(f"cliente_id inválido: {cliente_id!r}")
    k = cliente_id.replace("'", "")
    nulos = ", ".join(f"{c} = NULL" for c in COLUMNAS_PERSONALES)
    return [
        ("bronze_clientes", f"UPDATE {T('bronze_clientes')} SET {nulos} WHERE cliente_id = '{k}'"),
        ("silver_clientes", f"UPDATE {T('silver_clientes')} SET {nulos} WHERE cliente_id = '{k}'"),
        ("silver_cuarentena",
         f"UPDATE {T('silver_cuarentena')} SET registro = to_json(named_struct('cliente_id', clave_negocio, "
         f"'suprimido', true)) WHERE dataset = 'clientes' AND clave_negocio = '{k}'"),
    ]


def plan_vacuum(T: Callable[[str], str]) -> list[tuple[str, str]]:
    """VACUUM de las tablas con datos personales (respeta la retención configurada)."""
    return [(t, f"VACUUM {T(t)}") for t in ("bronze_clientes", "silver_clientes", "silver_cuarentena")]


def plan_retencion(T: Callable[[str], str], propiedades: dict[str, str]) -> list[tuple[str, str]]:
    props = ", ".join(f"'{k}' = '{v}'" for k, v in propiedades.items())
    return [(f"retención {t}", f"ALTER TABLE {T(t)} SET TBLPROPERTIES ({props})")
            for t in ("bronze_clientes", "silver_clientes", "silver_cuarentena")]
