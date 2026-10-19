from __future__ import annotations

import json
from collections.abc import Callable


class RollbackError(ValueError):
    """Publicación o versiones inválidas para restaurar."""


def plan_rollback(T: Callable[[str], str], versiones_delta: str | dict) -> list[tuple[str, str]]:
    versiones = json.loads(versiones_delta) if isinstance(versiones_delta, str) else versiones_delta
    if not versiones:
        raise RollbackError("La publicación no tiene versiones Delta registradas")
    plan = []
    for tabla, version in sorted(versiones.items()):
        if not tabla.startswith("gold_"):
            raise RollbackError(f"Solo se restauran productos Gold: {tabla}")
        if not isinstance(version, int) or version < 0:
            raise RollbackError(f"Versión inválida para {tabla}: {version}")
        plan.append((f"restaurar {tabla} a v{version}", f"RESTORE TABLE {T(tabla)} TO VERSION AS OF {version}"))
    return plan
