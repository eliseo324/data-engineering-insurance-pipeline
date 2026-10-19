from __future__ import annotations

NIVELES = {"ninguno": 0, "parche": 1, "menor": 2, "mayor": 3}


def _cols(raw: dict) -> dict[str, dict]:
    return {c["nombre"]: c for c in raw.get("columnas", [])}


def comparar_contratos(anterior: dict, nuevo: dict) -> tuple[str, list[str]]:
    """Devuelve (nivel requerido, lista de cambios detectados)."""
    cambios: list[tuple[str, str]] = []
    a, n = _cols(anterior), _cols(nuevo)

    for nombre in a.keys() - n.keys():
        cambios.append(("mayor", f"columna eliminada: {nombre}"))
    for nombre in n.keys() - a.keys():
        req = n[nombre].get("requerido", False)
        cambios.append(("mayor" if req else "menor", f"columna nueva {'requerida' if req else 'opcional'}: {nombre}"))
    for nombre in a.keys() & n.keys():
        ca, cn = a[nombre], n[nombre]
        if ca.get("tipo", "string") != cn.get("tipo", "string"):
            cambios.append(("mayor", f"{nombre}: tipo {ca.get('tipo', 'string')} -> {cn.get('tipo', 'string')}"))
        if not ca.get("requerido", False) and cn.get("requerido", False):
            cambios.append(("mayor", f"{nombre}: pasa a ser requerida"))
        if ca.get("requerido", False) and not cn.get("requerido", False):
            cambios.append(("menor", f"{nombre}: deja de ser requerida"))
        va, vn = set(ca.get("valores_permitidos") or []), set(cn.get("valores_permitidos") or [])
        if va and (not vn or vn != va):
            if va - vn and vn:
                cambios.append(("mayor", f"{nombre}: dominio reducido, se quitan {sorted(va - vn)}"))
            if vn - va:
                cambios.append(("menor", f"{nombre}: dominio ampliado con {sorted(vn - va)}"))
            if not vn:
                cambios.append(("menor", f"{nombre}: se elimina la restricción de dominio"))
        elif vn and not va:
            cambios.append(("mayor", f"{nombre}: se agrega restricción de dominio"))
        if ca.get("patron") != cn.get("patron"):
            cambios.append(("mayor" if cn.get("patron") else "menor", f"{nombre}: patrón modificado"))
        if ca.get("pii") != cn.get("pii"):
            cambios.append(("menor", f"{nombre}: clasificación PII {ca.get('pii')} -> {cn.get('pii')}"))
        if ca.get("descripcion") != cn.get("descripcion"):
            cambios.append(("parche", f"{nombre}: descripción"))

    for campo in ("clave_negocio", "historizacion", "orden_version"):
        if anterior.get(campo) != nuevo.get(campo):
            cambios.append(("mayor", f"{campo}: {anterior.get(campo)} -> {nuevo.get(campo)}"))
    for campo in ("descripcion", "owner"):
        if anterior.get(campo) != nuevo.get(campo):
            cambios.append(("parche", f"{campo} del contrato"))
    if anterior.get("sla") != nuevo.get("sla"):
        cambios.append(("menor", f"sla: {anterior.get('sla')} -> {nuevo.get('sla')}"))

    nivel = max((c[0] for c in cambios), key=NIVELES.__getitem__, default="ninguno")
    return nivel, [f"[{lvl}] {txt}" for lvl, txt in cambios]


def _semver(v: str) -> tuple[int, int, int]:
    partes = [int(x) for x in str(v).split(".")]
    return tuple((partes + [0, 0, 0])[:3])  # type: ignore[return-value]


def incremento_version(anterior: str, nueva: str) -> str:
    a, n = _semver(anterior), _semver(nueva)
    if n <= a:
        return "ninguno"
    if n[0] > a[0]:
        return "mayor"
    if n[1] > a[1]:
        return "menor"
    return "parche"


def validar_cambio(anterior: dict, nuevo: dict) -> list[str]:
    """Errores si el incremento de versión no cubre el nivel del cambio (lista vacía = válido)."""
    nivel, cambios = comparar_contratos(anterior, nuevo)
    if nivel == "ninguno":
        return []
    inc = incremento_version(str(anterior.get("version", "0.0.0")), str(nuevo.get("version", "0.0.0")))
    if NIVELES[inc] < NIVELES[nivel]:
        return [f"El contrato '{nuevo.get('dataset')}' requiere incremento {nivel.upper()} de versión "
                f"({anterior.get('version')} -> {nuevo.get('version')} es '{inc}'). Cambios: " + "; ".join(cambios)]
    return []
