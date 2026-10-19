from __future__ import annotations

from src.common.config import DATASETS
from src.common.contracts import cargar_contrato
from src.governance.catalogo import CLASIFICACION_PII, COLUMNAS_TECNICAS, GOLD, PREFIJO_REF


def _lit(texto: str) -> str:
    return "'" + " ".join(str(texto).split()).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _desc_columna(col: str, negocio: dict[str, str]) -> str | None:
    if col in negocio:
        return negocio[col]
    if col in COLUMNAS_TECNICAS:
        return COLUMNAS_TECNICAS[col]
    for pref, d in PREFIJO_REF.items():
        if col.startswith(pref):
            return f"{d}: {col.removeprefix(pref)}"
    return None


def plan_documentacion(T, columnas_por_tabla: dict[str, list[str]]) -> list[tuple[str, str]]:
    """Devuelve (descripción, sql) para documentar y etiquetar las tablas existentes.

    `columnas_por_tabla`: nombre corto de tabla -> columnas reales (solo se documenta lo que existe).
    """
    plan: list[tuple[str, str]] = []

    def tabla(nombre, comentario, tags, negocio, pii):
        if nombre not in columnas_por_tabla:
            return
        fqn = T(nombre)
        plan.append((f"comentario {nombre}", f"COMMENT ON TABLE {fqn} IS {_lit(comentario)}"))
        tags_sql = ", ".join(f"{_lit(k)} = {_lit(v)}" for k, v in tags.items())
        plan.append((f"tags {nombre}", f"ALTER TABLE {fqn} SET TAGS ({tags_sql})"))
        for col in columnas_por_tabla[nombre]:
            d = _desc_columna(col, negocio)
            if d:
                plan.append((f"comentario {nombre}.{col}", f"ALTER TABLE {fqn} ALTER COLUMN `{col}` COMMENT {_lit(d)}"))
            if col in pii:
                plan.append((f"pii {nombre}.{col}", f"ALTER TABLE {fqn} ALTER COLUMN `{col}` SET TAGS "
                                                     f"('pii' = {_lit(pii[col])}, "
                                                     f"'clasificacion' = {_lit(CLASIFICACION_PII[pii[col]])})"))

    for ds in DATASETS:
        c = cargar_contrato(ds)
        negocio = {col.nombre: col.descripcion for col in c.columnas if col.descripcion}
        pii = {col.nombre: col.pii for col in c.columnas_pii}
        base = {"dominio": ds, "owner": c.owner, "contrato_version": c.version,
                "contiene_pii": "true" if pii else "false"}
        tabla(f"bronze_{ds}", f"Bronze {ds}: copia fiel (texto) de los archivos de {c.fuente.get('sistema', ds)} "
                              f"con metadatos de ingesta. {c.descripcion}", {**base, "capa": "bronze"}, negocio, pii)
        tabla(f"silver_{ds}", f"Silver {ds}: tipado, validado, deduplicado y conformado "
                              f"(historización {c.historizacion}). {c.descripcion}", {**base, "capa": "silver"},
              negocio, pii)

    tabla("silver_cuarentena", "Registros rechazados por reglas de calidad con regla, fecha, pipeline y run_id. "
                               "El campo registro puede contener datos personales.",
          {"capa": "silver", "dominio": "calidad", "owner": "ingenieria-datos", "contiene_pii": "true"},
          {"registro": "Registro original en JSON tal como llegó (puede contener datos personales)"}, {})
    for nombre, meta in GOLD.items():
        tabla(f"gold_{nombre}", meta["comentario"],
              {"capa": "gold", "dominio": "seguros", "owner": "ingenieria-datos", "contiene_pii": "false",
               "producto_datos": "true"}, meta["columnas"], {})
    return plan
