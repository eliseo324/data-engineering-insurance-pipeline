from __future__ import annotations

from collections.abc import Callable


def cuerpos_mascara(es_lector: str) -> dict[str, tuple[str, str, str]]:
    """nombre -> (tipo de entrada, tipo de salida, expresión SQL sobre `v`). `es_lector` = expresión booleana."""
    return {
        "fn_mask_documento": ("STRING", "STRING",
                              f"CASE WHEN v IS NULL THEN NULL WHEN {es_lector} THEN v "
                              f"ELSE concat(repeat('*', greatest(length(v) - 4, 0)), right(v, 4)) END"),
        "fn_mask_correo": ("STRING", "STRING",
                           f"CASE WHEN v IS NULL THEN NULL WHEN {es_lector} THEN v "
                           f"ELSE concat(left(v, 1), '***@', substring_index(v, '@', -1)) END"),
        "fn_mask_telefono": ("STRING", "STRING",
                             f"CASE WHEN v IS NULL THEN NULL WHEN {es_lector} THEN v "
                             f"ELSE concat(repeat('*', greatest(length(v) - 3, 0)), right(v, 3)) END"),
        "fn_mask_fecha_nacimiento": ("DATE", "DATE",
                                     f"CASE WHEN v IS NULL THEN NULL WHEN {es_lector} THEN v "
                                     f"ELSE make_date(year(v), 1, 1) END"),
    }


MASCARA_POR_COLUMNA = {
    "numero_documento": "fn_mask_documento",
    "correo": "fn_mask_correo",
    "telefono": "fn_mask_telefono",
    "fecha_nacimiento": "fn_mask_fecha_nacimiento",
}


def plan_proteccion(T: Callable[[str], str], grupo_pii: str, aplicar_column_masks: bool = False) -> list[tuple[str, str]]:
    es_lector_fn = T("fn_es_lector_pii")
    plan = [("funcion fn_es_lector_pii", f"""
        CREATE OR REPLACE FUNCTION {es_lector_fn}() RETURNS BOOLEAN
        COMMENT 'TRUE si el usuario pertenece al grupo autorizado para ver datos personales ({grupo_pii})'
        RETURN is_account_group_member('{grupo_pii}')""")]
    for nombre, (tin, tout, cuerpo) in cuerpos_mascara(f"{es_lector_fn}()").items():
        plan.append((f"funcion {nombre}", f"""
            CREATE OR REPLACE FUNCTION {T(nombre)}(v {tin}) RETURNS {tout}
            COMMENT 'Enmascaramiento de datos personales: valor en claro solo para {grupo_pii}'
            RETURN {cuerpo}"""))

    cols = ", ".join(f"{T(fn)}({c}) AS {c}" for c, fn in MASCARA_POR_COLUMNA.items())
    plan.append(("vista vw_clientes_protegida", f"""
        CREATE OR REPLACE VIEW {T('vw_clientes_protegida')}
        COMMENT 'Clientes con datos personales enmascarados salvo para {grupo_pii}. Consumir esta vista, no la tabla.'
        AS SELECT cliente_id, tipo_documento, ciudad, segmento, fecha_actualizacion, origen, {cols},
                  vigente_desde, vigente_hasta, es_actual
           FROM {T('silver_clientes')}"""))
    plan.append(("vista vw_cuarentena_protegida", f"""
        CREATE OR REPLACE VIEW {T('vw_cuarentena_protegida')}
        COMMENT 'Cuarentena con el registro original redactado para clientes salvo para {grupo_pii}'
        AS SELECT dataset, clave_negocio, regla_id, reglas_incumplidas, severidad, dimension, descripcion,
                  CASE WHEN dataset = 'clientes' AND NOT {es_lector_fn}() THEN '***REDACTADO: contiene datos personales***'
                       ELSE registro END AS registro,
                  _source_file, run_id, pipeline, fecha_corte, fecha_cuarentena, ultima_deteccion_ts, estado_gestion
           FROM {T('silver_cuarentena')}"""))
    if aplicar_column_masks:
        for tabla in ("silver_clientes", "bronze_clientes"):
            for col, fn in MASCARA_POR_COLUMNA.items():
                if tabla == "bronze_clientes" and fn == "fn_mask_fecha_nacimiento":
                    continue
                plan.append((f"column mask {tabla}.{col}",
                             f"ALTER TABLE {T(tabla)} ALTER COLUMN {col} SET MASK {T(fn)}"))
    return plan
