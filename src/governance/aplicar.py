from __future__ import annotations


def ejecutar_plan(spark, plan: list[tuple[str, str]], modo: str, logger) -> list[dict]:
    resultados = []
    for desc, sql in plan:
        sql_limpio = " ".join(sql.split())
        if modo != "aplicar":
            resultados.append({"accion": desc, "estado": "PLAN", "detalle": sql_limpio})
            continue
        try:
            spark.sql(sql)
            resultados.append({"accion": desc, "estado": "OK", "detalle": sql_limpio})
        except Exception as exc:  
            msg = str(exc).split("\n")[0][:300]
            logger.warning(f"Gobierno: '{desc}' no se aplicó: {msg}")
            resultados.append({"accion": desc, "estado": "ERROR", "detalle": f"{msg} | {sql_limpio}"})
    return resultados


def asegurar_grupos(w, conf: dict, logger) -> list[dict]:
    """Crea los grupos declarados (si no existen) y agrega sus miembros. Requiere ser administrador."""
    from databricks.sdk.service import iam

    out = []
    for nombre, meta in conf["grupos"].items():
        try:
            existentes = list(w.groups.list(filter=f'displayName eq "{nombre}"'))
            grupo = existentes[0] if existentes else w.groups.create(display_name=nombre)
            ids_actuales = {m.value for m in (grupo.members or [])}
            for correo in meta.get("miembros", []):
                usuarios = list(w.users.list(filter=f'userName eq "{correo}"'))
                if not usuarios:
                    logger.warning(f"Usuario {correo} no existe en el workspace; agréguelo antes (grupo {nombre})")
                    continue
                if usuarios[0].id not in ids_actuales:
                    w.groups.patch(id=grupo.id, operations=[iam.Patch(
                        op=iam.PatchOp.ADD, path="members", value=[{"value": usuarios[0].id}])],
                        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP])
            out.append({"accion": f"grupo {nombre}", "estado": "OK", "detalle": "creado" if not existentes else "existente"})
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"No se pudo gestionar el grupo {nombre}: {exc}")
            out.append({"accion": f"grupo {nombre}", "estado": "ERROR", "detalle": str(exc)[:300]})
    return out
