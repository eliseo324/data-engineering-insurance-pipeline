from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.contracts import REPO_ROOT, Contrato

SEVERIDADES = ("bloqueo", "advertencia", "informativa")
ACCIONES = ("cuarentena", "marcar")
DIMENSIONES = ("completitud", "unicidad", "validez", "consistencia", "integridad_referencial", "oportunidad")


class RuleConfigError(ValueError):
    """Regla mal declarada."""


class DataQualityError(RuntimeError):
    """Se superó el umbral de una regla de severidad bloqueo: el pipeline se detiene."""


@dataclass(frozen=True)
class Regla:
    id: str
    dataset: str
    dimension: str
    descripcion: str
    expresion: str
    severidad: str
    accion: str
    umbral_pct: float
    origen: str = "negocio"

    def __post_init__(self) -> None:
        if self.severidad not in SEVERIDADES:
            raise RuleConfigError(f"{self.id}: severidad '{self.severidad}' inválida")
        if self.accion not in ACCIONES:
            raise RuleConfigError(f"{self.id}: accion '{self.accion}' inválida")
        if self.dimension not in DIMENSIONES:
            raise RuleConfigError(f"{self.id}: dimension '{self.dimension}' inválida")


@dataclass(frozen=True)
class ConfigCalidad:
    reglas: tuple[Regla, ...]
    umbral_cuarentena_pct: float


def _sql_literal(v: str) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def reglas_automaticas(contrato: Contrato, defaults: dict | None = None) -> list[Regla]:
    """Genera reglas a partir del contrato. La completitud de la clave de negocio es de bloqueo."""
    d = defaults or {}
    umbral = float(d.get("umbral_pct_automaticas", 2.0))
    umbral_clave = float(d.get("umbral_pct_clave", 1.0))
    ds = contrato.dataset
    reglas: list[Regla] = []
    for c in contrato.columnas:
        col = f"`{c.nombre}`"
        if c.requerido:
            es_clave = c.nombre in contrato.clave_negocio
            reglas.append(Regla(
                id=f"{ds}.COMP.{c.nombre}", dataset=ds, dimension="completitud",
                descripcion=f"{c.nombre} es obligatorio" + (" (clave de negocio)" if es_clave else ""),
                expresion=f"{col} IS NOT NULL",
                severidad="bloqueo" if es_clave else "advertencia", accion="cuarentena",
                umbral_pct=umbral_clave if es_clave else umbral, origen="contrato"))
        if c.tipo != "string":
            reglas.append(Regla(
                id=f"{ds}.FMT.{c.nombre}", dataset=ds, dimension="validez",
                descripcion=f"{c.nombre} debe ser convertible a {c.tipo}",
                expresion=f"NOT (_raw.{col} IS NOT NULL AND trim(_raw.{col}) <> '' AND {col} IS NULL)",
                severidad="advertencia", accion="cuarentena", umbral_pct=umbral, origen="contrato"))
        if c.valores_permitidos:
            valores = ", ".join(_sql_literal(v) for v in c.valores_permitidos)
            reglas.append(Regla(
                id=f"{ds}.DOM.{c.nombre}", dataset=ds, dimension="validez",
                descripcion=f"{c.nombre} debe estar en el dominio {list(c.valores_permitidos)}",
                expresion=f"{col} IN ({valores})",
                severidad="advertencia", accion="cuarentena", umbral_pct=umbral, origen="contrato"))
        if c.patron:
            reglas.append(Regla(
                id=f"{ds}.PAT.{c.nombre}", dataset=ds, dimension="validez",
                descripcion=f"{c.nombre} debe cumplir el patrón {c.patron}",
                expresion=f"{col} RLIKE {_sql_literal(c.patron)}",
                severidad="advertencia", accion="cuarentena", umbral_pct=umbral, origen="contrato"))
    return reglas


def _resolver_placeholders(expr: str, fecha_corte: date) -> str:
    return expr.replace("${fecha_corte}", f"DATE'{fecha_corte.isoformat()}'")


def cargar_config_calidad(
    contrato: Contrato, fecha_corte: date, path: str | Path = "conf/quality_rules.yml", raw: dict | None = None
) -> ConfigCalidad:
    """Combina reglas automáticas + reglas de negocio + overrides para un dataset."""
    if raw is None:
        import yaml

        p = Path(path)
        if not p.is_absolute():
            p = REPO_ROOT / p
        with open(p, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    defaults = raw.get("defaults", {})
    cfg_ds = (raw.get("datasets") or {}).get(contrato.dataset, {}) or {}

    reglas = {r.id: r for r in reglas_automaticas(contrato, defaults)}
    for r in cfg_ds.get("reglas", []):
        if r["id"] in reglas:
            raise RuleConfigError(f"Regla duplicada: {r['id']}")
        if not r["id"].startswith(f"{contrato.dataset}."):
            raise RuleConfigError(f"{r['id']}: el id debe iniciar con '{contrato.dataset}.'")
        reglas[r["id"]] = Regla(
            id=r["id"], dataset=contrato.dataset, dimension=r["dimension"], descripcion=r["descripcion"],
            expresion=" ".join(str(r["expresion"]).split()), severidad=r["severidad"], accion=r["accion"],
            umbral_pct=float(r.get("umbral_pct", 0)),
        )
    for rid, cambios in (cfg_ds.get("overrides") or {}).items():
        if rid not in reglas:
            raise RuleConfigError(f"Override sobre regla inexistente: {rid}")
        reglas[rid] = replace(reglas[rid], **cambios)

    resueltas = tuple(replace(r, expresion=_resolver_placeholders(r.expresion, fecha_corte)) for r in reglas.values())
    umbral = float(cfg_ds.get("umbral_cuarentena_pct", defaults.get("umbral_cuarentena_pct", 5.0)))
    return ConfigCalidad(reglas=resueltas, umbral_cuarentena_pct=umbral)


def aplicar_reglas(df: DataFrame, reglas: tuple[Regla, ...] | list[Regla]) -> DataFrame:
    """Evalúa todas las reglas y agrega `_dq_fallas`, `_dq_cuarentena` y `_dq_advertencias`.

    Una expresión que evalúa NULL se considera cumplida (cada regla mide una sola dimensión).
    """
    def falla(r: Regla):
        return ~F.coalesce(F.expr(r.expresion), F.lit(True))

    def ids(subset):
        if not subset:
            return F.array().cast("array<string>")
        return F.filter(F.array(*[F.when(falla(r), F.lit(r.id)) for r in subset]), lambda x: x.isNotNull())

    return (
        df.withColumn("_dq_fallas", ids(list(reglas)))
        .withColumn("_dq_cuarentena", ids([r for r in reglas if r.accion == "cuarentena"]))
        .withColumn("_dq_advertencias", ids([r for r in reglas if r.accion == "marcar"]))
    )


def resultado_regla(regla: Regla, fallidos: int, evaluados: int) -> tuple[float, str]:
    """Calcula % de fallo y resultado (OK | INFORMATIVA | ADVERTENCIA | FALLA) según severidad y umbral."""
    pct = round(100.0 * fallidos / evaluados, 4) if evaluados else 0.0
    if fallidos == 0:
        return pct, "OK"
    if regla.severidad == "informativa":
        return pct, "INFORMATIVA"
    if pct <= regla.umbral_pct:
        return pct, "OK_DENTRO_UMBRAL"
    return pct, {"bloqueo": "FALLA", "advertencia": "ADVERTENCIA", "informativa": "INFORMATIVA"}[regla.severidad]


def resumir_reglas(df_eval: DataFrame, reglas, evaluados: int) -> list[dict]:
    """Cuenta fallos por regla (una agregación distribuida; el resultado es una fila por regla)."""
    conteos = {
        r["regla"]: int(r["n"])
        for r in df_eval.select(F.explode("_dq_fallas").alias("regla")).groupBy("regla").count()
        .withColumnRenamed("count", "n").collect()
    }
    out = []
    for r in reglas:
        n = conteos.get(r.id, 0)
        pct, res = resultado_regla(r, n, evaluados)
        out.append({
            "regla_id": r.id, "dimension": r.dimension, "descripcion": r.descripcion, "severidad": r.severidad,
            "accion": r.accion, "registros_evaluados": evaluados, "registros_fallidos": n, "pct_fallo": pct,
            "umbral_pct": r.umbral_pct, "resultado": res,
        })
    return out


def verificar_bloqueos(resultados: list[dict], dataset: str) -> None:
    """Lanza DataQualityError si alguna regla de bloqueo superó su umbral."""
    fallas = [r for r in resultados if r["resultado"] == "FALLA"]
    if fallas:
        detalle = "; ".join(f"{r['regla_id']} {r['pct_fallo']}% > {r['umbral_pct']}%" for r in fallas)
        raise DataQualityError(
            f"[{dataset}] Calidad bloqueante incumplida: {detalle}. Silver no se actualizó; "
            f"los registros rechazados están en silver_cuarentena y el detalle en ctl_calidad_resultados."
        )
