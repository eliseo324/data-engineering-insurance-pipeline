from __future__ import annotations

import posixpath
import re
from pathlib import Path

from src.common.contracts import REPO_ROOT

JOB_FILE = "resources/jobs/insurance_pipeline.job.yml"
CRON_ESPERADO = "0 0 6 5 * ?"
ZONA_ESPERADA = "America/Bogota"
MAX_TAREAS_CONCURRENTES = 5  


class JobSpecError(ValueError):
    """Definición del job inválida."""


def _sustituir(texto: str, variables: dict, usuario: str) -> str:
    texto = texto.replace("${workspace.current_user.userName}", usuario)

    def var(m: re.Match) -> str:
        nombre = m.group(1)
        if nombre not in variables:
            raise JobSpecError(f"Variable sin valor: ${{var.{nombre}}}")
        return str(variables[nombre])

    return re.sub(r"\$\{var\.([a-zA-Z0-9_]+)\}", var, texto)


def cargar_spec_job(
    variables: dict, usuario: str, repo_root_workspace: str, job_file: str = JOB_FILE
) -> dict:
    """Lee el YAML del job, sustituye variables y resuelve rutas de notebooks al Git folder.

    `repo_root_workspace`: ruta del repositorio en el workspace, p. ej. /Users/yo@x.com/mi-repo.
    """
    import yaml

    path = Path(job_file) if Path(job_file).is_absolute() else REPO_ROOT / job_file
    raw = yaml.safe_load(_sustituir(path.read_text(encoding="utf-8"), variables, usuario))
    jobs = raw["resources"]["jobs"]
    if len(jobs) != 1:
        raise JobSpecError("Se espera exactamente un job en el archivo")
    spec = dict(next(iter(jobs.values())))

    dir_rel = posixpath.dirname(path.relative_to(REPO_ROOT).as_posix())
    root = repo_root_workspace.rstrip("/").removeprefix("/Workspace")
    for t in spec["tasks"]:
        nb = t.get("notebook_task")
        if nb:
            rel = posixpath.normpath(posixpath.join(dir_rel, nb["notebook_path"]))
            nb["notebook_path"] = f"{root}/{rel.removesuffix('.py')}"
            nb["source"] = "WORKSPACE"
    validar_dag(spec)
    return spec


def orden_topologico(tareas: list[dict]) -> list[list[str]]:
    """Agrupa tareas por nivel: cada nivel solo depende de niveles anteriores. Detecta ciclos."""
    deps = {t["task_key"]: {d["task_key"] for d in t.get("depends_on", [])} for t in tareas}
    faltantes = {d for ds in deps.values() for d in ds} - set(deps)
    if faltantes:
        raise JobSpecError(f"Dependencias inexistentes: {sorted(faltantes)}")
    niveles, hechas = [], set()
    while len(hechas) < len(deps):
        nivel = sorted(k for k, ds in deps.items() if k not in hechas and ds <= hechas)
        if not nivel:
            raise JobSpecError(f"Ciclo en el DAG entre: {sorted(set(deps) - hechas)}")
        niveles.append(nivel)
        hechas |= set(nivel)
    return niveles


def ancestros(tareas: list[dict], clave: str) -> set[str]:
    deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in tareas}
    out, pila = set(), list(deps[clave])
    while pila:
        k = pila.pop()
        if k not in out:
            out.add(k)
            pila.extend(deps[k])
    return out


def validar_dag(spec: dict) -> None:
    tareas = spec["tasks"]
    claves = [t["task_key"] for t in tareas]
    if len(claves) != len(set(claves)):
        raise JobSpecError("task_key duplicado")
    niveles = orden_topologico(tareas)
    if max(len(n) for n in niveles) > MAX_TAREAS_CONCURRENTES:
        raise JobSpecError(f"Un nivel del DAG supera {MAX_TAREAS_CONCURRENTES} tareas concurrentes")
    reglas = {
        "silver_polizas": {"silver_clientes"},
        "silver_recaudos": {"silver_polizas"},
        "silver_siniestros": {"silver_polizas"},
        "silver_movimientos": {"silver_clientes", "silver_polizas", "silver_recaudos", "silver_siniestros"},
        "gold_productos": {"silver_movimientos"},
        "publicacion": {"validaciones", "gold_productos"},
        "observabilidad": {"publicacion"},
    }
    for tarea, requeridas in reglas.items():
        if tarea in claves and not requeridas <= ancestros(tareas, tarea):
            raise JobSpecError(f"{tarea} debe ejecutarse después de {sorted(requeridas)}")
    obs = next((t for t in tareas if t["task_key"] == "observabilidad"), None)
    if obs and obs.get("run_if") != "ALL_DONE":
        raise JobSpecError("observabilidad debe usar run_if: ALL_DONE para registrar también las ejecuciones fallidas")
    sched = spec.get("schedule", {})
    if sched.get("quartz_cron_expression") != CRON_ESPERADO or sched.get("timezone_id") != ZONA_ESPERADA:
        raise JobSpecError("El schedule debe ser el día 5 de cada mes a las 06:00 America/Bogota")
    nombres = {p["name"] for p in spec.get("parameters", [])}
    if not {"fecha_corte", "modo", "job_run_id"} <= nombres:
        raise JobSpecError("Faltan parámetros obligatorios del job (fecha_corte, modo, job_run_id)")