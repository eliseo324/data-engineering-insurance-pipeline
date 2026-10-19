from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import yaml

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from src.common.contract_compat import comparar_contratos, validar_cambio  # noqa: E402


def contenido_en_ref(ref: str, ruta: str) -> str | None:
    r = subprocess.run(["git", "show", f"{ref}:{ruta}"], capture_output=True, text=True, cwd=RAIZ)
    return r.stdout if r.returncode == 0 else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/dll", help="Referencia git de la rama destino")
    args = parser.parse_args()

    errores, revisados = [], 0
    for path in sorted((RAIZ / "conf" / "contracts").glob("*.yml")):
        rel = path.relative_to(RAIZ).as_posix()
        previo = contenido_en_ref(args.base, rel)
        if previo is None:
            print(f"[nuevo]  {rel}: contrato nuevo, sin versión previa que comparar")
            continue
        anterior, nuevo = yaml.safe_load(previo), yaml.safe_load(path.read_text(encoding="utf-8"))
        nivel, cambios = comparar_contratos(anterior, nuevo)
        revisados += 1
        errores_contrato = validar_cambio(anterior, nuevo)
        estado = "sin cambios" if nivel == "ninguno" else f"cambio {nivel} ({anterior.get('version')} -> {nuevo.get('version')})"
        print(f"{'[error]' if errores_contrato else '[ok]':<8} {rel}: {estado}")
        for c in cambios:
            print(f"           {c}")
        errores += errores_contrato

    for e in errores:
        print(f"::error::{e}")
    print(f"{revisados} contrato(s) comparado(s) contra {args.base}; {len(errores)} error(es)")
    return 1 if errores else 0


if __name__ == "__main__":
    sys.exit(main())
