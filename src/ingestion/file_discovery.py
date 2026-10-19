from __future__ import annotations

import fnmatch
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from src.common.hashing import file_sha256


@dataclass(frozen=True)
class SourceFile:
    path: str
    name: str
    size_bytes: int
    modified_at: datetime
    sha256: str


def discover_files(landing_path: str, pattern: str = "*.csv", recursive: bool = True) -> list[SourceFile]:
    """Lista los archivos que cumplen el patrón, ordenados por fecha de modificación y nombre.

    Se ignoran archivos ocultos o temporales (prefijos '.' o '_'), p. ej. `_SUCCESS`.
    """
    if not os.path.isdir(landing_path):
        return []
    found: list[SourceFile] = []
    for root, dirs, files in os.walk(landing_path):
        dirs[:] = sorted(d for d in dirs if not d.startswith((".", "_")))
        for fname in sorted(files):
            if fname.startswith((".", "_")) or not fnmatch.fnmatch(fname.lower(), pattern.lower()):
                continue
            full = os.path.join(root, fname)
            st = os.stat(full)
            found.append(
                SourceFile(
                    path=full,
                    name=fname,
                    size_bytes=st.st_size,
                    modified_at=datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).replace(tzinfo=None),
                    sha256=file_sha256(full),
                )
            )
        if not recursive:
            break
    return sorted(found, key=lambda f: (f.modified_at, f.name))


def select_files_to_process(
    files: list[SourceFile], already_processed_hashes: set[str], modo: str
) -> tuple[list[SourceFile], list[SourceFile]]:
    """Separa archivos a procesar y omitidos.

    * modo normal: se omite todo archivo cuyo contenido (hash) ya fue ingerido, aunque cambie de
      nombre. También se omiten copias idénticas dentro del mismo lote.
    * modo reproceso: se reprocesan todos; la escritura por hash garantiza que no se duplique.
    """
    to_process: list[SourceFile] = []
    skipped: list[SourceFile] = []
    seen: set[str] = set()
    for f in files:
        if f.sha256 in seen or (modo == "normal" and f.sha256 in already_processed_hashes):
            skipped.append(f)
            continue
        seen.add(f.sha256)
        to_process.append(f)
    return to_process, skipped
