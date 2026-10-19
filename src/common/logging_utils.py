"""Logging estructurado con contexto de ejecución (run_id, pipeline, dataset, capa).

Se usa `logging` en lugar de `print`: los mensajes quedan en los logs del driver del job,
con nivel, timestamp y contexto suficiente para diagnosticar un fallo.
"""

from __future__ import annotations

import logging
import sys

_FORMAT = (
    "%(asctime)s | %(levelname)-7s | %(name)s | run_id=%(run_id)s | "
    "capa=%(capa)s | dataset=%(dataset)s | %(message)s"
)


class _ContextAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = {**self.extra, **kwargs.pop("extra", {})}
        kwargs["extra"] = extra
        return msg, kwargs

    def with_context(self, **context) -> _ContextAdapter:
        return _ContextAdapter(self.logger, {**self.extra, **context})


def get_logger(name: str = "insurance_pipeline", level: int = logging.INFO, **context) -> _ContextAdapter:
    """Devuelve un logger con contexto. Idempotente: no duplica handlers al re-ejecutar celdas."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not any(getattr(h, "_insurance_handler", False) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT))
        handler._insurance_handler = True  # type: ignore[attr-defined]
        logger.addHandler(handler)
        logger.propagate = False
    base = {"run_id": "-", "capa": "-", "dataset": "-"}
    base.update({k: v for k, v in context.items() if v is not None})
    return _ContextAdapter(logger, base)
