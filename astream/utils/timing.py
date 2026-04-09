"""
Module de timing pour instrumenter les flux critiques d'AStream.
Permet de mesurer précisément chaque étape et d'identifier les goulots.

Usage :
    timer = FlowTimer("STREAM", episode_id)
    async with timed_step(timer, "cache_lookup"):
        result = await some_async_call()
    timer.finish()

    # Ou avec le décorateur :
    @timed_async("catalog._enrich")
    async def _enrich_catalog_with_tmdb(self, ...):
        ...
"""

import time
import functools
from contextlib import asynccontextmanager
from typing import Optional

from astream.utils.logger import logger


# ===========================
# FlowTimer — trace un flux complet
# ===========================
class FlowTimer:
    """Trace un flux complet avec sous-étapes chronométrées."""

    def __init__(self, flow_name: str, flow_id: str):
        self.flow_name = flow_name
        self.flow_id = flow_id
        self.start = time.monotonic()
        self.steps: list = []

    def step(self, name: str) -> None:
        """Marque le début d'une sous-étape."""
        # Clore l'étape précédente si pas encore fermée
        self._close_last_step()
        self.steps.append({"name": name, "start": time.monotonic()})

    def _close_last_step(self) -> None:
        """Ferme la dernière étape ouverte."""
        if self.steps and "duration" not in self.steps[-1]:
            s = self.steps[-1]
            s["duration"] = round((time.monotonic() - s["start"]) * 1000)

    def end_step(self) -> None:
        """Termine explicitement la dernière sous-étape."""
        self._close_last_step()

    def finish(self) -> int:
        """Termine le flux et loggue le résumé. Retourne la durée totale en ms."""
        self._close_last_step()
        total = round((time.monotonic() - self.start) * 1000)

        if self.steps:
            parts = " | ".join(
                f"{s['name']}={s.get('duration', '?')}ms"
                for s in self.steps
            )
            logger.log(
                "PERFORMANCE",
                f"[{self.flow_name}] {self.flow_id} → {total}ms total | {parts}"
            )
        else:
            logger.log(
                "PERFORMANCE",
                f"[{self.flow_name}] {self.flow_id} → {total}ms total"
            )

        return total


# ===========================
# Context manager async
# ===========================
@asynccontextmanager
async def timed_step(timer: FlowTimer, name: str):
    """Context manager pour chronométrer une étape de façon propre."""
    timer.step(name)
    try:
        yield
    finally:
        timer.end_step()


# ===========================
# Décorateur async
# ===========================
def timed_async(label: Optional[str] = None):
    """
    Décorateur pour logger automatiquement la durée d'une fonction async.

    Usage :
        @timed_async("catalog.enrich_tmdb")
        async def _enrich_catalog_with_tmdb(self, ...):
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            fname = label or func.__qualname__
            t0 = time.monotonic()
            try:
                result = await func(*args, **kwargs)
                elapsed = round((time.monotonic() - t0) * 1000)
                logger.log("PERFORMANCE", f"{fname} → {elapsed}ms")
                return result
            except Exception as e:
                elapsed = round((time.monotonic() - t0) * 1000)
                logger.log("PERFORMANCE", f"{fname} → ERREUR après {elapsed}ms: {type(e).__name__}")
                raise
        return wrapper
    return decorator
