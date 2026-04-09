from typing import Any, Optional, Dict
from contextlib import asynccontextmanager
from collections import defaultdict

from astream.utils.database import (
    get_metadata_from_cache,
    set_metadata_to_cache,
    delete_metadata_from_cache,
    get_cache_age,
    DistributedLock
)
from astream.utils.logger import logger


# ===========================
# Classe CacheKeys
# ===========================
class CacheKeys:

    @staticmethod
    def homepage() -> str:
        return "as:homepage"

    @staticmethod
    def anime_details(anime_slug: str) -> str:
        return f"as:{anime_slug}"

    @staticmethod
    def planning() -> str:
        return "as:planning"

    @staticmethod
    def planning_by_day() -> str:
        return "as:planning:by_day"


# ===========================
# Statistiques de cache
# ===========================
class CacheStats:

    def __init__(self):
        self.stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"hits": 0, "misses": 0})

    def record_hit(self, category: str):
        self.stats[category]["hits"] += 1

    def record_miss(self, category: str):
        self.stats[category]["misses"] += 1

    def get_summary(self) -> Dict[str, Dict[str, Any]]:
        summary = {}
        for category, counts in self.stats.items():
            total = counts["hits"] + counts["misses"]
            hit_rate = (counts["hits"] / total * 100) if total > 0 else 0
            summary[category] = {
                "hits": counts["hits"],
                "misses": counts["misses"],
                "total": total,
                "hit_rate": hit_rate
            }
        return summary

    def log_summary(self):
        summary = self.get_summary()
        if not summary:
            return

        for category, stats in summary.items():
            logger.info(
                f"{category}: {stats['hits']} hits, {stats['misses']} misses "
                f"({stats['hit_rate']:.1f}% hit rate, {stats['total']} total)"
            )

    def reset(self):
        self.stats.clear()


cache_stats = CacheStats()


# ===========================
# Gestionnaire de cache
# ===========================
class CacheManager:

    @staticmethod
    async def get(cache_key: str) -> Optional[Any]:
        cached_data = await get_metadata_from_cache(cache_key)
        if cached_data:
            return cached_data
        return None

    @staticmethod
    async def set(cache_key: str, data: Any, ttl: Optional[int] = None) -> None:
        await set_metadata_to_cache(cache_key, data, ttl)

    @staticmethod
    async def invalidate(cache_key: str) -> None:
        try:
            await delete_metadata_from_cache(cache_key)
            logger.log("DATABASE", f"Cache invalidé: {cache_key}")
        except Exception as e:
            logger.warning(f"Erreur invalidation cache {cache_key}: {e}")

    @staticmethod
    async def invalidate_if_older_than(cache_key: str, max_age_seconds: int) -> bool:
        """
        Invalide le cache si l'entrée est plus vieille que max_age_seconds.
        Retourne True si le cache a été invalidé, False sinon.

        Utilisé par les sorties du jour pour forcer un refresh de la homepage
        si elle est trop vieille — les épisodes sortent tout au long de la journée.
        """
        try:
            age = await get_cache_age(cache_key)
            if age > max_age_seconds:
                await delete_metadata_from_cache(cache_key)
                logger.log("DATABASE", f"Cache {cache_key} invalidé (âge: {int(age)}s > max: {max_age_seconds}s)")
                return True
            return False
        except Exception as e:
            logger.warning(f"Erreur vérification âge cache {cache_key}: {e}")
            return False

    @staticmethod
    @asynccontextmanager
    async def with_lock(lock_key: str, instance_id: Optional[str] = None):
        async with DistributedLock(lock_key, instance_id):
            yield

    @staticmethod
    async def get_or_fetch(
        cache_key: str,
        fetch_func,
        lock_key: Optional[str] = None,
        ttl: Optional[int] = None,
        instance_id: Optional[str] = None
    ) -> Any:
        cached = await CacheManager.get(cache_key)
        if cached is not None:
            return cached

        if lock_key:
            async with CacheManager.with_lock(lock_key, instance_id):
                cached = await CacheManager.get(cache_key)
                if cached is not None:
                    return cached

                data = await fetch_func()
                if data:
                    await CacheManager.set(cache_key, data, ttl)
                return data
        else:
            data = await fetch_func()
            if data:
                await CacheManager.set(cache_key, data, ttl)
            return data
          
