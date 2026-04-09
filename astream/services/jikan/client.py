"""
Client Jikan API v4 — https://docs.api.jikan.moe/
Rate limit officiel : 3 req/s, 60 req/min.
Toutes les réponses sont mises en cache via CacheManager.
"""
import asyncio
import time
from typing import Optional, Dict, List, Any

from astream.utils.http_client import http_client, safe_json_decode
from astream.utils.cache import CacheManager
from astream.utils.logger import logger

JIKAN_BASE_URL = "https://api.jikan.moe/v4"

# ===========================
# Rate limiter : 3 req/s max
# ===========================
_jikan_lock = asyncio.Lock()
_last_request_time: float = 0.0
_MIN_INTERVAL = 0.4  # 400ms entre deux requêtes = 2.5 req/s (marge de sécurité)


class JikanClient:
    """
    Client HTTP pour l'API Jikan v4.
    - Rate limiting intégré (3 req/s)
    - Cache via CacheManager (SQLite/PostgreSQL)
    - Retry automatique sur 429
    """

    def __init__(self):
        self.base_url = JIKAN_BASE_URL
        self.client = http_client

    async def _request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Effectue une requête rate-limitée vers l'API Jikan."""
        global _last_request_time

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        async with _jikan_lock:
            elapsed = time.monotonic() - _last_request_time
            if elapsed < _MIN_INTERVAL:
                await asyncio.sleep(_MIN_INTERVAL - elapsed)

            try:
                response = await self.client.get(url, params=params or {})
                _last_request_time = time.monotonic()

                # Rate limit atteint → attente et retry
                if response.status_code == 429:
                    logger.warning("JIKAN: Rate limit 429 — pause 2s")
                    await asyncio.sleep(2.0)
                    response = await self.client.get(url, params=params or {})
                    _last_request_time = time.monotonic()

                if response.status_code == 404:
                    return None

                response.raise_for_status()
                return safe_json_decode(response, f"Jikan /{endpoint}", default=None)

            except Exception as e:
                logger.error(f"JIKAN: Erreur requête /{endpoint}: {e}")
                return None

    # ===========================
    # Planning du jour
    # ===========================
    async def get_schedules(self, day: str) -> List[Dict]:
        """
        GET /schedules?filter=<day>
        Anime diffusés un jour précis de la semaine.
        day: monday, tuesday, wednesday, thursday, friday, saturday, sunday
        """
        cache_key = f"jikan:schedule:{day}"

        async def fetch():
            data = await self._request("schedules", {"filter": day, "limit": 25})
            return data.get("data", []) if data else []

        try:
            return await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch,
                lock_key=f"lock:{cache_key}",
                ttl=86400,
            ) or []
        except Exception as e:
            logger.error(f"JIKAN: get_schedules({day}): {e}")
            return []

    # ===========================
    # Simulcasts en cours
    # ===========================
    async def get_airing(self, limit: int = 25) -> List[Dict]:
        """
        GET /anime?status=airing&order_by=score
        Anime TV actuellement en cours de diffusion, triés par score.
        """
        cache_key = f"jikan:airing:{limit}"

        async def fetch():
            data = await self._request("anime", {
                "status": "airing",
                "type": "tv",
                "order_by": "score",
                "sort": "desc",
                "limit": limit,
                "min_score": 5,
            })
            return data.get("data", []) if data else []

        try:
            return await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch,
                lock_key=f"lock:{cache_key}",
                ttl=86400,
            ) or []
        except Exception as e:
            logger.error(f"JIKAN: get_airing: {e}")
            return []

    # ===========================
    # Films d'anime
    # ===========================
    async def get_movies(self, limit: int = 25) -> List[Dict]:
        """
        GET /anime?type=movie&order_by=score
        Films d'anime triés par score.
        """
        cache_key = f"jikan:movies:{limit}"

        async def fetch():
            data = await self._request("anime", {
                "type": "movie",
                "order_by": "score",
                "sort": "desc",
                "limit": limit,
                "min_score": 6,
            })
            return data.get("data", []) if data else []

        try:
            return await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch,
                lock_key=f"lock:{cache_key}",
                ttl=86400,
            ) or []
        except Exception as e:
            logger.error(f"JIKAN: get_movies: {e}")
            return []

    # ===========================
    # Top Anime
    # ===========================
    async def get_top_anime(self, filter_type: str = "bypopularity", limit: int = 25) -> List[Dict]:
        """
        GET /top/anime?filter=<filter_type>
        filter_type: airing | upcoming | bypopularity | favorite
        """
        cache_key = f"jikan:top:{filter_type}:{limit}"

        async def fetch():
            data = await self._request("top/anime", {
                "filter": filter_type,
                "limit": limit,
            })
            return data.get("data", []) if data else []

        try:
            return await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch,
                lock_key=f"lock:{cache_key}",
                ttl=86400,
            ) or []
        except Exception as e:
            logger.error(f"JIKAN: get_top_anime({filter_type}): {e}")
            return []

    # ===========================
    # Anime par genre
    # ===========================
    async def get_anime_by_genre(self, genre_id: int, limit: int = 25) -> List[Dict]:
        """
        GET /anime?genres=<id>&order_by=score
        """
        cache_key = f"jikan:genre:{genre_id}:{limit}"

        async def fetch():
            data = await self._request("anime", {
                "genres": str(genre_id),
                "order_by": "score",
                "sort": "desc",
                "limit": limit,
                "min_score": 5,
            })
            return data.get("data", []) if data else []

        try:
            return await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch,
                lock_key=f"lock:{cache_key}",
                ttl=86400,
            ) or []
        except Exception as e:
            logger.error(f"JIKAN: get_anime_by_genre({genre_id}): {e}")
            return []

    # ===========================
    # Recherche
    # ===========================
    async def search_anime(self, query: str, genre_id: Optional[int] = None, limit: int = 25) -> List[Dict]:
        """
        GET /anime?q=<query>
        Recherche par titre avec prise en charge des titres Romaji/alternatifs.
        """
        cache_key = f"jikan:search:{query}:{genre_id}"

        async def fetch():
            params: Dict[str, Any] = {
                "q": query,
                "limit": limit,
                "order_by": "score",
                "sort": "desc",
            }
            if genre_id:
                params["genres"] = str(genre_id)
            data = await self._request("anime", params)
            return data.get("data", []) if data else []

        try:
            return await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch,
                lock_key=f"lock:{cache_key}",
                ttl=1800,
            ) or []
        except Exception as e:
            logger.error(f"JIKAN: search_anime('{query}'): {e}")
            return []

    # ===========================
    # Détail d'un anime par MAL ID
    # ===========================
    async def get_anime_by_id(self, mal_id: int) -> Optional[Dict]:
        """GET /anime/{mal_id}/full"""
        cache_key = f"jikan:anime:{mal_id}"

        async def fetch():
            data = await self._request(f"anime/{mal_id}/full")
            return data.get("data") if data else None

        try:
            return await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch,
                lock_key=f"lock:{cache_key}",
                ttl=86400,
            )
        except Exception as e:
            logger.error(f"JIKAN: get_anime_by_id({mal_id}): {e}")
            return None

    # ===========================
    # Liste des genres Jikan
    # ===========================
    async def get_genres(self) -> List[Dict]:
        """GET /genres/anime — liste complète des genres MAL"""
        cache_key = "jikan:genres"

        async def fetch():
            data = await self._request("genres/anime")
            return data.get("data", []) if data else []

        try:
            return await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch,
                lock_key="lock:jikan:genres",
                ttl=604800,  # 7 jours
            ) or []
        except Exception as e:
            logger.error(f"JIKAN: get_genres: {e}")
            return []


# ===========================
# Instance Singleton Globale
# ===========================
jikan_client = JikanClient()
