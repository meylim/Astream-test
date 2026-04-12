"""
Client Cinemeta — API officielle de Stremio.

Cinemeta est la source de vérité pour :
  - Structure des saisons et épisodes (numéros, titres, dates de diffusion, thumbnails)
  - Métadonnées de base (poster, synopsis anglais, note IMDb, durée, cast)
  - Identifiants IMDb (tt...)

Endpoint : https://v3-cinemeta.strem.io
  /catalog/series/top/search={query}.json  — recherche
  /meta/series/{imdb_id}.json              — détails série avec saisons/épisodes
  /meta/movie/{imdb_id}.json               — détails film

Toutes les réponses sont cachées 7 jours (données stables).
"""
import asyncio
import re
from typing import Optional, Dict, List, Any

from astream.utils.http_client import http_client, safe_json_decode
from astream.utils.cache import CacheManager
from astream.utils.logger import logger

CINEMETA_BASE = "https://v3-cinemeta.strem.io"


class CinemetaClient:
    """
    Client HTTP pour l'API Cinemeta de Stremio.
    - Cache 7 jours (données très stables)
    - Pas de rate limit officiel mais on espace les requêtes
    """

    def __init__(self):
        self.base = CINEMETA_BASE

    async def _get(self, path: str, cache_key: str, ttl: int = 604800) -> Optional[Dict]:
        """Requête GET avec cache."""
        async def fetch():
            url = f"{self.base}{path}"
            try:
                resp = await http_client.get(url)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return safe_json_decode(resp, f"Cinemeta {path}", default=None)
            except Exception as e:
                logger.error(f"CINEMETA: {path} → {e}")
                return None

        try:
            return await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch,
                lock_key=f"lock:{cache_key}",
                ttl=ttl,
            )
        except Exception as e:
            logger.error(f"CINEMETA cache: {e}")
            return None

    # ===========================
    # Récupérer les méta complètes par IMDb ID
    # ===========================
    async def get_meta(self, imdb_id: str, media_type: str = "series") -> Optional[Dict]:
        """
        GET /meta/{type}/{imdb_id}.json
        Retourne : poster, background, description, runtime, cast, genres,
                   videos[] avec season/episode/title/aired/thumbnail pour chaque épisode.
        media_type : "series" ou "movie"
        """
        if not imdb_id or not imdb_id.startswith("tt"):
            return None

        cache_key = f"cinemeta:meta:{media_type}:{imdb_id}"
        data = await self._get(f"/meta/{media_type}/{imdb_id}.json", cache_key, ttl=604800)

        if not data:
            return None

        meta = data.get("meta")
        if not meta:
            return None

        logger.log("CINEMETA", f"Meta {imdb_id} ({media_type}): {len(meta.get('videos', []))} épisodes")
        return meta

    # ===========================
    # Recherche brute (sans validation Kitsu) — pour résolution Adkami
    # ===========================
    async def search_raw(self, query: str, limit: int = 5) -> List[Dict]:
        """
        Recherche Cinemeta SANS validation Kitsu.
        Retourne le top 1 résultat série + top 1 film.
        Utilisée pour résoudre des titres Adkami (déjà confirmés anime)
        vers un tt* IMDb.
        """
        if not query or len(query.strip()) < 2:
            return []

        safe_query = query.strip().replace("/", " ")

        series_data, movie_data = await asyncio.gather(
            self._get(
                f"/catalog/series/top/search={safe_query}.json",
                cache_key=f"cinemeta:search:series:{safe_query.lower()}",
                ttl=3600,
            ),
            self._get(
                f"/catalog/movie/top/search={safe_query}.json",
                cache_key=f"cinemeta:search:movie:{safe_query.lower()}",
                ttl=3600,
            ),
        )

        all_metas: List[Dict] = []
        all_metas += (series_data or {}).get("metas", [])
        all_metas += (movie_data or {}).get("metas", [])

        return all_metas[:limit]

    # ===========================
    # Recherche (anime uniquement via validation croisée Kitsu)
    # ===========================
    async def search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        GET /catalog/series/top/search={query}.json  (séries)
        GET /catalog/movie/top/search={query}.json   (films)

        Retourne une liste de méta filtrées aux anime, validées par Kitsu.
        La validation croisée remplace l'ancien filtre heuristique basé sur
        le pays/genre : chaque résultat Cinemeta est soumis à is_valid_anime_kitsu()
        qui vérifie son existence dans la base Kitsu (via IMDb ID ou recherche textuelle).
        """
        # Import ici pour éviter les imports circulaires au chargement du module
        from astream.services.kitsu.validator import is_valid_anime_kitsu

        if not query or len(query.strip()) < 2:
            return []

        safe_query = query.strip().replace("/", " ")

        # --- Requêtes parallèles : séries + films ---
        series_data, movie_data = await asyncio.gather(
            self._get(
                f"/catalog/series/top/search={safe_query}.json",
                cache_key=f"cinemeta:search:series:{safe_query.lower()}",
                ttl=3600,
            ),
            self._get(
                f"/catalog/movie/top/search={safe_query}.json",
                cache_key=f"cinemeta:search:movie:{safe_query.lower()}",
                ttl=3600,
            ),
        )

        all_metas: List[Dict] = []
        all_metas += (series_data or {}).get("metas", [])
        all_metas += (movie_data  or {}).get("metas", [])

        if not all_metas:
            return []

        # --- Validation Kitsu en parallèle ---
        validations = await asyncio.gather(
            *[is_valid_anime_kitsu(safe_query, m) for m in all_metas],
            return_exceptions=True,
        )

        anime_results: List[Dict] = []
        for meta, result in zip(all_metas, validations):
            if isinstance(result, Exception):
                logger.warning(f"KITSU validation error for '{meta.get('name')}': {result}")
                continue
            is_ok, reason = result
            if is_ok:
                anime_results.append(meta)
                logger.debug(f"KITSU ✅ {meta.get('name')} → {reason}")
            else:
                logger.debug(f"KITSU ❌ {meta.get('name')} → {reason}")

        logger.log(
            "CINEMETA",
            f"Search '{query}': {len(all_metas)} résultats Cinemeta "
            f"→ {len(anime_results)} validés Kitsu"
        )
        return anime_results[:limit]

    # ===========================
    # Récupérer la structure des saisons depuis les vidéos Cinemeta
    # ===========================
    @staticmethod
    def extract_season_structure(videos: List[Dict]) -> Dict[int, List[Dict]]:
        """
        Organise les vidéos Cinemeta par saison.
        Retourne : {saison_num: [episode_dict, ...]}
        """
        structure: Dict[int, List[Dict]] = {}
        for video in videos:
            season = video.get("season")
            if season is None:
                continue
            if season not in structure:
                structure[season] = []
            structure[season].append(video)

        # Trier les épisodes dans chaque saison
        for season in structure:
            structure[season].sort(key=lambda v: v.get("episode", 0))

        return structure

    # ===========================
    # Récupérer un épisode précis
    # ===========================
    @staticmethod
    def get_episode(videos: List[Dict], season: int, episode: int) -> Optional[Dict]:
        """Retrouve un épisode précis dans la liste videos de Cinemeta."""
        for video in videos:
            if video.get("season") == season and video.get("episode") == episode:
                return video
        return None


# ===========================
# Instance Singleton Globale
# ===========================
cinemeta_client = CinemetaClient()
