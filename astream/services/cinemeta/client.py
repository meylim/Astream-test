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
import re
from typing import Optional, Dict, List, Any

from astream.utils.http_client import http_client, safe_json_decode
from astream.utils.cache import CacheManager
from astream.utils.logger import logger

CINEMETA_BASE = "https://v3-cinemeta.strem.io"

# Genres et mots-clés qui indiquent qu'un résultat Cinemeta est bien un anime
# (Cinemeta mélange anime + séries live dans sa recherche)
_ANIME_COUNTRY_CODES = {"JP", "KR"}  # Japon principalement, Corée pour webtoons
_ANIME_GENRES = {
    "animation", "anime", "animated", "cartoon"
}


def _is_likely_anime(meta: Dict) -> bool:
    """
    Filtre heuristique pour garder uniquement les anime dans les résultats Cinemeta.
    Vérifie : country, genre, language.
    """
    genres = [g.lower() for g in meta.get("genres", [])]
    if any(g in _ANIME_GENRES for g in genres):
        return True

    country = (meta.get("country") or "").upper()
    if country in _ANIME_COUNTRY_CODES:
        return True

    # Certains anime n'ont pas de genre "animation" mais ont "Japan" dans country_codes
    if isinstance(meta.get("country"), list):
        countries = [c.upper() for c in meta.get("country", [])]
        if any(c in _ANIME_COUNTRY_CODES for c in countries):
            return True

    return False


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
    # Recherche (anime uniquement via filtre heuristique)
    # ===========================
    async def search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        GET /catalog/series/top/search={query}.json
        Retourne une liste de méta filtrées aux anime.
        Note : Cinemeta n'est pas anime-only, on filtre les résultats.
        """
        if not query or len(query.strip()) < 2:
            return []

        safe_query = query.strip().replace("/", " ")
        cache_key = f"cinemeta:search:{safe_query.lower()}"

        data = await self._get(
            f"/catalog/series/top/search={safe_query}.json",
            cache_key,
            ttl=3600,  # Recherches cachées 1h
        )

        if not data:
            return []

        metas = data.get("metas", [])
        # Filtrer aux anime uniquement
        anime_results = [m for m in metas if _is_likely_anime(m)]
        logger.log("CINEMETA", f"Search '{query}': {len(metas)} résultats → {len(anime_results)} anime filtrés")
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
