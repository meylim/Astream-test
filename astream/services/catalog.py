"""
CatalogService — Pilier B de l'architecture AStream.

Sources de données :
  • CATALOGUES (browse/genre)  → JSON Adkami (scraper.py) pré-construits sur disque
  • SEARCH                     → Cinemeta + validation Kitsu (logique anim2.py)
  • ENRICHISSEMENT             → TMDB optionnel (inchangé)
  • STREAMING                  → Anime-Sama (inchangé)
"""

import asyncio
from typing import List, Dict, Any, Optional

from astream.utils.logger import logger
from astream.services.adkami_catalog import (
    adkami_catalog_service,
    ADKAMI_CATALOG_MAP,
    ADKAMI_GENRE_DISPLAY,
)
from astream.scrapers.adkami.scraper import scan_simulcasts_cached
from astream.services.cinemeta.client import cinemeta_client
from astream.services.kitsu.validator import filter_jikan_items
from astream.services.tmdb.service import tmdb_service
from astream.utils.cache import cache_stats
from astream.utils.stremio_helpers import StremioMetaBuilder, StremioLinkBuilder
from astream.scrapers.animesama.helpers import parse_genres_string
from astream.config.settings import settings

# Ré-export pour que les imports dans routes.py et scheduler.py continuent de fonctionner
GENRE_CATALOG_MAP: Dict[str, str] = ADKAMI_CATALOG_MAP

# ===========================
# Sémaphore TMDB
# ===========================
_tmdb_semaphore = asyncio.Semaphore(5)


# ===========================
# Déduplication par mal_id
# ===========================
def _dedup_by_mal_id(items: List[Dict]) -> List[Dict]:
    seen: set = set()
    result = []
    for item in items:
        mid = item.get("mal_id")
        if mid and mid in seen:
            continue
        if mid:
            seen.add(mid)
        result.append(item)
    return result


class CatalogService:

    def __init__(self):
        self.tmdb_service = tmdb_service
        self.cinemeta_client = cinemeta_client

    # ===========================
    # Pipeline Adkami → Kitsu → TMDB → Metas
    # ===========================
    async def _pipeline(self, request, b64config, anime_data: List[Dict], config, label: str) -> List[Dict]:
        """
        Pipeline standard pour les catalogues Adkami :
          1. Déduplication par mal_id
          2. Filtre Kitsu (validation croisée)
          3. Enrichissement TMDB
          4. Construction metas Stremio
        """
        anime_data = _dedup_by_mal_id(anime_data)
        logger.log("API", f"{label} — {len(anime_data)} après dédup")

        anime_data = await filter_jikan_items(anime_data)
        logger.log("API", f"{label} — {len(anime_data)} après filtre Kitsu")

        if not anime_data:
            return []

        enhanced = await self._enrich_catalog_with_tmdb(anime_data, config)
        metas = await self._build_catalog_metas(request, b64config, enhanced, config)
        cache_stats.log_summary()
        cache_stats.reset()
        logger.log("API", f"{label} — {len(metas)} metas retournées")
        return metas

    # ===========================
    # RECHERCHE (anim2.py — Cinemeta + Kitsu)
    # ===========================
    async def get_complete_catalog(self, request, b64config, search=None, genre=None, config=None):
        """
        • search → Cinemeta search + validation Kitsu (logique anim2.py)
                   Fallback : recherche locale dans les JSON Adkami
        • genre  → catalogue Adkami JSON du genre correspondant
        • aucun  → catalogue simulcast Adkami
        """
        logger.log("API", f"CATALOG — recherche: {search}, genre: {genre}")

        if search:
            return await self._search_cinemeta(request, b64config, search, genre, config)
        elif genre:
            return await self._get_adkami_genre_catalog(request, b64config, genre, config)
        else:
            return await self._get_default_catalog(request, b64config, config)

    async def _search_cinemeta(self, request, b64config, query: str, genre: Optional[str], config) -> List[Dict]:
        """
        Recherche via Cinemeta + validation Kitsu (portage de anim2.py).
        Retourne des metas Stremio construites depuis les résultats Cinemeta.
        """
        logger.log("API", f"SEARCH Cinemeta+Kitsu : '{query}'")

        try:
            # cinemeta_client.search() fait déjà la validation Kitsu en parallèle (vagues de 10)
            cinemeta_items = await self.cinemeta_client.search(query, limit=25)
        except Exception as e:
            logger.error(f"Cinemeta search error: {e}")
            cinemeta_items = []

        if cinemeta_items:
            metas = self._build_metas_from_cinemeta(request, b64config, cinemeta_items, config, genre)
            logger.log("API", f"SEARCH Cinemeta : {len(metas)} résultats")
            return metas

        # Fallback : recherche locale dans les JSON Adkami
        logger.log("API", f"SEARCH Cinemeta vide — fallback Adkami local pour '{query}'")
        local_items = adkami_catalog_service.search_in_catalogs(query, limit=25)
        if local_items:
            return await self._pipeline(request, b64config, local_items, config, f"SEARCH LOCAL '{query}'")

        return []

    def _build_metas_from_cinemeta(
        self,
        request,
        b64config: str,
        cinemeta_items: List[Dict],
        config,
        genre_filter: Optional[str] = None,
    ) -> List[Dict]:
        """
        Construit des metas Stremio depuis des résultats Cinemeta validés.
        Les items Cinemeta ont déjà l'id IMDb, le poster, etc.
        """
        metas = []
        seen_ids: set = set()

        for item in cinemeta_items:
            try:
                imdb_id = item.get("id", "")
                name = item.get("name", "").strip()
                if not imdb_id or not name:
                    continue

                if imdb_id in seen_ids:
                    continue
                seen_ids.add(imdb_id)

                item_type = item.get("type", "series")
                stremio_type = "movie" if item_type == "movie" else "anime"

                meta: Dict[str, Any] = {
                    "id": imdb_id,
                    "type": stremio_type,
                    "name": name,
                    "posterShape": "poster",
                }

                if item.get("poster"):
                    meta["poster"] = item["poster"]
                if item.get("background"):
                    meta["background"] = item["background"]
                if item.get("description"):
                    meta["description"] = item["description"]
                if item.get("imdbRating"):
                    meta["imdbRating"] = item["imdbRating"]
                if item.get("releaseInfo"):
                    meta["releaseInfo"] = item["releaseInfo"]
                if item.get("runtime"):
                    meta["runtime"] = item["runtime"]

                genres = item.get("genres", [])
                if isinstance(genres, str):
                    genres = parse_genres_string(genres)

                meta["genres"] = genres
                meta["links"] = (
                    StremioLinkBuilder.build_genre_links(request, b64config, genres)
                    + StremioLinkBuilder.build_imdb_link({"imdb_id": imdb_id})
                )

                metas.append(meta)

            except Exception as e:
                logger.error(f"Build meta Cinemeta '{item.get('name', '?')}': {e}")

        return metas

    # ===========================
    # Catalogues Adkami (genre + default)
    # ===========================
    async def _get_adkami_genre_catalog(self, request, b64config, display_genre: str, config) -> List[Dict]:
        """Catalogue d'un genre via les JSON Adkami."""
        slug = adkami_catalog_service.get_genre_by_display_name(display_genre)
        if not slug:
            slug = display_genre if display_genre in ADKAMI_GENRE_DISPLAY else None
        if not slug:
            logger.warning(f"CATALOG GENRE — genre inconnu: {display_genre}")
            return []

        items = adkami_catalog_service.get_genre_catalog(slug, limit=25)
        if not items:
            return []

        return await self._pipeline(request, b64config, items, config, f"GENRE '{slug}'")

    async def _get_default_catalog(self, request, b64config, config) -> List[Dict]:
        """
        Catalogue par défaut = simulcasts Adkami rechargés EN DIRECT depuis Adkami.
        Enrichissement instantané depuis le cache Jikan en mémoire (0 appel Jikan).
        Fallback sur simulcast.json si Adkami inaccessible.
        """
        logger.log("API", "SIMULCAST — scraping Adkami en direct...")
        raw_entries = await scan_simulcasts_cached()
        if not raw_entries:
            return []
        items = adkami_catalog_service._convert_raw_list(raw_entries, "Simulcast")
        if not items:
            return []
        return await self._pipeline(request, b64config, items, config, "SIMULCASTS LIVE")

    # ===========================
    # GENRES (manifest)
    # ===========================
    async def extract_unique_genres(self) -> List[str]:
        return adkami_catalog_service.get_manifest_genres()

    # ===========================
    # SIMULCASTS
    # ===========================
    async def get_simulcasts_catalog(self, request, b64config, config) -> List[Dict]:
        return await self._get_default_catalog(request, b64config, config)

    async def get_en_cours_catalog(self, request, b64config, config) -> List[Dict]:
        return await self.get_simulcasts_catalog(request, b64config, config)

    async def get_nouveautes_catalog(self, request, b64config, config) -> List[Dict]:
        return await self.get_simulcasts_catalog(request, b64config, config)

    # ===========================
    # SORTIES DU JOUR
    # ===========================
    async def get_sorties_du_jour_catalog(self, request, b64config, config) -> List[Dict]:
        try:
            entries = adkami_catalog_service.get_simulcast_catalog(limit=25)
            return await self._pipeline(request, b64config, entries, config, "SORTIES DU JOUR")
        except Exception as e:
            logger.error(f"Erreur sorties du jour: {e}")
            return []

    # ===========================
    # FILMS
    # ===========================
    async def get_films_catalog(self, request, b64config, config) -> List[Dict]:
        try:
            action = adkami_catalog_service.get_genre_catalog("Action", limit=15)
            hist = adkami_catalog_service.get_genre_catalog("Historique", limit=10)
            merged = _dedup_by_mal_id(action + hist)
            return await self._pipeline(request, b64config, merged, config, "FILMS")
        except Exception as e:
            logger.error(f"Erreur films: {e}")
            return []

    # ===========================
    # TOP ANIME
    # ===========================
    async def get_top_anime_catalog(self, request, b64config, config) -> List[Dict]:
        return await self.get_simulcasts_catalog(request, b64config, config)

    # ===========================
    # SAISON EN COURS / SUIVANTE
    # ===========================
    async def get_season_now_catalog(self, request, b64config, config) -> List[Dict]:
        return await self.get_simulcasts_catalog(request, b64config, config)

    async def get_season_upcoming_catalog(self, request, b64config, config) -> List[Dict]:
        return await self.get_simulcasts_catalog(request, b64config, config)

    # ===========================
    # CATALOGUE PAR GENRE (catalog_id Stremio)
    # ===========================
    async def get_genre_catalog(self, request, b64config, config, catalog_id: str) -> List[Dict]:
        genre_slug = ADKAMI_CATALOG_MAP.get(catalog_id)
        if not genre_slug:
            logger.warning(f"CATALOG GENRE — catalog_id inconnu: {catalog_id}")
            return []
        items = adkami_catalog_service.get_genre_catalog(genre_slug, limit=25)
        if not items:
            return []
        return await self._pipeline(request, b64config, items, config, f"GENRE '{genre_slug}'")

    # ===========================
    # Enrichissement TMDB
    # ===========================
    async def _enrich_catalog_with_tmdb(self, anime_data, config):
        if not config.tmdbEnabled or not (config.tmdbApiKey or settings.TMDB_API_KEY):
            return anime_data
        try:
            async def enrich(anime):
                async with _tmdb_semaphore:
                    return await self.tmdb_service.enhance_anime_metadata(anime, config)

            results = await asyncio.gather(*[enrich(a) for a in anime_data], return_exceptions=True)
            enriched_count = sum(1 for r in results if not isinstance(r, Exception) and r.get("poster"))
            final = [r if not isinstance(r, Exception) else anime_data[i] for i, r in enumerate(results)]
            if enriched_count:
                logger.log("TMDB", f"Enrichissement: {enriched_count}/{len(anime_data)} enrichis")
            return final
        except Exception as e:
            logger.error(f"Erreur enrichissement TMDB: {e}")
            return anime_data

    # ===========================
    # Construction metas Stremio
    # ===========================
    async def _build_catalog_metas(self, request, b64config, anime_data, config, genre_filter=None):
        metas = []
        seen_imdb_ids: set = set()

        for anime in anime_data:
            try:
                anime_title = anime.get("title", "").strip()
                anime_slug = anime.get("slug", "")
                if not anime_title:
                    anime_title = anime_slug.replace("-", " ").title() or "Titre indisponible"

                genres_raw = anime.get("genres", [])
                genres = parse_genres_string(genres_raw) if isinstance(genres_raw, str) else genres_raw

                if genre_filter and genre_filter not in genres:
                    continue

                imdb_id = anime.get("imdb_id")
                if imdb_id:
                    if imdb_id in seen_imdb_ids:
                        continue
                    seen_imdb_ids.add(imdb_id)

                meta = StremioMetaBuilder.build_catalog_meta(anime, config)

                if not meta.get("imdbRating") and anime.get("mal_score"):
                    meta["imdbRating"] = str(anime["mal_score"])

                meta["links"] = (
                    StremioLinkBuilder.build_genre_links(request, b64config, genres)
                    + StremioLinkBuilder.build_imdb_link(anime)
                )
                meta["genres"] = genres
                metas.append(meta)

            except Exception as e:
                logger.error(f"Erreur meta pour {anime.get('slug', '?')}: {e}")

        logo_count = sum(1 for a in anime_data if a.get("logo"))
        logger.log(
            "API",
            f"BUILD METAS — {len(metas)} metas, {logo_count} avec logo TMDB, "
            f"{len(seen_imdb_ids)} imdb_id uniques",
        )
        return metas


# ===========================
# Instance Singleton Globale
# ===========================
catalog_service = CatalogService()
