import asyncio
from typing import List, Dict, Any, Optional

from astream.utils.logger import logger
from astream.scrapers.animesama.client import animesama_api
from astream.scrapers.animesama.helpers import parse_genres_string
from astream.services.tmdb.service import tmdb_service
from astream.services.metadata import metadata_service
from astream.services.jikan.service import jikan_service
from astream.utils.cache import cache_stats, CacheManager
from astream.utils.stremio_helpers import StremioMetaBuilder, StremioLinkBuilder
from astream.config.settings import settings


# ===========================
# Sémaphore TMDB
# ===========================
_tmdb_semaphore = asyncio.Semaphore(5)


class CatalogService:

    def __init__(self):
        self.animesama_api = animesama_api
        self.tmdb_service = tmdb_service
        self.jikan_service = jikan_service

    # ===========================
    # CATALOGUE PRINCIPAL — Jikan (recherche + genre)
    # ===========================
    async def get_complete_catalog(self, request, b64config, search=None, genre=None, config=None):
        logger.log("API", f"CATALOG — recherche: {search}, genre: {genre}")
        anime_data = await self._get_jikan_catalog_data(search, genre)
        logger.log("API", f"Traitement de {len(anime_data)} anime.")
        enhanced = await self._enrich_catalog_with_tmdb(anime_data, config)
        metas = await self._build_catalog_metas(request, b64config, enhanced, config)
        cache_stats.log_summary()
        cache_stats.reset()
        logger.log("API", f"CATALOG — {len(metas)} retournés (search={search}, genre={genre})")
        return metas

    async def _get_jikan_catalog_data(self, search, genre):
        try:
            if search:
                return await self.jikan_service.search(query=search, genre_name=genre, limit=25)
            elif genre:
                return await self.jikan_service.get_by_genre(genre_name=genre, limit=25)
            else:
                airing, top = await asyncio.gather(
                    self.jikan_service.get_simulcasts(limit=15),
                    self.jikan_service.get_top_anime(filter_type="bypopularity", limit=15),
                    return_exceptions=True,
                )
                airing = airing if not isinstance(airing, Exception) else []
                top = top if not isinstance(top, Exception) else []
                seen_ids = set()
                result = []
                for item in (airing + top):
                    mid = item.get("mal_id")
                    if mid and mid not in seen_ids:
                        seen_ids.add(mid)
                        result.append(item)
                return result[:25]
        except Exception as e:
            logger.error(f"Erreur récupération catalogue Jikan: {e}")
            return []

    # ===========================
    # GENRES — depuis Jikan
    # ===========================
    async def extract_unique_genres(self):
        try:
            genres = await self.jikan_service.get_manifest_genres()
            logger.debug(f"Genres Jikan: {len(genres)}")
            return genres
        except Exception as e:
            logger.error(f"Erreur genres Jikan: {e}")
            return []

    # ===========================
    # SORTIES DU JOUR — Planning Jikan du jour
    # ===========================
    async def get_sorties_du_jour_catalog(self, request, b64config, config):
        try:
            results = await self.jikan_service.get_today_releases()
            if not results:
                logger.warning("CATALOG SORTIES DU JOUR — Aucun anime aujourd'hui")
                return []
            logger.log("API", f"CATALOG SORTIES DU JOUR — {len(results)} anime")
            enhanced = await self._enrich_catalog_with_tmdb(results, config)
            metas = await self._build_catalog_metas(request, b64config, enhanced, config)
            logger.log("API", f"CATALOG SORTIES DU JOUR — {len(metas)} retournés")
            return metas
        except Exception as e:
            logger.error(f"Erreur catalogue sorties du jour: {e}")
            return []

    # ===========================
    # SIMULCASTS — Anime TV en cours
    # ===========================
    async def get_simulcasts_catalog(self, request, b64config, config):
        try:
            results = await self.jikan_service.get_simulcasts(limit=25)
            if not results:
                return []
            logger.log("API", f"CATALOG SIMULCASTS — {len(results)} anime")
            enhanced = await self._enrich_catalog_with_tmdb(results, config)
            metas = await self._build_catalog_metas(request, b64config, enhanced, config)
            logger.log("API", f"CATALOG SIMULCASTS — {len(metas)} retournés")
            return metas
        except Exception as e:
            logger.error(f"Erreur catalogue simulcasts: {e}")
            return []

    # ===========================
    # FILMS — Films d'anime
    # ===========================
    async def get_films_catalog(self, request, b64config, config):
        try:
            results = await self.jikan_service.get_films(limit=25)
            if not results:
                return []
            logger.log("API", f"CATALOG FILMS — {len(results)} films")
            enhanced = await self._enrich_catalog_with_tmdb(results, config)
            metas = await self._build_catalog_metas(request, b64config, enhanced, config)
            logger.log("API", f"CATALOG FILMS — {len(metas)} retournés")
            return metas
        except Exception as e:
            logger.error(f"Erreur catalogue films: {e}")
            return []

    # ===========================
    # TOP ANIME
    # ===========================
    async def get_top_anime_catalog(self, request, b64config, config):
        try:
            results = await self.jikan_service.get_top_anime(filter_type="bypopularity", limit=25)
            if not results:
                return []
            logger.log("API", f"CATALOG TOP — {len(results)} anime")
            enhanced = await self._enrich_catalog_with_tmdb(results, config)
            metas = await self._build_catalog_metas(request, b64config, enhanced, config)
            logger.log("API", f"CATALOG TOP — {len(metas)} retournés")
            return metas
        except Exception as e:
            logger.error(f"Erreur catalogue top anime: {e}")
            return []

    # ===========================
    # Aliases compat. anciennes configs
    # ===========================
    async def get_en_cours_catalog(self, request, b64config, config):
        return await self.get_simulcasts_catalog(request, b64config, config)

    async def get_nouveautes_catalog(self, request, b64config, config):
        return await self.get_simulcasts_catalog(request, b64config, config)

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

                meta = StremioMetaBuilder.build_catalog_meta(anime, config)

                # Score MAL comme fallback si pas de rating TMDB
                if not meta.get("imdbRating") and anime.get("mal_score"):
                    meta["imdbRating"] = str(anime["mal_score"])

                meta["links"] = (
                    StremioLinkBuilder.build_genre_links(request, b64config, genres)
                    + StremioLinkBuilder.build_imdb_link(anime)
                )
                meta["genres"] = genres
                metas.append(meta)
            except Exception as e:
                logger.error(f"Erreur meta pour {anime.get('slug','?')}: {e}")

        logo_count = sum(1 for a in anime_data if a.get("logo"))
        logger.log("API", f"CATALOG — {len(metas)} metas, {logo_count} avec logo TMDB")
        return metas


# ===========================
# Instance Singleton Globale
# ===========================
catalog_service = CatalogService()
