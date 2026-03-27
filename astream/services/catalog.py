import asyncio
from typing import List, Dict, Any, Optional

from astream.utils.logger import logger
from astream.scrapers.animesama.client import animesama_api
from astream.scrapers.animesama.helpers import parse_genres_string
from astream.services.tmdb.service import tmdb_service
from astream.services.metadata import metadata_service
from astream.utils.cache import cache_stats, CacheManager
from astream.utils.stremio_helpers import StremioMetaBuilder, StremioLinkBuilder
from astream.config.settings import settings


# ===========================
# Sémaphore TMDB : limite la concurrence pour ne pas saturer 0.1 vCPU
# ===========================
_tmdb_semaphore = asyncio.Semaphore(5)


# ===========================
# Classe CatalogService
# ===========================
class CatalogService:

    def __init__(self):
        self.animesama_api = animesama_api
        self.tmdb_service = tmdb_service

    async def get_complete_catalog(self, request, b64config: str, search: Optional[str] = None,
                                   genre: Optional[str] = None, config=None) -> List[Dict[str, Any]]:
        logger.log("API", f"CATALOG - Catalogue Anime-Sama demandé, recherche: {search}, genre: {genre}")

        language_filter = config.language if config.language != "Tout" else None

        anime_data = await self._get_catalog_data(search, genre, language_filter)
        logger.log("API", f"Traitement de {len(anime_data)} anime.")

        enhanced_anime_data = await self._enrich_catalog_with_tmdb(anime_data, config)

        metas = await self._build_catalog_metas(request, b64config, enhanced_anime_data, config, metadata_service, genre)

        cache_stats.log_summary()
        cache_stats.reset()

        if search and genre:
            logger.log("API", f"CATALOG - Recherche '{search}' + Genre '{genre}': {len(metas)} anime trouvés")
        elif search:
            logger.log("API", f"CATALOG - Recherche '{search}': {len(metas)} anime trouvés")
        elif genre:
            logger.log("API", f"CATALOG - Genre '{genre}': {len(metas)} anime trouvés")
        else:
            logger.log("API", f"CATALOG - Retour de tous les {len(metas)} anime valides")

        return metas

    async def _get_catalog_data(self, search: Optional[str] = None, genre: Optional[str] = None,
                                language: Optional[str] = None) -> List[Dict[str, Any]]:
        try:
            if search:
                logger.log("ANIMESAMA", f"Recherche '{search}' (genre: {genre}, langue: {language})")
                return await self.animesama_api.search_anime(search, language, genre)
            else:
                logger.log("ANIMESAMA", "Récupération contenu homepage complet")
                return await self.animesama_api.get_homepage_content()

        except Exception as e:
            logger.error(f"Erreur récupération catalogue: {e}")
            return []

    def _extract_available_genres(self, catalog_data: List[Dict[str, Any]]) -> List[str]:
        try:
            genres = set()
            for anime in catalog_data:
                anime_genres = anime.get('genres', '')
                if isinstance(anime_genres, str) and anime_genres:
                    genre_list = parse_genres_string(anime_genres)
                    genres.update(genre_list)
                elif isinstance(anime_genres, list):
                    genres.update(anime_genres)
            cleaned_genres = [g for g in genres if len(g) > 1 and g not in ['N/A', 'n/a', '']]
            return sorted(cleaned_genres)
        except Exception as e:
            logger.warning(f"Erreur extraction genres: {e}")
            return []

    async def extract_unique_genres(self) -> List[str]:
        try:
            anime_data = await self.animesama_api.get_homepage_content()
            genres = self._extract_available_genres(anime_data)
            logger.debug(f"Extraction de {len(genres)} genres uniques depuis le catalogue")
            return genres
        except Exception as e:
            logger.error(f"Erreur extraction genres uniques: {e}")
            return []

    async def _enrich_catalog_with_tmdb(self, anime_data: list, config) -> list:
        if not config.tmdbEnabled or not (config.tmdbApiKey or settings.TMDB_API_KEY):
            return anime_data

        try:
            async def enrich_with_semaphore(anime):
                async with _tmdb_semaphore:
                    return await self.tmdb_service.enhance_anime_metadata(anime, config)

            tasks = [enrich_with_semaphore(anime) for anime in anime_data]
            enhanced_anime_data = await asyncio.gather(*tasks, return_exceptions=True)

            enriched_count = sum(1 for result in enhanced_anime_data if not isinstance(result, Exception) and result.get('poster'))
            enhanced_anime_data = [
                result if not isinstance(result, Exception) else anime_data[i]
                for i, result in enumerate(enhanced_anime_data)
            ]

            if enriched_count > 0:
                logger.log("TMDB", f"Enrichissement catalogue: {enriched_count}/{len(anime_data)} anime enrichis")

            return enhanced_anime_data
        except Exception as e:
            logger.error(f"Erreur enrichissement TMDB catalogue: {e}")
            return anime_data

    async def _build_catalog_metas(self, request, b64config: str, anime_data: list, config, metadata_service, genre_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        metas = []

        for anime in anime_data:
            try:
                anime_slug = anime.get('slug', '')
                anime_title = anime.get('title', '').strip()

                if not anime_title:
                    anime_title = anime_slug.replace('-', ' ').title() if anime_slug else 'Titre indisponible'
                    logger.warning(f"CATALOG - Pas de titre pour {anime_slug}, utilisation de '{anime_title}'")

                genres_raw = anime.get('genres', '')
                genres = parse_genres_string(genres_raw) if isinstance(genres_raw, str) else genres_raw

                if genre_filter and genre_filter not in genres:
                    continue

                meta = StremioMetaBuilder.build_catalog_meta(anime, config)

                genre_links = StremioLinkBuilder.build_genre_links(request, b64config, genres)
                imdb_links = StremioLinkBuilder.build_imdb_link(anime)
                meta["links"] = genre_links + imdb_links
                meta["genres"] = genres

                metas.append(meta)

            except Exception as e:
                logger.error(f"Erreur construction meta pour {anime.get('slug', 'unknown')}: {e}")
                continue

        logo_count = sum(1 for anime in anime_data if anime.get('logo'))
        logger.log("API", f"CATALOG - Traitement terminé: {len(metas)} anime, {logo_count} avec logo TMDB")

        return metas

    # ===========================
    # SORTIES DU JOUR — Toujours frais, jamais de cache
    # ===========================
    async def get_sorties_du_jour_catalog(self, request, b64config: str, config) -> List[Dict[str, Any]]:
        """
        Catalogue des anime qui ont RÉELLEMENT un nouvel épisode aujourd'hui.

        À chaque appel :
          1. Invalide le cache homepage → force un re-scrape
          2. Re-scrape la homepage (récupère les dernières sorties à jour)
          3. Intersection planning du jour ∩ dernières sorties = vrais sorties

        Coût : ~1s par appel (scrape homepage), mais données toujours fraîches.
        Les épisodes sortent tout au long de la journée, le cache ne convient pas ici.
        """
        try:
            # Toujours invalider + re-scraper la homepage
            await CacheManager.invalidate("as:homepage")
            await self.animesama_api.get_homepage_content()

            # Intersection depuis les données fraîches
            today_slugs = await self.animesama_api.catalog.get_today_releases()

            if not today_slugs:
                logger.warning("CATALOG SORTIES DU JOUR - Aucun anime sorti aujourd'hui")
                return []

            logger.log("API", f"CATALOG SORTIES DU JOUR - {len(today_slugs)} anime réellement sortis")

            homepage_anime = await self.animesama_api.get_homepage_content()
            homepage_by_slug = {a.get("slug", ""): a for a in homepage_anime}

            results = []
            for slug in today_slugs:
                if slug in homepage_by_slug:
                    anime = dict(homepage_by_slug[slug])
                else:
                    anime = {"slug": slug, "title": slug.replace("-", " ").title(), "genres": []}
                anime["today_release"] = True
                results.append(anime)

            enhanced = await self._enrich_catalog_with_tmdb(results, config)
            metas = await self._build_catalog_metas(request, b64config, enhanced, config, None, None)
            logger.log("API", f"CATALOG SORTIES DU JOUR - {len(metas)} anime retournés")
            return metas

        except Exception as e:
            logger.error(f"Erreur catalogue sorties du jour: {e}")
            return []

    # ===========================
    # EN COURS
    # ===========================
    async def get_en_cours_catalog(self, request, b64config: str, config) -> List[Dict[str, Any]]:
        """Catalogue des anime en cours de diffusion (depuis le planning homepage)."""
        try:
            slugs = await self.animesama_api.catalog.get_planning_slugs()

            if not slugs:
                logger.warning("CATALOG EN COURS - Aucun anime dans le planning")
                return []

            today_slugs = set(await self.animesama_api.catalog.get_today_releases())
            logger.log("API", f"CATALOG EN COURS - {len(slugs)} anime, {len(today_slugs)} sortis aujourd'hui")

            homepage_anime = await self.animesama_api.get_homepage_content()
            homepage_by_slug = {a.get("slug", ""): a for a in homepage_anime}

            sorted_slugs = sorted(slugs, key=lambda s: (0 if s in today_slugs else 1))

            results = []
            for slug in sorted_slugs:
                if slug in homepage_by_slug:
                    anime = dict(homepage_by_slug[slug])
                else:
                    anime = {"slug": slug, "title": slug.replace("-", " ").title(), "genres": []}
                if slug in today_slugs:
                    anime["today_release"] = True
                results.append(anime)

            enhanced = await self._enrich_catalog_with_tmdb(results, config)
            metas = await self._build_catalog_metas(request, b64config, enhanced, config, None, None)
            logger.log("API", f"CATALOG EN COURS - {len(metas)} anime retournés ({len(today_slugs)} sorties du jour en tête)")
            return metas

        except Exception as e:
            logger.error(f"Erreur catalogue en cours: {e}")
            return []

    async def get_nouveautes_catalog(self, request, b64config: str, config) -> List[Dict[str, Any]]:
        try:
            homepage_anime = await self.animesama_api.get_homepage_content()
            if not homepage_anime:
                return []

            nouveautes = homepage_anime[:24]
            logger.log("API", f"CATALOG NOUVEAUTES - {len(nouveautes)} anime récents")

            enhanced = await self._enrich_catalog_with_tmdb(nouveautes, config)
            metas = await self._build_catalog_metas(request, b64config, enhanced, config, None, None)
            logger.log("API", f"CATALOG NOUVEAUTES - {len(metas)} anime retournés")
            return metas

        except Exception as e:
            logger.error(f"Erreur catalogue nouveautés: {e}")
            return []


# ===========================
# Instance Singleton Globale
# ===========================
catalog_service = CatalogService()
