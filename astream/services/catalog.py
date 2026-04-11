import asyncio
from typing import List, Dict, Any, Optional

from astream.utils.logger import logger
from astream.scrapers.animesama.client import animesama_api
from astream.scrapers.animesama.helpers import parse_genres_string
from astream.services.tmdb.service import tmdb_service
from astream.services.jikan.service import jikan_service
from astream.services.kitsu.validator import filter_jikan_items
from astream.utils.cache import cache_stats
from astream.utils.stremio_helpers import StremioMetaBuilder, StremioLinkBuilder
from astream.config.settings import settings


# ===========================
# Sémaphore TMDB
# ===========================
_tmdb_semaphore = asyncio.Semaphore(5)

# ===========================
# Mapping catalog_id → genre Jikan
# ===========================
GENRE_CATALOG_MAP: Dict[str, str] = {
    "jikan_genre_action":        "Action",
    "jikan_genre_adventure":     "Adventure",
    "jikan_genre_comedy":        "Comedy",
    "jikan_genre_drama":         "Drama",
    "jikan_genre_fantasy":       "Fantasy",
    "jikan_genre_romance":       "Romance",
    "jikan_genre_sci_fi":        "Sci-Fi",
    "jikan_genre_slice_of_life": "Slice of Life",
    "jikan_genre_supernatural":  "Supernatural",
    "jikan_genre_sports":        "Sports",
    "jikan_genre_horror":        "Horror",
    "jikan_genre_psychological": "Psychological",
    "jikan_genre_shounen":       "Shounen",
    "jikan_genre_isekai":        "Isekai",
    "jikan_genre_mecha":         "Mecha",
    "jikan_genre_historical":    "Historical",
}

GENRE_EMOJI: Dict[str, str] = {
    "Action":        "⚔️",
    "Adventure":     "🗺️",
    "Comedy":        "😂",
    "Drama":         "🎭",
    "Fantasy":       "✨",
    "Romance":       "💕",
    "Sci-Fi":        "🚀",
    "Slice of Life": "☀️",
    "Supernatural":  "👻",
    "Sports":        "⚽",
    "Horror":        "😱",
    "Psychological": "🧠",
    "Shounen":       "🔥",
    "Isekai":        "🌀",
    "Mecha":         "🤖",
    "Historical":    "⛩️",
}


# ===========================
# Déduplication par mal_id
# ===========================
def _dedup_by_mal_id(items: List[Dict]) -> List[Dict]:
    """Supprime les doublons par mal_id (avant enrichissement TMDB)."""
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
        self.animesama_api = animesama_api
        self.tmdb_service = tmdb_service
        self.jikan_service = jikan_service

    # ===========================
    # Pipeline commun : Jikan → Kitsu → TMDB → Metas
    # ===========================
    async def _pipeline(self, request, b64config, anime_data: List[Dict], config, label: str) -> List[Dict]:
        """
        Pipeline standard appliqué à tous les catalogues Jikan :
          1. Déduplication par mal_id
          2. Filtre Kitsu (validation croisée)
          3. Enrichissement TMDB
          4. Construction metas Stremio (dédup finale par imdb_id)
        """
        # 1. Dédup mal_id
        anime_data = _dedup_by_mal_id(anime_data)
        logger.log("API", f"{label} — {len(anime_data)} après dédup mal_id")

        # 2. Filtre Kitsu
        anime_data = await filter_jikan_items(anime_data)
        logger.log("API", f"{label} — {len(anime_data)} après filtre Kitsu")

        if not anime_data:
            return []

        # 3. Enrichissement TMDB
        enhanced = await self._enrich_catalog_with_tmdb(anime_data, config)

        # 4. Construction metas (dédup imdb_id incluse)
        metas = await self._build_catalog_metas(request, b64config, enhanced, config)
        cache_stats.log_summary()
        cache_stats.reset()
        logger.log("API", f"{label} — {len(metas)} metas retournées")
        return metas

    # ===========================
    # CATALOGUE PRINCIPAL — Recherche + genre filter
    # ===========================
    async def get_complete_catalog(self, request, b64config, search=None, genre=None, config=None):
        logger.log("API", f"CATALOG — recherche: {search}, genre: {genre}")
        anime_data = await self._get_jikan_catalog_data(search, genre)
        return await self._pipeline(request, b64config, anime_data, config, f"CATALOG search={search} genre={genre}")

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
            logger.error(f"Erreur catalogue Jikan: {e}")
            return []

    # ===========================
    # GENRES (manifest)
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
    # SORTIES DU JOUR
    # ===========================
    async def get_sorties_du_jour_catalog(self, request, b64config, config):
        try:
            results = await self.jikan_service.get_today_releases()
            if not results:
                return []
            return await self._pipeline(request, b64config, results, config, "SORTIES DU JOUR")
        except Exception as e:
            logger.error(f"Erreur sorties du jour: {e}")
            return []

    # ===========================
    # SIMULCASTS
    # ===========================
    async def get_simulcasts_catalog(self, request, b64config, config):
        try:
            results = await self.jikan_service.get_simulcasts(limit=25)
            if not results:
                return []
            return await self._pipeline(request, b64config, results, config, "SIMULCASTS")
        except Exception as e:
            logger.error(f"Erreur simulcasts: {e}")
            return []

    # ===========================
    # FILMS
    # ===========================
    async def get_films_catalog(self, request, b64config, config):
        try:
            results = await self.jikan_service.get_films(limit=25)
            if not results:
                return []
            return await self._pipeline(request, b64config, results, config, "FILMS")
        except Exception as e:
            logger.error(f"Erreur films: {e}")
            return []

    # ===========================
    # TOP ANIME
    # ===========================
    async def get_top_anime_catalog(self, request, b64config, config):
        try:
            results = await self.jikan_service.get_top_anime(filter_type="bypopularity", limit=25)
            if not results:
                return []
            return await self._pipeline(request, b64config, results, config, "TOP ANIME")
        except Exception as e:
            logger.error(f"Erreur top anime: {e}")
            return []

    # ===========================
    # SAISON EN COURS
    # ===========================
    async def get_season_now_catalog(self, request, b64config, config):
        try:
            results = await self.jikan_service.get_season_now(limit=25)
            if not results:
                return []
            return await self._pipeline(request, b64config, results, config, "SAISON EN COURS")
        except Exception as e:
            logger.error(f"Erreur saison en cours: {e}")
            return []

    # ===========================
    # PROCHAINE SAISON
    # ===========================
    async def get_season_upcoming_catalog(self, request, b64config, config):
        try:
            results = await self.jikan_service.get_season_upcoming(limit=25)
            if not results:
                return []
            return await self._pipeline(request, b64config, results, config, "PROCHAINE SAISON")
        except Exception as e:
            logger.error(f"Erreur prochaine saison: {e}")
            return []

    # ===========================
    # CATALOGUE PAR GENRE
    # ===========================
    async def get_genre_catalog(self, request, b64config, config, catalog_id: str):
        genre_name = GENRE_CATALOG_MAP.get(catalog_id)
        if not genre_name:
            logger.warning(f"CATALOG GENRE — catalog_id inconnu: {catalog_id}")
            return []
        try:
            results = await self.jikan_service.get_by_genre(genre_name=genre_name, limit=25)
            if not results:
                return []
            return await self._pipeline(request, b64config, results, config, f"GENRE '{genre_name}'")
        except Exception as e:
            logger.error(f"Erreur genre '{genre_name}': {e}")
            return []

    # ===========================
    # Aliases compat
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
    # Déduplication finale par imdb_id (IDs tt... Cinemeta)
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

                # --- Déduplication par imdb_id ---
                imdb_id = anime.get("imdb_id")
                if imdb_id:
                    if imdb_id in seen_imdb_ids:
                        logger.debug(f"DEDUP: {anime_title} ({imdb_id}) déjà présent, ignoré")
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

                # Pré-peupler xref en arrière-plan
                if anime_slug and anime_title and not anime.get("_is_jikan"):
                    tmdb_id = anime.get("tmdb_id")
                    mal_id = anime.get("mal_id")
                    import asyncio as _asyncio
                    from astream.utils.cross_ref import get_or_resolve_xref as _get_xref
                    from astream.utils.http_client import http_client as _hc
                    _asyncio.create_task(_get_xref(
                        anime_slug, anime_title, _hc,
                        tmdb_api_key=settings.TMDB_API_KEY,
                        existing_tmdb_id=tmdb_id,
                        existing_mal_id=mal_id,
                    ))

            except Exception as e:
                logger.error(f"Erreur meta pour {anime.get('slug', '?')}: {e}")

        logo_count = sum(1 for a in anime_data if a.get("logo"))
        logger.log("API", f"BUILD METAS — {len(metas)} metas, {logo_count} avec logo TMDB, {len(seen_imdb_ids)} imdb_id uniques")
        return metas


# ===========================
# Instance Singleton Globale
# ===========================
catalog_service = CatalogService()
