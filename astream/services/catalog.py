"""
CatalogService — Catalogues Adkami indexés sur Cinemeta.

Architecture :
  - Navigation par genre : fichiers JSON Adkami → résolution Cinemeta (tt*)
  - Recherche : Cinemeta directe + validation Kitsu (logique anim2.py)
  - Toutes les metas retournent des IDs tt* pour que Stremio
    délègue les fiches complètes à Cinemeta.
"""
import asyncio
from typing import List, Dict, Any, Optional

from astream.utils.logger import logger
from astream.services.adkami.catalog_loader import adkami_loader, ADKAMI_CATEGORIES
from astream.utils.stremio_helpers import StremioLinkBuilder
from astream.config.settings import settings


# ===========================
# Mapping catalog_id → catégorie Adkami
# ===========================
GENRE_CATALOG_MAP: Dict[str, str] = {
    f"adkami_genre_{name.lower()}": name
    for name in ADKAMI_CATEGORIES.keys()
}


class CatalogService:

    def __init__(self):
        self.adkami_loader = adkami_loader

    # ===========================
    # CATALOGUE PRINCIPAL — Recherche + genre
    # ===========================
    async def get_complete_catalog(self, request, b64config, search=None, genre=None, config=None):
        logger.log("API", f"CATALOG — recherche: {search}, genre: {genre}")

        if search:
            return await self._search_cinemeta(request, b64config, search)
        elif genre:
            genre_key = self._find_adkami_genre(genre)
            if genre_key:
                metas = await self.adkami_loader.get_genre_catalog(genre_key, limit=50)
                self._inject_links(request, b64config, metas)
                return metas
            return []
        else:
            metas = await self.adkami_loader.get_simulcasts(limit=25)
            self._inject_links(request, b64config, metas)
            return metas

    # ===========================
    # Recherche Cinemeta + Kitsu
    # ===========================
    async def _search_cinemeta(self, request, b64config, query: str) -> List[Dict]:
        """
        Recherche via Cinemeta + validation Kitsu.
        Retourne des metas Stremio avec tt* IDs.
        """
        from astream.services.cinemeta.client import cinemeta_client

        logger.log("API", f"SEARCH — Cinemeta+Kitsu pour '{query}'")

        results = await cinemeta_client.search(query, limit=25)
        if not results:
            return []

        metas = []
        seen_ids = set()
        for item in results:
            meta_id = item.get("id", "")
            if not meta_id or meta_id in seen_ids:
                continue
            seen_ids.add(meta_id)

            meta = {
                "id": meta_id,
                "type": item.get("type", "series"),
                "name": item.get("name", ""),
                "posterShape": "poster",
            }
            for field in ("poster", "background", "description", "releaseInfo",
                          "imdbRating", "runtime", "logo"):
                val = item.get(field)
                if val:
                    meta[field] = val

            genres = item.get("genres", [])
            if isinstance(genres, str):
                from astream.scrapers.animesama.helpers import parse_genres_string
                genres = parse_genres_string(genres)
            meta["genres"] = genres
            meta["links"] = (
                StremioLinkBuilder.build_genre_links(request, b64config, genres)
                + StremioLinkBuilder.build_imdb_link(item)
            )
            metas.append(meta)

        logger.log("API", f"SEARCH — {len(metas)} metas retournées")
        return metas

    # ===========================
    # Helpers
    # ===========================
    def _find_adkami_genre(self, genre: str) -> Optional[str]:
        if genre in ADKAMI_CATEGORIES:
            return genre
        genre_clean = genre.lower().replace(" ", "_").replace("-", "_")
        for key in ADKAMI_CATEGORIES:
            if key.lower() == genre_clean:
                return key
        return None

    def _inject_links(self, request, b64config, metas: List[Dict]):
        """Ajoute les liens Stremio aux metas déjà construites."""
        for meta in metas:
            genres = meta.get("genres", [])
            if "links" not in meta:
                meta["links"] = StremioLinkBuilder.build_genre_links(request, b64config, genres)

    # ===========================
    # GENRES (manifest)
    # ===========================
    async def extract_unique_genres(self):
        return self.adkami_loader.get_all_genres()

    # ===========================
    # Catalogues spéciaux → tous servis par Adkami
    # ===========================
    async def get_simulcasts_catalog(self, request, b64config, config):
        try:
            metas = await self.adkami_loader.get_simulcasts(limit=50)
            self._inject_links(request, b64config, metas)
            return metas
        except Exception as e:
            logger.error(f"Erreur simulcasts: {e}")
            return []

    async def get_genre_catalog(self, request, b64config, config, catalog_id: str):
        genre_name = GENRE_CATALOG_MAP.get(catalog_id)
        if not genre_name:
            logger.warning(f"CATALOG GENRE — catalog_id inconnu: {catalog_id}")
            return []
        try:
            metas = await self.adkami_loader.get_genre_catalog(genre_name, limit=50)
            self._inject_links(request, b64config, metas)
            return metas
        except Exception as e:
            logger.error(f"Erreur genre '{genre_name}': {e}")
            return []

    # Aliases compat — tous redirigent vers simulcasts
    async def get_en_cours_catalog(self, request, b64config, config):
        return await self.get_simulcasts_catalog(request, b64config, config)

    async def get_nouveautes_catalog(self, request, b64config, config):
        return await self.get_simulcasts_catalog(request, b64config, config)

    async def get_sorties_du_jour_catalog(self, request, b64config, config):
        return await self.get_simulcasts_catalog(request, b64config, config)

    async def get_top_anime_catalog(self, request, b64config, config):
        try:
            metas = await self.adkami_loader.get_genre_catalog("Action", limit=50)
            self._inject_links(request, b64config, metas)
            return metas
        except Exception as e:
            logger.error(f"Erreur top anime: {e}")
            return []

    async def get_films_catalog(self, request, b64config, config):
        return await self.get_simulcasts_catalog(request, b64config, config)

    async def get_season_now_catalog(self, request, b64config, config):
        return await self.get_simulcasts_catalog(request, b64config, config)

    async def get_season_upcoming_catalog(self, request, b64config, config):
        return await self.get_simulcasts_catalog(request, b64config, config)


# ===========================
# Instance Singleton Globale
# ===========================
catalog_service = CatalogService()
