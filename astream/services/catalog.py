"""
CatalogService — Catalogues Adkami pré-chargés + recherche Cinemeta/Kitsu.

Tous les catalogues de navigation sont servis instantanément depuis la mémoire.
La recherche utilisateur passe par Cinemeta + validation Kitsu.
Pagination native via skip/limit.
"""
from typing import List, Dict, Any, Optional

from astream.utils.logger import logger
from astream.services.adkami.catalog_loader import adkami_loader, ADKAMI_CATEGORIES, PAGE_SIZE
from astream.utils.stremio_helpers import StremioLinkBuilder


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
    async def get_complete_catalog(self, request, b64config, search=None, genre=None,
                                   config=None, skip: int = 0):
        if search:
            return await self._search_cinemeta(request, b64config, search)
        elif genre:
            genre_key = self._find_adkami_genre(genre)
            if genre_key:
                metas = self.adkami_loader.get_genre_catalog(genre_key, skip=skip, limit=PAGE_SIZE)
                self._inject_links(request, b64config, metas)
                return metas
            return []
        else:
            metas = self.adkami_loader.get_simulcasts(skip=skip, limit=PAGE_SIZE)
            self._inject_links(request, b64config, metas)
            return metas

    # ===========================
    # Recherche Cinemeta + Kitsu (inchangée)
    # ===========================
    async def _search_cinemeta(self, request, b64config, query: str) -> List[Dict]:
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

        logger.log("API", f"SEARCH — {len(metas)} metas")
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
    # Catalogues spéciaux — tous instantanés depuis la mémoire
    # ===========================
    def get_simulcasts_catalog(self, request, b64config, config, skip: int = 0):
        metas = self.adkami_loader.get_simulcasts(skip=skip, limit=PAGE_SIZE)
        self._inject_links(request, b64config, metas)
        return metas

    def get_genre_catalog(self, request, b64config, config, catalog_id: str, skip: int = 0):
        genre_name = GENRE_CATALOG_MAP.get(catalog_id)
        if not genre_name:
            logger.warning(f"CATALOG GENRE — catalog_id inconnu: {catalog_id}")
            return []
        metas = self.adkami_loader.get_genre_catalog(genre_name, skip=skip, limit=PAGE_SIZE)
        self._inject_links(request, b64config, metas)
        return metas

    # Aliases compat
    def get_en_cours_catalog(self, request, b64config, config, skip: int = 0):
        return self.get_simulcasts_catalog(request, b64config, config, skip=skip)

    def get_nouveautes_catalog(self, request, b64config, config, skip: int = 0):
        return self.get_simulcasts_catalog(request, b64config, config, skip=skip)

    def get_sorties_du_jour_catalog(self, request, b64config, config, skip: int = 0):
        return self.get_simulcasts_catalog(request, b64config, config, skip=skip)

    def get_top_anime_catalog(self, request, b64config, config, skip: int = 0):
        metas = self.adkami_loader.get_genre_catalog("Action", skip=skip, limit=PAGE_SIZE)
        self._inject_links(request, b64config, metas)
        return metas

    def get_films_catalog(self, request, b64config, config, skip: int = 0):
        return self.get_simulcasts_catalog(request, b64config, config, skip=skip)

    def get_season_now_catalog(self, request, b64config, config, skip: int = 0):
        return self.get_simulcasts_catalog(request, b64config, config, skip=skip)

    def get_season_upcoming_catalog(self, request, b64config, config, skip: int = 0):
        return self.get_simulcasts_catalog(request, b64config, config, skip=skip)


# ===========================
# Instance Singleton Globale
# ===========================
catalog_service = CatalogService()
