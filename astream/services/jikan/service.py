"""
Service Jikan — Pilier A de l'architecture AStream.
Transforme les données Jikan/MAL en format interne AStream
et alimente les catalogues Stremio.
"""
import re
from datetime import datetime
from typing import List, Dict, Any, Optional

from astream.services.jikan.client import jikan_client
from astream.utils.logger import logger

# ===========================
# Mapping genre Jikan (nom → ID MAL)
# ===========================
JIKAN_GENRE_ID_MAP: Dict[str, int] = {
    "Action": 1,
    "Adventure": 2,
    "Avant Garde": 5,
    "Award Winning": 46,
    "Boys Love": 28,
    "Comedy": 4,
    "Drama": 8,
    "Ecchi": 9,
    "Fantasy": 10,
    "Girls Love": 26,
    "Gourmet": 47,
    "Horror": 14,
    "Mystery": 7,
    "Romance": 22,
    "Sci-Fi": 24,
    "Slice of Life": 36,
    "Sports": 30,
    "Supernatural": 37,
    "Suspense": 41,
    "Psychological": 40,
    "Mecha": 18,
    "Music": 19,
    "Historical": 13,
    "Military": 38,
    "Harem": 35,
    "School": 23,
    "Isekai": 62,
    "Reincarnation": 60,
    # Démographies
    "Shounen": 27,
    "Shoujo": 25,
    "Seinen": 42,
    "Josei": 43,
}

# Genres à exposer dans le manifest (ordre d'affichage)
MANIFEST_GENRES = [
    "Action", "Adventure", "Comedy", "Drama", "Fantasy",
    "Horror", "Mystery", "Romance", "Sci-Fi", "Slice of Life",
    "Sports", "Supernatural", "Suspense", "Psychological",
    "Mecha", "Historical", "Isekai", "School", "Music",
    "Shounen", "Shoujo", "Seinen",
]

# Mapping jour FR → anglais Jikan
_DAY_MAP = {
    0: "monday", 1: "tuesday", 2: "wednesday",
    3: "thursday", 4: "friday", 5: "saturday", 6: "sunday",
}


# ===========================
# Conversion Jikan → AStream
# ===========================
def jikan_to_astream(anime: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convertit un objet anime Jikan en dictionnaire interne AStream.
    Le champ `_meta_id` est utilisé par StremioMetaBuilder pour l'ID Stremio.
    """
    mal_id = anime.get("mal_id")
    if not mal_id:
        return {}

    # Titre : préférer l'anglais pour la compatibilité TMDB
    title = (
        anime.get("title_english")
        or anime.get("title")
        or ""
    ).strip()

    # Genres : combiner genres + démographies + thèmes pertinents
    genres: List[str] = []
    for key in ("genres", "demographics", "themes"):
        for g in anime.get(key, []):
            name = g.get("name", "")
            if name and name not in genres:
                genres.append(name)

    # Poster depuis Jikan (sera potentiellement remplacé par TMDB)
    images = anime.get("images", {})
    poster = (
        images.get("webp", {}).get("large_image_url")
        or images.get("jpg", {}).get("large_image_url")
        or images.get("jpg", {}).get("image_url")
    )

    # Année de première diffusion
    year = ""
    if anime.get("year"):
        year = str(anime["year"])
    elif anime.get("aired", {}).get("from"):
        year = anime["aired"]["from"][:4]

    # Synopsis
    synopsis = (anime.get("synopsis") or "").strip()
    if synopsis.endswith("(Source: MAL Rewrite)"):
        synopsis = synopsis[:-21].strip()

    # Durée
    runtime = ""
    raw_duration = anime.get("duration", "")
    if raw_duration:
        dur_match = re.search(r"(\d+)\s*min", raw_duration)
        if dur_match:
            runtime = f"{dur_match.group(1)} min"

    # Type : film ou série ?
    anime_type = anime.get("type", "TV")
    is_movie = anime_type in ("Movie", "ONA", "OVA", "Special")

    # Score MAL (affiché si TMDB n'en a pas)
    score = anime.get("score")

    return {
        # Identifiants
        "_meta_id": f"jikan:{mal_id}",
        "slug": f"jikan-{mal_id}",   # slug virtuel interne
        "mal_id": mal_id,
        "_is_jikan": True,
        "_is_movie": is_movie,

        # Contenu
        "title": title,
        "genres": genres,
        "description": synopsis,
        "synopsis": synopsis,
        "year": year,
        "runtime": runtime,
        "poster": poster,
        "image": poster,

        # Métadonnées
        "mal_score": score,
        "episodes_count": anime.get("episodes"),
        "status": anime.get("status", ""),
        "season": anime.get("season", ""),
        "type": anime_type,
    }


# ===========================
# Classe JikanService
# ===========================
class JikanService:

    def __init__(self):
        self.client = jikan_client

    # ----------------------------
    # Sorties du jour
    # ----------------------------
    async def get_today_releases(self) -> List[Dict[str, Any]]:
        """
        Anime avec un épisode diffusé aujourd'hui selon le calendrier Jikan.
        Utilise le planning hebdomadaire de Jikan.
        """
        today_idx = datetime.now().weekday()
        day_en = _DAY_MAP.get(today_idx, "monday")

        logger.log("JIKAN", f"Sorties du jour — planning {day_en}")
        raw = await self.client.get_schedules(day_en)
        results = [jikan_to_astream(a) for a in raw if a.get("mal_id")]
        logger.log("JIKAN", f"Sorties du jour: {len(results)} anime")
        return results

    # ----------------------------
    # Simulcasts en cours
    # ----------------------------
    async def get_simulcasts(self, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Anime TV actuellement en cours de diffusion, triés par score.
        """
        logger.log("JIKAN", f"Simulcasts — {limit} anime")
        raw = await self.client.get_airing(limit=limit)
        results = [jikan_to_astream(a) for a in raw if a.get("mal_id")]
        logger.log("JIKAN", f"Simulcasts: {len(results)} anime retournés")
        return results

    # ----------------------------
    # Films
    # ----------------------------
    async def get_films(self, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Films d'anime triés par score.
        """
        logger.log("JIKAN", f"Films — {limit} résultats")
        raw = await self.client.get_movies(limit=limit)
        results = [jikan_to_astream(a) for a in raw if a.get("mal_id")]
        logger.log("JIKAN", f"Films: {len(results)} films retournés")
        return results

    # ----------------------------
    # Top Anime
    # ----------------------------
    async def get_top_anime(self, filter_type: str = "bypopularity", limit: int = 25) -> List[Dict[str, Any]]:
        """
        Top anime par popularité, score, ou en cours.
        filter_type: airing | bypopularity | favorite
        """
        logger.log("JIKAN", f"Top anime — filtre: {filter_type}")
        raw = await self.client.get_top_anime(filter_type=filter_type, limit=limit)
        results = [jikan_to_astream(a) for a in raw if a.get("mal_id")]
        logger.log("JIKAN", f"Top anime: {len(results)} retournés")
        return results

    # ----------------------------
    # Par genre
    # ----------------------------
    async def get_by_genre(self, genre_name: str, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Anime filtrés par genre (nom → ID Jikan).
        """
        genre_id = JIKAN_GENRE_ID_MAP.get(genre_name)
        if not genre_id:
            logger.warning(f"JIKAN: Genre inconnu '{genre_name}', retour liste vide")
            return []

        logger.log("JIKAN", f"Genre '{genre_name}' (ID {genre_id})")
        raw = await self.client.get_anime_by_genre(genre_id=genre_id, limit=limit)
        results = [jikan_to_astream(a) for a in raw if a.get("mal_id")]
        logger.log("JIKAN", f"Genre '{genre_name}': {len(results)} anime")
        return results

    # ----------------------------
    # Recherche
    # ----------------------------
    async def search(self, query: str, genre_name: Optional[str] = None, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Recherche par titre avec support Romaji/anglais/alternatifs.
        Genre optionnel pour affiner les résultats.
        """
        genre_id = JIKAN_GENRE_ID_MAP.get(genre_name) if genre_name else None
        logger.log("JIKAN", f"Recherche '{query}'" + (f" + genre '{genre_name}'" if genre_name else ""))

        raw = await self.client.search_anime(query=query, genre_id=genre_id, limit=limit)
        results = [jikan_to_astream(a) for a in raw if a.get("mal_id")]
        logger.log("JIKAN", f"Recherche '{query}': {len(results)} résultats")
        return results

    # ----------------------------
    # Détail d'un anime
    # ----------------------------
    async def get_anime(self, mal_id: int) -> Optional[Dict[str, Any]]:
        """Récupère les détails complets d'un anime par MAL ID."""
        raw = await self.client.get_anime_by_id(mal_id)
        if not raw:
            return None
        return jikan_to_astream(raw)

    # ----------------------------
    # Liste des genres pour le manifest
    # ----------------------------
    async def get_manifest_genres(self) -> List[str]:
        """
        Retourne la liste des genres à exposer dans le manifest Stremio.
        Utilise la liste statique MANIFEST_GENRES enrichie des genres Jikan.
        """
        try:
            jikan_genres = await self.client.get_genres()
            dynamic_names = [g["name"] for g in jikan_genres if g.get("name")]
            # Union: liste statique ordonnée + genres Jikan supplémentaires
            result = list(MANIFEST_GENRES)
            for name in dynamic_names:
                if name not in result and name in JIKAN_GENRE_ID_MAP:
                    result.append(name)
            return result
        except Exception as e:
            logger.error(f"JIKAN: get_manifest_genres: {e}")
            return list(MANIFEST_GENRES)


    # ----------------------------
    # Saison en cours
    # ----------------------------
    async def get_season_now(self, limit: int = 25) -> List[Dict[str, Any]]:
        """Anime de la saison en cours (printemps/été/automne/hiver)."""
        logger.log("JIKAN", "Saison en cours")
        raw = await self.client.get_season_now(limit=limit)
        raw.sort(key=lambda a: a.get("score") or 0, reverse=True)
        results = [jikan_to_astream(a) for a in raw if a.get("mal_id")]
        logger.log("JIKAN", f"Saison en cours: {len(results)} anime")
        return results

    # ----------------------------
    # Prochaine saison
    # ----------------------------
    async def get_season_upcoming(self, limit: int = 25) -> List[Dict[str, Any]]:
        """Anime annoncés pour la prochaine saison."""
        logger.log("JIKAN", "Prochaine saison")
        raw = await self.client.get_season_upcoming(limit=limit)
        raw.sort(key=lambda a: a.get("members") or 0, reverse=True)
        results = [jikan_to_astream(a) for a in raw if a.get("mal_id")]
        logger.log("JIKAN", f"Prochaine saison: {len(results)} anime")
        return results


# ===========================
# Instance Singleton Globale
# ===========================
jikan_service = JikanService()
