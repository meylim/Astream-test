from typing import Dict, List, Any, Optional
from urllib.parse import quote


# ===========================
# Classe StremioMetaBuilder
# ===========================
class StremioMetaBuilder:

    @staticmethod
    def build_catalog_meta(anime_data: Dict[str, Any], config: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Construit un objet meta Stremio pour le catalogue.
        Utilisé dans l'endpoint /catalog.
        """
        anime_slug = anime_data.get("slug", "")
        anime_title = anime_data.get("title", "").strip()

        if not anime_title:
            anime_title = anime_slug.replace('-', ' ').title() if anime_slug else 'Titre indisponible'

        # ===================================================
        # Priorité ID (architecture Stateless UI / Cinemeta)
        # ===================================================
        # 1. TMDB ID disponible → "tmdb:XXXXX"
        #    Stremio délègue les méta à Cinemeta (posters HD, saisons, cast)
        #    Notre addon ne sert que les streams.
        # 2. ID Jikan → "jikan:MAL_ID" (fallback pour items non TMDB)
        # 3. Slug natif Anime-Sama → "as:slug"
        tmdb_id = anime_data.get("tmdb_id")
        tmdb_media_type = anime_data.get("tmdb_media_type", "tv")

        # Optionnel: on peut déléguer UNIQUEMENT les films à TMDB, car Cinemeta les supporte
        if tmdb_id and tmdb_media_type == "movie":
            meta_id = f"tmdb:{tmdb_id}"
            stremio_type = "movie"
        elif anime_data.get("_meta_id"):
            # Pour toutes les séries, on force l'utilisation de Jikan/Anime-Sama
            meta_id = anime_data["_meta_id"]
            stremio_type = "movie" if anime_data.get("_is_movie") else "anime"
        else:
            meta_id = f"as:{anime_slug}"
            stremio_type = "anime"

        meta = {
            "id": meta_id,
            "type": stremio_type,
            "name": anime_title,
            "posterShape": "poster",
        }

        poster = anime_data.get("poster") or anime_data.get("image")
        if poster:
            meta["poster"] = poster

        background = anime_data.get("background") or anime_data.get("image")
        if background:
            meta["background"] = background

        logo = anime_data.get("logo")
        if logo:
            meta["logo"] = logo

        description = anime_data.get("description") or anime_data.get("synopsis")
        if description:
            meta["description"] = description

        if anime_data.get("runtime"):
            meta["runtime"] = anime_data.get("runtime")

        if anime_data.get("year_range"):
            meta["releaseInfo"] = anime_data.get("year_range")
        elif anime_data.get("year"):
            meta["releaseInfo"] = anime_data.get("year")

        if anime_data.get("imdbRating"):
            meta["imdbRating"] = anime_data.get("imdbRating")

        if anime_data.get("trailers"):
            meta["trailers"] = anime_data.get("trailers")

        return meta

    @staticmethod
    def build_detail_meta(anime_data: Dict[str, Any], videos: List[Dict[str, Any]], config: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Construit un objet meta Stremio détaillé pour l'endpoint /meta.
        Inclut les vidéos et les behavior hints.
        """
        meta = StremioMetaBuilder.build_catalog_meta(anime_data, config)

        if videos:
            meta["videos"] = videos

        meta["behaviorHints"] = {
            "hasScheduledVideos": True
        }

        if "description" not in meta:
            meta["description"] = anime_data.get("synopsis", "Aucune description disponible")

        genres = anime_data.get("genres", [])
        if isinstance(genres, str):
            from astream.scrapers.animesama.helpers import parse_genres_string
            genres = parse_genres_string(genres)
        meta["genres"] = genres if isinstance(genres, list) else []

        return meta


# ===========================
# Classe StremioLinkBuilder
# ===========================
class StremioLinkBuilder:

    @staticmethod
    def build_genre_links(request, b64config: str, genres: list) -> list:
        if not genres:
            return []
        genre_links = []
        base_url = str(request.base_url).rstrip('/')
        if b64config:
            encoded_manifest = f"{base_url}/{b64config}/manifest.json"
        else:
            encoded_manifest = f"{base_url}/manifest.json"

        encoded_manifest = quote(encoded_manifest, safe='')

        for genre_name in genres:
            genre_links.append({
                "name": genre_name,
                "category": "Genres",
                "url": f"stremio:///discover/{encoded_manifest}/anime/animesama_catalog?genre={quote(genre_name)}"
            })

        return genre_links

    @staticmethod
    def build_imdb_link(anime_data: dict) -> list:
        imdb_links = []

        imdb_id = anime_data.get('imdb_id')
        tmdb_rating = anime_data.get('tmdb_rating')

        if imdb_id and tmdb_rating:
            rating_display = str(tmdb_rating)
            imdb_links.append({
                "name": rating_display,
                "category": "imdb",
                "url": f"https://imdb.com/title/{imdb_id}"
            })
        elif imdb_id:
            imdb_links.append({
                "name": "IMDB",
                "category": "imdb",
                "url": f"https://imdb.com/title/{imdb_id}"
            })

        return imdb_links


# ===========================
# Formateur de flux
# ===========================
def format_stream_for_stremio(video_url: str, language: str, anime_slug: str, season: int, source_prefix: str = "") -> Dict[str, Any]:
    from astream.config.settings import settings

    return {
        "name": f"🐍 {settings.ADDON_NAME}{source_prefix}",
        "title": f"🔗 {video_url}\n🌍 {language.upper()}",
        "url": video_url,
        "language": language,
        "behaviorHints": {
            "notWebReady": True,
            "bingeGroup": f"astream-{anime_slug}-{season}"
        }
    }
