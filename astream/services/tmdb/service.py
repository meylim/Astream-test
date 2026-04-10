import asyncio
from typing import Optional, Dict, List, Any

from astream.services.tmdb.client import TMDBClient, normalize_title
from astream.utils.http_client import http_client, safe_json_decode
from astream.utils.logger import logger
from astream.utils.validators import ConfigModel
from astream.config.settings import settings


# ===========================
# Aide : Sélectionner la meilleure image
# ===========================
def _select_best_image(images: List[Dict[str, Any]], prefer_lang: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not images:
        return None

    if prefer_lang:
        lang_images = [img for img in images if img.get("iso_639_1") == prefer_lang]
        if lang_images:
            return max(lang_images, key=lambda x: x.get("width", 0) * x.get("height", 0))

    return max(images, key=lambda x: x.get("width", 0) * x.get("height", 0))


# ===========================
# Classe TMDBService
# ===========================
class TMDBService:

    def __init__(self):
        self.http_client = http_client

    def _get_tmdb_client(self, config: ConfigModel) -> Optional[TMDBClient]:

        api_key = config.tmdbApiKey if config.tmdbApiKey else settings.TMDB_API_KEY
        if not api_key:
            return None

        return TMDBClient(self.http_client, api_key)

    async def enhance_anime_metadata(self, anime_data: Dict[str, Any], config: ConfigModel) -> Dict[str, Any]:
        if not config.tmdbEnabled:
            return anime_data

        tmdb_client = self._get_tmdb_client(config)
        if not tmdb_client:
            logger.log("TMDB", "Aucune clé API TMDB disponible")
            return anime_data

        try:
            title = anime_data.get("title", anime_data.get("name", ""))
            if not title:
                return anime_data

            clean_title = normalize_title(title, for_search=True)
            tmdb_anime = await tmdb_client.search_anime(clean_title)
            if not tmdb_anime:
                return anime_data

            media_type = tmdb_anime.get("media_type", "tv")
            tmdb_details = await tmdb_client.get_anime_details(tmdb_anime["id"], media_type)
            if not tmdb_details:
                return anime_data

            enhanced_data = anime_data.copy()

            # Stocker l'ID TMDB numérique et le type — utilisés par stremio_helpers
            # pour construire l'identifiant "tmdb:XXXXX" (architecture Stateless UI)
            enhanced_data["tmdb_id"] = tmdb_anime["id"]
            enhanced_data["tmdb_media_type"] = media_type  # "tv" ou "movie"

            if tmdb_details.get("images", {}).get("posters"):
                posters = tmdb_details["images"]["posters"]
                selected_poster = _select_best_image(posters, prefer_lang="fr") or _select_best_image(posters, prefer_lang="en")

                if selected_poster:
                    enhanced_data["poster"] = tmdb_client._get_poster_url(selected_poster["file_path"])
                    enhanced_data["image"] = enhanced_data["poster"]

            elif tmdb_details.get("poster_path"):
                enhanced_data["poster"] = tmdb_client._get_poster_url(tmdb_details["poster_path"])
                enhanced_data["image"] = enhanced_data["poster"]

            if tmdb_details.get("images", {}).get("backdrops"):
                backdrops = tmdb_details["images"]["backdrops"]
                no_lang_backdrops = [bg for bg in backdrops if bg.get("iso_639_1") is None]
                selected_background = _select_best_image(no_lang_backdrops)

                if selected_background:
                    enhanced_data["background"] = tmdb_client._get_backdrop_url(selected_background["file_path"])

            elif tmdb_details.get("backdrop_path"):
                enhanced_data["background"] = tmdb_client._get_backdrop_url(tmdb_details["backdrop_path"])

            if tmdb_details.get("images", {}).get("logos"):
                logos = tmdb_details["images"]["logos"]
                selected_logo = _select_best_image(logos, prefer_lang="fr") or _select_best_image(logos, prefer_lang="en")

                if selected_logo:
                    logo_url = tmdb_client._get_logo_url(selected_logo["file_path"])
                    enhanced_data["logo"] = logo_url

            tmdb_description = tmdb_details.get("overview", "").strip()
            if tmdb_description and len(tmdb_description) > 10:
                enhanced_data["description"] = tmdb_description
                enhanced_data["synopsis"] = tmdb_description

            if tmdb_details.get("videos"):
                trailer_id = tmdb_client._extract_trailer_id(tmdb_details["videos"])
                if trailer_id:
                    enhanced_data["trailers"] = [{"source": trailer_id, "type": "Trailer"}]

            start_year = None
            end_year = None

            if tmdb_details.get("first_air_date"):
                start_year = tmdb_details["first_air_date"][:4]

            if tmdb_details.get("last_air_date") and tmdb_details.get("status") == "Ended":
                end_year = tmdb_details["last_air_date"][:4]

            if start_year and end_year:
                if start_year == end_year:
                    enhanced_data["year_range"] = start_year
                else:
                    enhanced_data["year_range"] = f"{start_year}-{end_year}"
            elif start_year:
                if tmdb_details.get("status") in ["Returning Series", "In Production"]:
                    enhanced_data["year_range"] = f"{start_year}-"
                else:
                    enhanced_data["year_range"] = start_year

            if enhanced_data.get("year_range"):
                enhanced_data["year"] = enhanced_data["year_range"]

            imdb_id = None
            tmdb_rating = None
            if tmdb_details.get("external_ids", {}).get("imdb_id"):
                imdb_id = tmdb_details["external_ids"]["imdb_id"]
            if tmdb_details.get("vote_average") and tmdb_details["vote_average"] > 0:
                tmdb_rating = round(tmdb_details["vote_average"], 1)

            if imdb_id:
                enhanced_data["imdb_id"] = imdb_id
            if tmdb_rating:
                enhanced_data["tmdb_rating"] = tmdb_rating

            if media_type == "movie" and tmdb_details.get("runtime"):
                runtime = tmdb_details["runtime"]
                enhanced_data["runtime"] = f"{runtime} min"
            elif media_type == "tv":
                if tmdb_details.get("episode_run_time") and tmdb_details["episode_run_time"]:
                    episode_duration = tmdb_details["episode_run_time"][0]
                    enhanced_data["runtime"] = f"{episode_duration} min"

                else:
                    try:
                        tmdb_id = tmdb_details.get("id")
                        season_url = f"{tmdb_client.base_url}/tv/{tmdb_id}/season/1"
                        season_params = {
                            "api_key": tmdb_client.api_key,
                            "language": "fr-FR"
                        }
                        season_response = await tmdb_client.client.get(season_url, params=season_params)
                        season_data = safe_json_decode(season_response, f"TMDB saison 1 (ID {tmdb_id})", default=None)
                        if season_data and "episodes" in season_data and season_data["episodes"]:
                            first_episode = season_data["episodes"][0]
                            if first_episode.get("runtime"):
                                runtime = first_episode["runtime"]
                                enhanced_data["runtime"] = f"{runtime} min"

                    except Exception as e:
                        logger.debug(f"Erreur récupération durée S1E1: {e}")
            return enhanced_data

        except Exception as e:
            logger.error(f"Erreur enrichissement TMDB pour '{title}': {e}")
            return anime_data

    async def _create_tmdb_episodes_map(self, tmdb_client: TMDBClient, tmdb_id: int, seasons: List[Dict]) -> Dict[str, Dict]:

        episodes_map = {}
        normal_seasons = [s for s in seasons if s.get("season_number", 0) > 0]
        season_tasks = [
            tmdb_client.get_season_details(tmdb_id, season["season_number"])
            for season in normal_seasons
        ]

        season_results = await asyncio.gather(*season_tasks, return_exceptions=True)

        for i, season_data in enumerate(season_results):
            if isinstance(season_data, Exception) or not season_data:
                continue

            season_number = normal_seasons[i]["season_number"]

            if "episodes" in season_data:
                for episode in season_data["episodes"]:
                    episode_number = episode.get("episode_number")
                    if episode_number:
                        key = f"s{season_number}e{episode_number}"
                        episodes_map[key] = episode

        return episodes_map

    async def get_episodes_mapping(self, anime_data: Dict[str, Any], config: ConfigModel) -> Dict[str, Dict]:

        if not config.tmdbEnabled:
            return {}

        tmdb_client = self._get_tmdb_client(config)
        if not tmdb_client:
            return {}

        try:
            title = anime_data.get("title", anime_data.get("name", ""))
            clean_title = normalize_title(title, for_search=True)
            tmdb_anime = await tmdb_client.search_anime(clean_title)
            if not tmdb_anime:
                return {}
            tmdb_details = await tmdb_client.get_anime_details(tmdb_anime["id"])
            if not tmdb_details or not tmdb_details.get("seasons"):
                return {}
            episodes_map = await self._create_tmdb_episodes_map(
                tmdb_client, tmdb_anime["id"], tmdb_details["seasons"]
            )

            return episodes_map

        except Exception as e:
            logger.error(f"Erreur création mapping épisodes TMDB: {e}")
            return {}


# ===========================
# Instance Singleton Globale
# ===========================
tmdb_service = TMDBService()
