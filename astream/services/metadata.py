import asyncio
from typing import Dict, Any, Optional, TYPE_CHECKING

from astream.utils.logger import logger
from astream.scrapers.animesama.client import animesama_api
from astream.scrapers.animesama.player import animesama_player
from astream.scrapers.animesama.details import get_or_fetch_anime_details
from astream.services.tmdb.service import tmdb_service
from astream.services.tmdb.client import TMDBClient
from astream.config.settings import settings, SEASON_TYPE_FILM
from astream.scrapers.animesama.helpers import parse_genres_string
from astream.utils.stremio_helpers import StremioMetaBuilder, StremioLinkBuilder
from astream.utils.id_resolver import resolve_external_id_to_slug, is_external_id
from astream.utils.http_client import http_client as global_http_client

if TYPE_CHECKING:
    from astream.scrapers.animesama.player import AnimeSamaPlayer
    from astream.scrapers.animesama.client import AnimeSamaAPI


# ===========================
# Classe MetadataService
# ===========================
class MetadataService:
    """
    Service responsable de la gestion des métadonnées d'anime.
    Gère la récupération des détails, la construction des listes de vidéos,
    et l'enrichissement TMDB des métadonnées.
    """

    def __init__(self):
        self.animesama_api = animesama_api
        self.tmdb_service = tmdb_service

    async def get_complete_anime_meta(self, anime_id: str, config, request, b64config: str) -> Dict[str, Any]:
        """
        Récupère les métadonnées complètes d'un anime pour Stremio.
        TOUTE la logique de construction des métadonnées.

        Args:
            anime_id: ID de l'anime (format: as:slug)
            config: Configuration utilisateur
            request: Objet Request FastAPI
            b64config: Configuration encodée base64

        Returns:
            Dictionnaire meta Stremio complet
        """
        # --- Résolution ID Jikan (jikan:MAL_ID) — chemin spécial ---
        if anime_id.startswith("jikan:"):
            return await self._get_jikan_meta(anime_id, config, request, b64config)

        # --- Résolution ID externe (tt.../kitsu...) ---
        if is_external_id(anime_id):
            from astream.scrapers.animesama.client import animesama_api as _api
            resolved_slug = await resolve_external_id_to_slug(anime_id, global_http_client, _api)
            if not resolved_slug:
                logger.warning(f"META: Impossible de résoudre {anime_id} vers un slug Anime-Sama")
                return {}
            anime_slug = resolved_slug
            logger.log("ID_RESOLVER", f"META: {anime_id} → slug: {anime_slug}")
        else:
            anime_slug = anime_id.replace("as:", "")

        anime_data = await self._get_anime_details(anime_slug)
        if not anime_data:
            return {}

        enhanced_anime_data = await self._apply_tmdb_enhancement(anime_data, config, self.tmdb_service, "métadonnées")

        tmdb_episodes_map = {}
        if config.tmdbEnabled and (config.tmdbApiKey or settings.TMDB_API_KEY):
            try:
                tmdb_episodes_map = await self.tmdb_service.get_episodes_mapping(enhanced_anime_data, config)
                logger.log("TMDB", f"Mapping épisodes TMDB récupéré: {len(tmdb_episodes_map)} épisodes")
            except Exception as e:
                logger.error(f"Erreur récupération mapping épisodes: {e}")
                tmdb_episodes_map = {}

        seasons = enhanced_anime_data.get("seasons", [])
        episodes_map = await self._build_episodes_mapping(seasons, anime_slug, animesama_player)

        intelligent_tmdb_map = await self._create_tmdb_episodes_mapping(
            config, enhanced_anime_data, self.tmdb_service, tmdb_episodes_map, seasons, episodes_map
        )

        videos = await self._build_videos_list(
            seasons, episodes_map, intelligent_tmdb_map, enhanced_anime_data,
            anime_slug, self.animesama_api, config
        )

        meta = StremioMetaBuilder.build_detail_meta(enhanced_anime_data, videos, config)

        genres = anime_data.get('genres', [])
        if isinstance(genres, str):
            genres = parse_genres_string(genres)
        meta['genres'] = genres

        genre_links = StremioLinkBuilder.build_genre_links(request, b64config, genres)
        imdb_links = StremioLinkBuilder.build_imdb_link(enhanced_anime_data)
        meta['links'] = genre_links + imdb_links

        return meta

    async def _get_anime_details(self, anime_slug: str) -> Optional[Dict[str, Any]]:
        try:
            anime_data = await get_or_fetch_anime_details(self.animesama_api.details, anime_slug)
            return anime_data
        except Exception as e:
            logger.error(f"Erreur récupération détails anime {anime_slug}: {e}")
            return None

    async def _apply_tmdb_enhancement(self, anime_data: dict, config, tmdb_service, context: str = "") -> dict:
        if not config.tmdbEnabled or not (config.tmdbApiKey or settings.TMDB_API_KEY):
            return anime_data

        if not anime_data:
            return anime_data

        try:
            enhanced_data = await tmdb_service.enhance_anime_metadata(anime_data, config)
            return enhanced_data
        except Exception as e:
            anime_slug = anime_data.get('slug', 'unknown')
            logger.error(f"Erreur enrichissement TMDB {context} pour {anime_slug}: {e}")
            return anime_data

    async def _build_episodes_mapping(self, seasons: list, anime_slug: str, animesama_player: "AnimeSamaPlayer") -> dict:
        detection_tasks = [self._detect_episodes_for_season(season, anime_slug, animesama_player) for season in seasons]
        episodes_results = await asyncio.gather(*detection_tasks)
        return dict(episodes_results)

    async def _create_tmdb_episodes_mapping(self, config, enhanced_anime_data: dict, tmdb_service,
                                            tmdb_episodes_map: dict, seasons: list, episodes_map: dict) -> dict:
        """
        Crée un mapping intelligent entre les épisodes TMDB et Anime-Sama.

        Args:
            config: Configuration utilisateur
            enhanced_anime_data: Données enrichies de l'anime
            tmdb_service: Service TMDB
            tmdb_episodes_map: Map des épisodes TMDB
            seasons: Liste des saisons
            episodes_map: Map des épisodes détectés

        Returns:
            Mapping intelligent des épisodes
        """
        if not (config.tmdbEnabled and config.tmdbEpisodeMapping and tmdb_episodes_map):
            return {}

        try:
            from astream.scrapers.animesama.tmdb_episode_mapper import create_intelligent_episode_mapping
            intelligent_tmdb_map = create_intelligent_episode_mapping(tmdb_episodes_map, seasons, episodes_map)
            return intelligent_tmdb_map
        except Exception as e:
            logger.error(f"Erreur création mapping intelligent: {e}")
            return {}

    async def _build_videos_list(self, seasons: list, episodes_map: dict, intelligent_tmdb_map: dict,
                                 enhanced_anime_data: dict, anime_slug: str, animesama_api: "AnimeSamaAPI", config) -> list:
        """
        Construit la liste complète des vidéos pour toutes les saisons.

        Args:
            seasons: Liste des saisons
            episodes_map: Map des épisodes par saison
            intelligent_tmdb_map: Mapping TMDB intelligent
            enhanced_anime_data: Données enrichies de l'anime
            anime_slug: Slug de l'anime
            animesama_api: API Anime-Sama
            config: Configuration utilisateur

        Returns:
            Liste des objets vidéo Stremio
        """
        videos = []
        tmdb_enriched_count = 0

        final_tmdb_map = intelligent_tmdb_map if intelligent_tmdb_map else {}
        if final_tmdb_map:
            logger.log("TMDB", f"Utilisation mapping intelligent: {len(final_tmdb_map)} correspondances")
        else:
            logger.log("TMDB", "Aucun mapping épisodes utilisé (désactivé ou sécurité)")

        for season in seasons:
            season_number = season.get('season_number')
            season_name = season.get('name')
            max_episodes = episodes_map.get(season_number, 0)

            # Ignorer les saisons sans épisodes détectés (pas de valeurs hardcodées)
            if max_episodes == 0:
                continue

            for episode_num in range(1, max_episodes + 1):
                episode_title, episode_overview = await self._get_episode_title_and_overview(
                    season_number, episode_num, anime_slug, enhanced_anime_data, season_name, animesama_api
                )

                video = {
                    "id": f"as:{anime_slug}:s{season_number}e{episode_num}",
                    "title": episode_title,
                    "season": season_number,
                    "episode": episode_num,
                    "thumbnail": enhanced_anime_data.get('image'),
                    "overview": episode_overview
                }

                if self._apply_tmdb_episode_metadata(video, final_tmdb_map, config, season_number, episode_num):
                    tmdb_enriched_count += 1

                videos.append(video)

        if tmdb_enriched_count > 0:
            logger.log("TMDB", f"Enrichissement épisodes: {tmdb_enriched_count}/{len(videos)} épisodes enrichis")

        logger.log("API", f"Métadonnées construites: {len(seasons)} saisons, {len(videos)} épisodes total")

        return videos

    # ===========================
    # Méthodes privées pour métadonnées
    # ===========================
    async def _detect_episodes_for_season(self, season: dict, anime_slug: str, animesama_player: "AnimeSamaPlayer") -> tuple:
        season_number = season.get('season_number')
        try:
            episode_counts_dict = await animesama_player.get_available_episodes_count(anime_slug, season)
            available_episodes = max(episode_counts_dict.values()) if episode_counts_dict and episode_counts_dict.values() else 0

            if available_episodes > 0:
                return season_number, available_episodes
            else:
                logger.debug(f"Aucun épisode détecté pour {anime_slug} S{season_number} - retour 0")
                return season_number, 0
        except Exception as e:
            logger.warning(f"Impossible de détecter le nombre d'épisodes pour {anime_slug} S{season_number}: {e}")
            return season_number, 0

    async def _get_episode_title_and_overview(self, season_number: int, episode_num: int, anime_slug: str,
                                              enhanced_anime_data: dict, season_name: str, animesama_api: "AnimeSamaAPI") -> tuple:
        if season_number == SEASON_TYPE_FILM:
            logger.log("API", f"FILM DETECTE - anime: {anime_slug}, saison: {season_number}, episode: {episode_num}")
            try:
                film_title = await animesama_api.get_film_title(anime_slug, episode_num)
                if film_title:
                    episode_title = film_title
                    episode_overview = enhanced_anime_data.get('synopsis', film_title)
                    logger.log("API", f"FILM - Titre final utilisé: '{episode_title}'")
                else:
                    episode_title = f"Film {episode_num}"
                    episode_overview = enhanced_anime_data.get('synopsis', f"Film {episode_num}")
                    logger.warning(f"FILM - Titre par défaut utilisé: '{episode_title}'")
            except Exception as e:
                logger.error(f"FILM - Erreur récupération titre {anime_slug} #{episode_num}: {e}")
                episode_title = f"Film {episode_num}"
                episode_overview = enhanced_anime_data.get('synopsis', f"Film {episode_num}")
        else:  # Épisodes normaux
            episode_title = f"Episode {episode_num}"
            episode_overview = enhanced_anime_data.get('synopsis', f"Episode {episode_num} de {season_name}")

        return episode_title, episode_overview

    def _apply_tmdb_episode_metadata(self, video: dict, final_tmdb_map: dict, config,
                                     season_number: int, episode_num: int) -> bool:
        episode_key = f"s{season_number}e{episode_num}"

        if episode_key in final_tmdb_map:
            tmdb_episode = final_tmdb_map[episode_key]

            if config.tmdbEpisodeMapping and season_number > 0:
                enriched = False
                if tmdb_episode.get("still_path"):
                    temp_client = TMDBClient(None)
                    video['thumbnail'] = temp_client.get_episode_image_url(tmdb_episode["still_path"])
                    enriched = True

                if tmdb_episode.get("air_date"):
                    video['released'] = f"{tmdb_episode['air_date']}T00:00:00.000Z"
                    enriched = True

                if tmdb_episode.get("name"):
                    video['title'] = tmdb_episode["name"]
                    enriched = True

                if tmdb_episode.get("overview") and len(tmdb_episode["overview"].strip()) > 10:
                    video['overview'] = tmdb_episode["overview"]
                    enriched = True

                return enriched

        return False


    # ===========================
    # Chemin Jikan : méta sans Anime-Sama (si slug non résolvable)
    # ===========================
    async def _get_jikan_meta(self, anime_id: str, config, request, b64config: str) -> Dict[str, Any]:
        """
        Construit les métadonnées pour un anime Jikan.
        1. Essaie de résoudre vers un slug Anime-Sama → méta complète avec épisodes
        2. Fallback : méta partielle depuis Jikan + enrichissement TMDB
        """
        from astream.services.jikan.service import jikan_service
        from astream.scrapers.animesama.client import animesama_api as _api

        # Étape 1 : tentative de résolution vers Anime-Sama
        resolved_slug = await resolve_external_id_to_slug(anime_id, global_http_client, _api)

        if resolved_slug:
            logger.log("ID_RESOLVER", f"META Jikan: {anime_id} → slug AS: {resolved_slug}")
            anime_data = await self._get_anime_details(resolved_slug)
            if anime_data:
                enhanced = await self._apply_tmdb_enhancement(anime_data, config, self.tmdb_service, "métadonnées")
                tmdb_episodes_map = {}
                if config.tmdbEnabled and (config.tmdbApiKey or settings.TMDB_API_KEY):
                    try:
                        tmdb_episodes_map = await self.tmdb_service.get_episodes_mapping(enhanced, config)
                    except Exception:
                        pass
                seasons = enhanced.get("seasons", [])
                episodes_map = await self._build_episodes_mapping(seasons, resolved_slug, animesama_player)
                intelligent_tmdb_map = await self._create_tmdb_episodes_mapping(
                    config, enhanced, self.tmdb_service, tmdb_episodes_map, seasons, episodes_map
                )
                videos = await self._build_videos_list(
                    seasons, episodes_map, intelligent_tmdb_map, enhanced,
                    resolved_slug, self.animesama_api, config
                )
                meta = StremioMetaBuilder.build_detail_meta(enhanced, videos, config)
                # Garder l'ID Jikan original pour cohérence
                meta["id"] = anime_id
                genres = enhanced.get("genres", [])
                if isinstance(genres, str):
                    genres = parse_genres_string(genres)
                meta["genres"] = genres
                meta["links"] = (
                    StremioLinkBuilder.build_genre_links(request, b64config, genres)
                    + StremioLinkBuilder.build_imdb_link(enhanced)
                )
                return meta

        # Étape 2 : fallback — méta partielle depuis Jikan + TMDB
        logger.log("JIKAN", f"META fallback Jikan pour {anime_id} (pas de slug AS trouvé)")
        import re as _re
        mal_match = _re.match(r"^jikan:(\d+)$", anime_id)
        if not mal_match:
            return {}

        mal_id = int(mal_match.group(1))
        jikan_data = await jikan_service.get_anime(mal_id)
        if not jikan_data:
            return {}

        enhanced = await self._apply_tmdb_enhancement(jikan_data, config, self.tmdb_service, "métadonnées Jikan")
        genres = enhanced.get("genres", [])
        meta = StremioMetaBuilder.build_detail_meta(enhanced, [], config)
        meta["id"] = anime_id
        meta["genres"] = genres if isinstance(genres, list) else []
        meta["links"] = (
            StremioLinkBuilder.build_genre_links(request, b64config, meta["genres"])
            + StremioLinkBuilder.build_imdb_link(enhanced)
        )
        # Informations supplémentaires Jikan
        if enhanced.get("mal_score"):
            meta.setdefault("description", "")
            meta["description"] = (meta.get("description", "") or "").strip()
        return meta


# ===========================
# Instance Singleton Globale
# ===========================
metadata_service = MetadataService()
            
