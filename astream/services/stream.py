from typing import List, Optional, Dict, Any
import asyncio

from astream.utils.logger import logger
from astream.scrapers.animesama.client import animesama_api
from astream.scrapers.animesama.player import animesama_player
from astream.utils.http_client import http_client
from astream.utils.parsers import MediaIdParser
from astream.scrapers.animesama.details import get_or_fetch_anime_details
from astream.scrapers.animesama.video_resolver import AnimeSamaVideoResolver
from astream.utils.data_loader import get_dataset_loader
from astream.utils.cache import CacheManager
from astream.utils.timing import FlowTimer, timed_step
from astream.utils.id_resolver import resolve_external_id_to_slug, is_external_id, extract_episode_info_from_id
from astream.config.settings import settings
from astream.utils.languages import filter_by_language, sort_by_language_priority
from astream.utils.stremio_helpers import format_stream_for_stremio


# ===========================
# Classe StreamService
# ===========================
class StreamService:
    """
    Service responsable de la résolution des streams vidéo.
    Gère la fusion dataset + scraping et le filtrage par langue.
    """

    def __init__(self):
        pass

    async def get_episode_streams(self, episode_id: str, language_filter: Optional[str] = None, language_order: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Récupère les streams disponibles pour un épisode.

        Args:
            episode_id: ID de l'épisode (format: as:slug:s1e1)
            language_filter: Filtre de langue (VOSTFR, VF, ou Tout)
            language_order: Ordre de priorité des langues (ex: VOSTFR,VF)
            config: Configuration utilisateur

        Returns:
            Liste des streams formatés pour Stremio
        """
        timer = FlowTimer("STREAM", episode_id)
        try:
            # --- Résolution ID externe (tt.../kitsu...) ---
            async with timed_step(timer, "id_resolve"):
                if is_external_id(episode_id):
                    ext_info = extract_episode_info_from_id(episode_id)
                    if not ext_info:
                        logger.warning(f"ID externe non parsable: {episode_id}")
                        return []
                    external_id, season_number, episode_number = ext_info
                    anime_slug = await resolve_external_id_to_slug(
                        external_id, http_client, animesama_api
                    )
                    if not anime_slug:
                        logger.warning(f"Impossible de résoudre {external_id} vers un slug Anime-Sama")
                        return []
                    logger.log("ID_RESOLVER", f"{external_id} → slug: {anime_slug}")
                else:
                    parsed_id = MediaIdParser.parse_episode_id(episode_id)
                    if not parsed_id or parsed_id['is_metadata_only']:
                        logger.error(f"Episode_id invalide ou métadonnées seulement: {episode_id}")
                        return []
                    anime_slug = parsed_id['anime_slug']
                    season_number = parsed_id['season_number']
                    episode_number = parsed_id['episode_number']

            logger.log("STREAM", f"Récupération streams {anime_slug} S{season_number}E{episode_number}")
            cache_key = f"as:{anime_slug}:s{season_number}e{episode_number}"
            lock_key = f"lock:stream:{anime_slug}:s{season_number}e{episode_number}"

            # Récupération player URLs avec DistributedLock pour éviter race conditions
            async def fetch_player_urls():
                logger.log("DATABASE", f"Cache miss {cache_key} - Extraction dataset + scraping puis fusion")
                # Fusion dataset + scraping en parallèle pour maximiser les sources disponibles
                dataset_task = asyncio.create_task(self._get_dataset_player_urls(anime_slug, season_number, episode_number, language_filter))
                scraping_task = asyncio.create_task(self._get_scraping_player_urls(anime_slug, season_number, episode_number, language_filter, config))
                dataset_players, scraping_players = await asyncio.gather(dataset_task, scraping_task, return_exceptions=True)

                if isinstance(dataset_players, Exception):
                    logger.warning(f"Erreur récupération players dataset: {dataset_players}")
                    dataset_players = []

                if isinstance(scraping_players, Exception):
                    logger.warning(f"Erreur récupération players scraping: {scraping_players}")
                    scraping_players = []

                all_players = dataset_players + scraping_players
                seen_urls = set()
                unique_players = []
                for player in all_players:
                    url = player.get("url")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        unique_players.append(player)

                if not unique_players:
                    logger.log("DATABASE", f"Aucun player trouvé pour {cache_key} - pas de cache")
                    return None

                cache_data = {
                    "player_urls": unique_players,
                    "anime_slug": anime_slug,
                    "season": season_number,
                    "episode": episode_number,
                    "language_filter": language_filter,
                    "total_players": len(unique_players)
                }
                logger.log("DATABASE", f"Cache set {cache_key} - {len(unique_players)} players fusionnés (dataset + scraping)")
                return cache_data

            async with timed_step(timer, "cache_lookup"):
                cached_players = await CacheManager.get_or_fetch(
                    cache_key=cache_key,
                    fetch_func=fetch_player_urls,
                    lock_key=lock_key,
                    ttl=settings.EPISODE_TTL
                )

            player_urls_with_language = cached_players.get("player_urls", []) if cached_players else []

            async with timed_step(timer, "video_resolve"):
                if player_urls_with_language:
                    video_http_client = await self._get_http_client()
                    resolver = AnimeSamaVideoResolver(video_http_client)

                    logger.log("STREAM", f"Extraction vidéos depuis {len(player_urls_with_language)} URLs")
                    video_urls_with_language = await resolver.extract_video_urls_from_players_with_language(
                        player_urls_with_language, config
                    )
                    unique_streams = []
                    for video_data in video_urls_with_language:
                        video_url = video_data.get("url", "")
                        language = video_data.get("language", "VOSTFR")

                        unique_streams.append(
                            format_stream_for_stremio(video_url, language, anime_slug, season_number)
                        )

                    logger.log("STREAM", f"Résultat: {len(unique_streams)} streams extraits")
                else:
                    unique_streams = []

            logger.log("STREAM", f"Résultat final: {len(unique_streams)} streams uniques")

            async with timed_step(timer, "filter_sort"):
                if language_filter and language_filter != "Tout":
                    unique_streams = filter_by_language(unique_streams, language_filter)
                if language_order:
                    unique_streams = sort_by_language_priority(unique_streams, language_order)

            timer.finish()
            return unique_streams

        except Exception as e:
            timer.finish()
            logger.error(f"Erreur récupération streams {episode_id}: {e}")
            return []

    # ===========================
    # Méthodes privées pour récupération des streams
    # ===========================
    async def _get_dataset_player_urls(self, anime_slug: str, season: int, episode: int, language_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        try:
            logger.log("DATASET", f"Extraction URLs player dataset pour {anime_slug} S{season}E{episode}")

            dataset_loader = get_dataset_loader()
            if not dataset_loader:
                logger.debug("DATASET: Loader non disponible")
                return []

            streams = await dataset_loader.get_streams(anime_slug, season, episode, language_filter)

            if not streams:
                return []
            player_urls_with_language = []
            for stream in streams:
                player_urls_with_language.append({
                    "url": stream.get("url", ""),
                    "language": stream.get("language", "VOSTFR").lower()
                })

            logger.log("DATASET", f"{len(player_urls_with_language)} URLs player dataset extraites")
            return player_urls_with_language

        except Exception as e:
            logger.error(f"Erreur récupération URLs player dataset: {e}")
            return []

    async def _get_scraping_player_urls(self, anime_slug: str, season: int, episode: int, language_filter: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            anime_data = await get_or_fetch_anime_details(animesama_api.details, anime_slug)
            if not anime_data:
                logger.warning(f"Aucune donnée trouvée pour {anime_slug}")
                return []

            seasons = anime_data.get("seasons", [])
            target_season = None

            for season_data in seasons:
                if season_data.get("season_number") == season:
                    target_season = season_data
                    break

            if not target_season:
                logger.warning(f"Saison {season} introuvable pour {anime_slug}")
                return []

            player_urls = await animesama_player.extractor.extract_player_urls_smart_mapping_with_language(
                anime_slug=anime_slug,
                season_data=target_season,
                episode_number=episode,
                language_filter=language_filter,
                config=config
            )

            if player_urls:
                logger.log("ANIMESAMA", f"{len(player_urls)} URLs player scrapées pour {anime_slug} S{season}E{episode}")

            return player_urls

        except Exception as e:
            logger.error(f"Erreur scraping URLs player: {e}")
            return []

    async def _get_http_client(self):
        return http_client


# ===========================
# Instance Singleton
# ===========================
stream_service = StreamService()
            
