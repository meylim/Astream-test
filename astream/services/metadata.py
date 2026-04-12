"""
MetadataService — Pilier central de l'addon.

Chaîne de priorité pour construire la fiche d'un anime :
  1. Cinemeta   → structure des saisons/épisodes, titre officiel, synopsis EN,
                   poster/background, note IMDb, durée, cast
  2. Jikan/MAL  → poster haute qualité, score MAL, genres anime, status,
                   synopsis alternatif si Cinemeta n'en a pas
  3. TMDB       → images HD (logo, backdrop), description FR, trailer
  4. Anime-Sama → marquage des épisodes ayant des streams disponibles

La table `anime_xref` centralise les mappings d'IDs entre ces sources.
"""
import asyncio
import re
from typing import Dict, Any, Optional, List, TYPE_CHECKING

from astream.utils.logger import logger
from astream.scrapers.animesama.client import animesama_api
from astream.scrapers.animesama.player import animesama_player
from astream.scrapers.animesama.details import get_or_fetch_anime_details
from astream.services.tmdb.service import tmdb_service
from astream.services.tmdb.client import TMDBClient
from astream.services.cinemeta.client import cinemeta_client
from astream.config.settings import settings, SEASON_TYPE_FILM
from astream.scrapers.animesama.helpers import parse_genres_string
from astream.utils.stremio_helpers import StremioMetaBuilder, StremioLinkBuilder
from astream.utils.id_resolver import resolve_external_id_to_slug, is_external_id
from astream.utils.cross_ref import get_xref, get_or_resolve_xref, save_xref
from astream.utils.http_client import http_client as global_http_client

if TYPE_CHECKING:
    from astream.scrapers.animesama.player import AnimeSamaPlayer
    from astream.scrapers.animesama.client import AnimeSamaAPI


# ===========================
# Classe MetadataService
# ===========================
class MetadataService:
    """
    Construit les métadonnées complètes d'un anime pour Stremio.
    Fusionne Cinemeta + Jikan + TMDB + Anime-Sama en un seul objet méta.
    """

    def __init__(self):
        self.animesama_api = animesama_api
        self.tmdb_service = tmdb_service

    # ===========================
    # Point d'entrée principal
    # ===========================
    async def get_complete_anime_meta(self, anime_id: str, config, request, b64config: str) -> Dict[str, Any]:
        """
        Construit la fiche complète d'un anime.
        Supporte : as:slug, jikan:MAL_ID, tmdb:TMDB_ID, tt..., kitsu:...
        """
        # --- Résolution vers as:slug ---
        if anime_id.startswith("jikan:"):
            return await self._get_meta_from_jikan_id(anime_id, config, request, b64config)

        if is_external_id(anime_id):  # tmdb:, tt:, kitsu:
            resolved_slug = await resolve_external_id_to_slug(anime_id, global_http_client, self.animesama_api)
            if not resolved_slug:
                logger.warning(f"META: Impossible de résoudre {anime_id} → slug AS")
                return {}
            logger.log("ID_RESOLVER", f"META: {anime_id} → {resolved_slug}")
            anime_slug = resolved_slug
        else:
            anime_slug = anime_id.replace("as:", "")

        return await self._build_meta_for_slug(anime_slug, config, request, b64config)

    # ===========================
    # Build principal (par slug AS)
    # ===========================
    async def _build_meta_for_slug(
        self, anime_slug: str, config, request, b64config: str
    ) -> Dict[str, Any]:
        """
        Construit la fiche pour un slug Anime-Sama connu.
        Applique la chaîne de priorité : Cinemeta → Jikan → TMDB → AS
        """
        # 1. Données Anime-Sama (structure des saisons, épisodes disponibles)
        as_data = await self._get_anime_details(anime_slug)
        if not as_data:
            return {}

        title = as_data.get("title", anime_slug.replace("-", " ").title())

        # 2. Cross-ref : récupère ou résout tous les IDs
        tmdb_id_hint = as_data.get("tmdb_id")
        mal_id_hint = as_data.get("mal_id")
        xref = await get_or_resolve_xref(
            anime_slug, title, global_http_client,
            tmdb_api_key=config.tmdbApiKey or settings.TMDB_API_KEY,
            existing_tmdb_id=tmdb_id_hint,
            existing_mal_id=mal_id_hint,
        )

        imdb_id       = xref.get("imdb_id")
        tmdb_id       = xref.get("tmdb_id")
        mal_id        = xref.get("mal_id")
        cinemeta_type = xref.get("cinemeta_type", "series")

        # 3. Fetch en parallèle depuis toutes les sources
        cinemeta_task = self._fetch_cinemeta(imdb_id, cinemeta_type)
        jikan_task    = self._fetch_jikan(mal_id)
        tmdb_task     = self._fetch_tmdb(as_data, config) if (tmdb_id or title) else asyncio.sleep(0, result=None)

        cinemeta_meta, jikan_data, tmdb_data = await asyncio.gather(
            cinemeta_task, jikan_task, tmdb_task,
            return_exceptions=True,
        )
        cinemeta_meta = cinemeta_meta if not isinstance(cinemeta_meta, Exception) else None
        jikan_data    = jikan_data    if not isinstance(jikan_data, Exception) else None
        tmdb_data     = tmdb_data     if not isinstance(tmdb_data, Exception) else None

        # 4. Construire l'objet méta fusionné
        merged = await self._merge_metadata(
            anime_slug, as_data, cinemeta_meta, jikan_data, tmdb_data,
            xref, config
        )

        # 5. Construire la liste de vidéos (épisodes)
        videos = await self._build_videos(
            anime_slug, as_data, cinemeta_meta, tmdb_data, config, xref=xref
        )

        # 6. Assembler la réponse Stremio
        genres = merged.get("genres", [])
        meta = {
            "id": f"as:{anime_slug}",
            "type": "series" if cinemeta_type == "series" else "movie",
            "name": merged.get("title", title),
            "poster": merged.get("poster"),
            "background": merged.get("background"),
            "logo": merged.get("logo"),
            "description": merged.get("description"),
            "releaseInfo": merged.get("release_info"),
            "runtime": merged.get("runtime"),
            "imdbRating": merged.get("imdb_rating"),
            "genres": genres,
            "cast": merged.get("cast", []),
            "director": merged.get("director", []),
            "trailers": merged.get("trailers", []),
            "behaviorHints": {"hasScheduledVideos": True},
            "links": (
                StremioLinkBuilder.build_genre_links(request, b64config, genres)
                + self._build_imdb_link(imdb_id, merged.get("imdb_rating"))
            ),
        }

        if videos:
            meta["videos"] = videos

        # Nettoyer les None
        meta = {k: v for k, v in meta.items() if v is not None and v != [] and v != ""}

        logger.log("API", f"META {anime_slug}: {len(videos)} épisodes | Cinemeta={'✓' if cinemeta_meta else '✗'} Jikan={'✓' if jikan_data else '✗'} TMDB={'✓' if tmdb_data else '✗'}")
        return meta

    # ===========================
    # Fetchers individuels
    # ===========================
    async def _fetch_cinemeta(self, imdb_id: Optional[str], media_type: str) -> Optional[Dict]:
        if not imdb_id:
            return None
        try:
            return await cinemeta_client.get_meta(imdb_id, media_type)
        except Exception as e:
            logger.warning(f"META Cinemeta {imdb_id}: {e}")
            return None

    async def _fetch_jikan(self, mal_id: Optional[int]) -> Optional[Dict]:
        if not mal_id:
            return None
        try:
            from astream.services.jikan.client import jikan_client
            return await jikan_client.get_anime_by_id(mal_id)
        except Exception as e:
            logger.warning(f"META Jikan {mal_id}: {e}")
            return None

    async def _fetch_tmdb(self, as_data: Dict, config) -> Optional[Dict]:
        try:
            return await self.tmdb_service.enhance_anime_metadata(as_data, config)
        except Exception as e:
            logger.warning(f"META TMDB: {e}")
            return None

    async def _get_anime_details(self, anime_slug: str) -> Optional[Dict]:
        try:
            return await get_or_fetch_anime_details(self.animesama_api.details, anime_slug)
        except Exception as e:
            logger.error(f"META AS details {anime_slug}: {e}")
            return None

    # ===========================
    # Fusion des métadonnées
    # ===========================
    async def _merge_metadata(
        self,
        anime_slug: str,
        as_data: Dict,
        cinemeta_meta: Optional[Dict],
        jikan_data: Optional[Dict],
        tmdb_data: Optional[Dict],
        xref: Dict,
        config,
    ) -> Dict:
        """
        Fusionne les données de toutes les sources.
        Priorité : Cinemeta > TMDB > Jikan > Anime-Sama
        """
        merged: Dict[str, Any] = {}

        # --- Titre ---
        merged["title"] = (
            (cinemeta_meta or {}).get("name")
            or (tmdb_data or {}).get("title")
            or (jikan_data or {}).get("title_english")
            or as_data.get("title", "")
        )

        # --- Poster (Jikan > TMDB > Cinemeta — Jikan a les meilleurs posters anime) ---
        jikan_poster = None
        if jikan_data:
            imgs = jikan_data.get("images", {})
            jikan_poster = (
                imgs.get("webp", {}).get("large_image_url")
                or imgs.get("jpg", {}).get("large_image_url")
            )

        merged["poster"] = (
            jikan_poster
            or (tmdb_data or {}).get("poster")
            or (cinemeta_meta or {}).get("poster")
            or as_data.get("image")
        )

        # --- Background (TMDB > Cinemeta) ---
        merged["background"] = (
            (tmdb_data or {}).get("background")
            or (cinemeta_meta or {}).get("background")
        )

        # --- Logo (TMDB uniquement) ---
        merged["logo"] = (tmdb_data or {}).get("logo")

        # --- Description (TMDB FR > Cinemeta EN > Jikan EN > AS) ---
        tmdb_desc = (tmdb_data or {}).get("description") or (tmdb_data or {}).get("synopsis")
        cinemeta_desc = (cinemeta_meta or {}).get("description")
        jikan_desc = (jikan_data or {}).get("synopsis", "")
        if jikan_desc and jikan_desc.endswith("(Source: MAL Rewrite)"):
            jikan_desc = jikan_desc[:-21].strip()

        merged["description"] = (
            tmdb_desc
            or cinemeta_desc
            or jikan_desc
            or as_data.get("synopsis", "")
        )

        # --- Release info ---
        release_info = None
        if cinemeta_meta and cinemeta_meta.get("releaseInfo"):
            release_info = str(cinemeta_meta["releaseInfo"])
        elif tmdb_data and tmdb_data.get("year"):
            release_info = str(tmdb_data["year"])
        elif jikan_data and jikan_data.get("aired", {}).get("from"):
            release_info = jikan_data["aired"]["from"][:4]
        merged["release_info"] = release_info

        # --- Runtime ---
        runtime = (cinemeta_meta or {}).get("runtime") or (tmdb_data or {}).get("runtime")
        if not runtime and jikan_data:
            dur = jikan_data.get("duration", "")
            import re as _re
            m = _re.search(r"(\d+)\s*min", dur)
            if m:
                runtime = f"{m.group(1)} min"
        merged["runtime"] = runtime

        # --- Note IMDb ---
        imdb_rating = None
        if cinemeta_meta and cinemeta_meta.get("imdbRating"):
            imdb_rating = str(cinemeta_meta["imdbRating"])
        elif jikan_data and jikan_data.get("score"):
            imdb_rating = str(round(jikan_data["score"], 1))
        merged["imdb_rating"] = imdb_rating

        # --- Genres (Jikan > Cinemeta > AS — Jikan a les genres anime précis) ---
        genres: List[str] = []
        if jikan_data:
            for key in ("genres", "demographics", "themes"):
                for g in jikan_data.get(key, []):
                    name = g.get("name", "")
                    if name and name not in genres:
                        genres.append(name)
        if not genres and cinemeta_meta:
            genres = cinemeta_meta.get("genres", [])
        if not genres:
            genres_raw = as_data.get("genres", [])
            genres = parse_genres_string(genres_raw) if isinstance(genres_raw, str) else genres_raw

        merged["genres"] = genres

        # --- Cast & Director (Cinemeta) ---
        merged["cast"] = (cinemeta_meta or {}).get("cast", [])
        merged["director"] = (cinemeta_meta or {}).get("director", [])

        # --- Trailer (TMDB) ---
        trailers = (tmdb_data or {}).get("trailers", [])
        merged["trailers"] = trailers

        return merged

    # ===========================
    # Construction de la liste d'épisodes
    # ===========================
    async def _build_videos(
        self,
        anime_slug: str,
        as_data: Dict,
        cinemeta_meta: Optional[Dict],
        tmdb_data: Optional[Dict],
        config,
        xref: Optional[Dict] = None,
    ) -> List[Dict]:
        """
        Construit la liste de vidéos Stremio.

        Gestion du split-cour (ex. Re:Zero S2 Part 1 + Part 2 = Cinemeta S2) :
          - Utilise anime_db.build_season_concordance() pour mapper
            les saisons AS aux saisons Cinemeta avec le bon offset épisodique.
          - Cinemeta fournit les métadonnées de chaque épisode (titre, date, thumbnail).
          - Anime-Sama détermine quels épisodes ont des streams disponibles.
        """
        from astream.config.settings import SEASON_TYPE_FILM, SEASON_TYPE_OVA, SPECIAL_SEASON_THRESHOLD
        from astream.utils.anime_db import build_season_concordance

        seasons = as_data.get("seasons", [])
        if not seasons:
            return []

        # Détecter les épisodes disponibles sur Anime-Sama
        episodes_map = await self._detect_available_episodes(seasons, anime_slug)

        # Récupérer les vidéos Cinemeta indexées par (saison, épisode)
        cinemeta_videos = (cinemeta_meta or {}).get("videos", [])
        cinemeta_index: Dict[str, Dict] = {}
        for v in cinemeta_videos:
            s = v.get("season")
            e = v.get("episode")
            if s is not None and e is not None:
                cinemeta_index[f"s{s}e{e}"] = v

        # Construire la table de concordance saisonnière via anime_db
        # (gère le split-cour : AS S3 Part 2 → Cinemeta S2 avec offset)
        mal_id = (xref or {}).get("mal_id")
        normal_seasons = {
            s.get("season_number"): episodes_map.get(s.get("season_number"), 0)
            for s in seasons
            if s.get("season_number", 0) < SPECIAL_SEASON_THRESHOLD
            and s.get("season_number", 0) > 0
        }

        concordance: Dict[int, Dict] = {}
        if mal_id and normal_seasons:
            try:
                concordance = build_season_concordance(mal_id, normal_seasons)
            except Exception as e:
                logger.warning(f"Concordance saisonnière {anime_slug}: {e}")

        # Image par défaut
        default_thumb = (tmdb_data or {}).get("poster") or as_data.get("image")

        videos = []
        for season in seasons:
            season_number = season.get("season_number")
            max_episodes  = episodes_map.get(season_number, 0)
            if max_episodes == 0:
                continue

            # === Saisons spéciales (films, OVA) — pas de concordance ===
            if season_number == SEASON_TYPE_FILM or season_number >= SPECIAL_SEASON_THRESHOLD:
                for ep_num in range(1, max_episodes + 1):
                    if season_number == SEASON_TYPE_FILM:
                        try:
                            title = await self.animesama_api.get_film_title(anime_slug, ep_num)
                            ep_title = title or f"Film {ep_num}"
                        except Exception:
                            ep_title = f"Film {ep_num}"
                    else:
                        ep_title = f"Épisode spécial {ep_num}"

                    videos.append({
                        "id": f"as:{anime_slug}:s{season_number}e{ep_num}",
                        "title": ep_title,
                        "season": 0,  # Stremio affiche les spéciaux en S0
                        "episode": ep_num,
                        "thumbnail": default_thumb,
                        "overview": "",
                    })
                continue

            # === Saisons normales — avec concordance split-cour ===
            conf = concordance.get(season_number, {
                "cinemeta_season": season_number,
                "cinemeta_ep_offset": 0,
            })
            cinemeta_s      = conf.get("cinemeta_season", season_number)
            ep_offset       = conf.get("cinemeta_ep_offset", 0)
            is_split        = conf.get("is_split_cour", False)

            for ep_num in range(1, max_episodes + 1):
                # Calculer l'épisode correspondant dans Cinemeta
                cinemeta_ep_num = ep_offset + ep_num
                cinemeta_key    = f"s{cinemeta_s}e{cinemeta_ep_num}"
                cinemeta_ep     = cinemeta_index.get(cinemeta_key, {})

                ep_title = cinemeta_ep.get("title") or f"Épisode {ep_num}"

                thumbnail = cinemeta_ep.get("thumbnail") or default_thumb

                video: Dict[str, Any] = {
                    "id": f"as:{anime_slug}:s{season_number}e{ep_num}",
                    "title": ep_title,
                    "season": cinemeta_s,     # Numéro de saison Cinemeta (cohérent avec l'affichage)
                    "episode": cinemeta_ep_num,  # Épisode Cinemeta (avec offset split-cour)
                    "thumbnail": thumbnail,
                    "overview": cinemeta_ep.get("overview", ""),
                    "_as_season": season_number,  # Gardé pour résolution des streams
                    "_as_episode": ep_num,
                }

                if cinemeta_ep.get("released"):
                    video["released"] = cinemeta_ep["released"]

                videos.append(video)

        logger.log(
            "API",
            f"Vidéos {anime_slug}: {len(videos)} épisodes "
            f"(Cinemeta={len(cinemeta_index)}, concordance={len(concordance)} saisons)"
        )
        return videos

    async def _detect_available_episodes(self, seasons: list, anime_slug: str) -> Dict[int, int]:
        """Détecte le nombre d'épisodes disponibles sur Anime-Sama par saison."""
        tasks = [
            self._detect_season_episodes(season, anime_slug)
            for season in seasons
        ]
        results = await asyncio.gather(*tasks)
        return dict(results)

    async def _detect_season_episodes(self, season: dict, anime_slug: str) -> tuple:
        season_number = season.get("season_number")
        try:
            counts = await animesama_player.get_available_episodes_count(anime_slug, season)
            available = max(counts.values()) if counts else 0
            return season_number, available
        except Exception as e:
            logger.warning(f"Détection épisodes {anime_slug} S{season_number}: {e}")
            return season_number, 0

    # ===========================
    # Chemin Jikan (jikan:MAL_ID)
    # ===========================
    async def _get_meta_from_jikan_id(self, anime_id: str, config, request, b64config: str) -> Dict:
        """
        Gère les IDs jikan:MAL_ID.
        Essaie d'abord de résoudre vers un slug AS, sinon méta partielle.
        """
        m = re.match(r"^jikan:(\d+)$", anime_id)
        if not m:
            return {}

        mal_id = int(m.group(1))

        # Chercher si on a déjà un slug AS mappé
        from astream.utils.cross_ref import get_xref_by_mal
        xref = await get_xref_by_mal(mal_id)
        if xref and xref.get("as_slug"):
            return await self._build_meta_for_slug(xref["as_slug"], config, request, b64config)

        # Essayer de résoudre via le titre Jikan → AS search
        resolved_slug = await resolve_external_id_to_slug(anime_id, global_http_client, self.animesama_api)
        if resolved_slug:
            return await self._build_meta_for_slug(resolved_slug, config, request, b64config)

        # Fallback : méta partielle depuis Jikan + TMDB
        logger.log("JIKAN", f"META fallback pour jikan:{mal_id}")
        from astream.services.jikan.service import jikan_service
        jikan_data = await jikan_service.get_anime(mal_id)
        if not jikan_data:
            return {}

        as_data = {"title": jikan_data.get("title", ""), "genres": jikan_data.get("genres", []), "seasons": []}
        tmdb_data = await self._fetch_tmdb(as_data, config)

        imgs = jikan_data.get("_raw", {}).get("images", {}) if jikan_data.get("_raw") else {}
        poster = (
            imgs.get("webp", {}).get("large_image_url")
            or imgs.get("jpg", {}).get("large_image_url")
            or (tmdb_data or {}).get("poster")
        )
        genres = jikan_data.get("genres", [])
        if isinstance(genres, list) and genres and isinstance(genres[0], dict):
            genres = [g.get("name", "") for g in genres]

        return {
            "id": anime_id,
            "type": "movie" if jikan_data.get("_is_movie") else "series",
            "name": jikan_data.get("title", ""),
            "poster": poster,
            "background": (tmdb_data or {}).get("background"),
            "description": jikan_data.get("description", ""),
            "releaseInfo": jikan_data.get("year", ""),
            "runtime": jikan_data.get("runtime", ""),
            "imdbRating": str(jikan_data.get("mal_score", "")) if jikan_data.get("mal_score") else None,
            "genres": genres,
            "links": StremioLinkBuilder.build_genre_links(request, b64config, genres),
            "behaviorHints": {"hasScheduledVideos": False},
        }

    # ===========================
    # Helpers
    # ===========================
    @staticmethod
    def _build_imdb_link(imdb_id: Optional[str], rating: Optional[str]) -> List[Dict]:
        if not imdb_id:
            return []
        return [{
            "name": rating or "IMDb",
            "category": "imdb",
            "url": f"https://imdb.com/title/{imdb_id}",
        }]


# ===========================
# Instance Singleton Globale
# ===========================
metadata_service = MetadataService()
