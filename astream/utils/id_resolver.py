"""
Traducteur d'IDs externes → slug Anime-Sama.

Flux :
  1. ID IMDb (tt...) ou Kitsu (kitsu...)
  2. Appel API Kitsu pour récupérer le titre japonais + anglais
  3. Recherche du titre sur Anime-Sama
  4. Retourne le slug du premier résultat correspondant
"""

from typing import Optional, Dict, Any, List
import re

from astream.utils.logger import logger
from astream.utils.cache import CacheManager


# ===========================
# Constantes
# ===========================
KITSU_API_BASE = "https://kitsu.app/api/edge"
KITSU_ANILIST_BASE = "https://graphql.anilist.co"
JIKAN_API_BASE = "https://api.jikan.moe/v4"


# ===========================
# Résolution Jikan MAL ID → titres
# ===========================
async def _resolve_jikan_mal_id(mal_id: str, http_client) -> Optional[Dict[str, str]]:
    """
    Interroge l'API Jikan pour récupérer les titres d'un anime via son MAL ID.
    Retourne un dict avec les clés 'en', 'ja', 'canonical'.
    """
    try:
        url = f"{JIKAN_API_BASE}/anime/{mal_id}"
        response = await http_client.get(url)
        response.raise_for_status()
        data = response.json()
        anime = data.get("data", {})

        return {
            "en": anime.get("title_english") or "",
            "ja": anime.get("title") or "",  # Généralement en Romaji
            "canonical": anime.get("title_english") or anime.get("title") or "",
        }
    except Exception as e:
        logger.warning(f"ID_RESOLVER: Erreur Jikan pour MAL {mal_id}: {e}")
        return None


# ===========================
# Résolution Kitsu ID → titre
# ===========================
async def _resolve_kitsu_id(kitsu_id: str, http_client) -> Optional[Dict[str, str]]:
    """
    Interroge l'API Kitsu pour récupérer les titres d'un anime via son ID Kitsu.
    Retourne un dict avec les clés 'en', 'ja', 'canonical'.
    """
    try:
        url = f"{KITSU_API_BASE}/anime/{kitsu_id}"
        response = await http_client.get(url, headers={"Accept": "application/vnd.api+json"})
        response.raise_for_status()
        data = response.json()

        attrs = data.get("data", {}).get("attributes", {})
        titles = attrs.get("titles", {})

        return {
            "en": titles.get("en") or titles.get("en_jp") or "",
            "ja": titles.get("ja_jp") or "",
            "canonical": attrs.get("canonicalTitle") or "",
        }
    except Exception as e:
        logger.warning(f"ID_RESOLVER: Erreur API Kitsu pour {kitsu_id}: {e}")
        return None


# ===========================
# Résolution IMDb ID → titre via Kitsu
# ===========================
async def _resolve_imdb_id(imdb_id: str, http_client) -> Optional[Dict[str, str]]:
    """
    Cherche un anime sur Kitsu en utilisant l'ID IMDb comme référence croisée.
    Kitsu ne supporte pas la recherche directe par IMDb ID, donc on passe par
    l'API AniList qui a cette correspondance, puis on récupère le titre.
    """
    try:
        # AniList GraphQL supporte la recherche par idMal/imdbId
        query = """
        query ($search: String) {
            Media(type: ANIME, search: $search) {
                title { romaji english native }
                idMal
            }
        }
        """
        # Stratégie : on ne peut pas résoudre un tt... directement sans mapping.
        # On utilise l'API Open Movie Database (OMDb) gratuite pour récupérer le titre,
        # puis on cherche ce titre sur Anime-Sama.
        omdb_url = f"https://www.omdbapi.com/?i={imdb_id}&apikey=trilogy"
        response = await http_client.get(omdb_url)

        if response.status_code == 200:
            data = response.json()
            if data.get("Response") == "True":
                return {
                    "en": data.get("Title", ""),
                    "ja": "",
                    "canonical": data.get("Title", ""),
                }

        # Fallback: essayer directement l'API Kitsu avec l'ID imdb comme mappingId
        # Certains clients utilisent kitsu:{id} pour les IDs Kitsu natifs
        return None

    except Exception as e:
        logger.warning(f"ID_RESOLVER: Erreur résolution IMDb {imdb_id}: {e}")
        return None


# ===========================
# Recherche sur Anime-Sama
# ===========================
async def _find_slug_from_titles(titles: Dict[str, str], animesama_api) -> Optional[str]:
    """
    Tente de trouver le slug Anime-Sama en cherchant avec les différents titres disponibles.
    Essaie d'abord le titre canonique, puis l'anglais, puis le japonais romanisé.
    """
    candidates = []
    if titles.get("canonical"):
        candidates.append(titles["canonical"])
    if titles.get("en") and titles["en"] not in candidates:
        candidates.append(titles["en"])
    if titles.get("ja") and titles["ja"] not in candidates:
        candidates.append(titles["ja"])

    for title in candidates:
        if not title:
            continue
        try:
            logger.log("ID_RESOLVER", f"Recherche Anime-Sama pour: '{title}'")
            results = await animesama_api.search_anime(title)
            if results:
                slug = results[0].get("slug") or results[0].get("id", "").replace("as:", "")
                if slug:
                    logger.log("ID_RESOLVER", f"Slug trouvé: '{slug}' pour titre '{title}'")
                    return slug
        except Exception as e:
            logger.warning(f"ID_RESOLVER: Erreur recherche pour '{title}': {e}")

    return None


# ===========================
# Point d'entrée principal
# ===========================
async def resolve_external_id_to_slug(
    external_id: str,
    http_client,
    animesama_api,
) -> Optional[str]:
    """
    Traduit un ID externe (tt.../kitsu:...) en slug Anime-Sama.

    Args:
        external_id: ID au format 'tt1234567' ou 'kitsu:12345' ou 'kitsu12345'
        http_client: Client HTTP disponible
        animesama_api: Instance de l'API Anime-Sama

    Returns:
        Le slug Anime-Sama correspondant, ou None si non trouvé.
    """
    cache_key = f"as:id_resolve:{external_id}"

    async def do_resolve():
        # --- Jikan ID (jikan:MAL_ID) ---
        jikan_match = re.match(r'^jikan:(\d+)$', external_id)
        if jikan_match:
            mal_id = jikan_match.group(1)
            logger.log("ID_RESOLVER", f"Résolution Jikan MAL ID: {mal_id}")
            titles = await _resolve_jikan_mal_id(mal_id, http_client)
            if not titles:
                return None
            slug = await _find_slug_from_titles(titles, animesama_api)
            return {"slug": slug} if slug else None

        # --- Kitsu ID ---
        kitsu_match = re.match(r'^kitsu[:\-]?(\d+)$', external_id)
        if kitsu_match:
            kitsu_id = kitsu_match.group(1)
            logger.log("ID_RESOLVER", f"Résolution Kitsu ID: {kitsu_id}")
            titles = await _resolve_kitsu_id(kitsu_id, http_client)
            if not titles:
                return None
            slug = await _find_slug_from_titles(titles, animesama_api)
            return {"slug": slug} if slug else None

        # --- IMDb ID ---
        if re.match(r'^tt\d+$', external_id):
            logger.log("ID_RESOLVER", f"Résolution IMDb ID: {external_id}")
            titles = await _resolve_imdb_id(external_id, http_client)
            if not titles:
                return None
            slug = await _find_slug_from_titles(titles, animesama_api)
            return {"slug": slug} if slug else None

        return None

    try:
        cached = await CacheManager.get_or_fetch(
            cache_key=cache_key,
            fetch_func=do_resolve,
            lock_key=f"lock:{cache_key}",
            ttl=86400,  # Cache 24h — les mappings ne changent pas
        )
        return cached.get("slug") if cached else None
    except Exception as e:
        logger.error(f"ID_RESOLVER: Erreur résolution '{external_id}': {e}")
        return None


# ===========================
# Helpers publics
# ===========================
def is_external_id(anime_id: str) -> bool:
    """Retourne True si l'ID n'est pas un ID natif Anime-Sama (as:...)."""
    return not anime_id.startswith("as:")


def extract_episode_info_from_id(episode_id: str):
    """
    Extrait les infos d'épisode d'un ID Stremio externe.
    Stremio envoie les streams sous la forme: {id}:{season}:{episode}
    Exemple: tt0388629:1:1  ou  kitsu:12:1:1
    
    Retourne (external_id, season, episode) ou None.
    """
    # Format Stremio pour series: id:season:episode
    parts = episode_id.split(":")

    # Cas jikan:12345:season:episode (4 parties)
    if parts[0] == "jikan" and len(parts) == 4:
        external_id = f"jikan:{parts[1]}"
        try:
            return external_id, int(parts[2]), int(parts[3])
        except ValueError:
            return None

    # Cas jikan:12345:episode (3 parties — saison implicite = 1)
    if parts[0] == "jikan" and len(parts) == 3:
        external_id = f"jikan:{parts[1]}"
        try:
            return external_id, 1, int(parts[2])
        except ValueError:
            return None

    # Cas kitsu:12345:season:episode (4 parties)
    if parts[0] == "kitsu" and len(parts) == 4:
        external_id = f"kitsu:{parts[1]}"
        try:
            return external_id, int(parts[2]), int(parts[3])
        except ValueError:
            return None

    # Cas kitsu:12345:episode (3 parties — format réel Stremio, saison implicite = 1)
    if parts[0] == "kitsu" and len(parts) == 3:
        external_id = f"kitsu:{parts[1]}"
        try:
            return external_id, 1, int(parts[2])
        except ValueError:
            return None

    # Cas tt1234567:season:episode
    if re.match(r'^tt\d+$', parts[0]) and len(parts) == 3:
        try:
            return parts[0], int(parts[1]), int(parts[2])
        except ValueError:
            return None

    # Cas kitsu12345:season:episode (sans séparateur deux-points)
    if re.match(r'^kitsu\d+$', parts[0]) and len(parts) == 3:
        try:
            return parts[0], int(parts[1]), int(parts[2])
        except ValueError:
            return None

    # Cas movie : ID seul sans épisode (tt1234567 ou kitsu:12345 ou jikan:12345)
    if re.match(r'^tt\d+$', episode_id) or re.match(r'^kitsu[:\-]?\d+$', episode_id):
        return episode_id, 1, 1

    if re.match(r'^jikan:\d+$', episode_id):
        return episode_id, 1, 1

    return None
