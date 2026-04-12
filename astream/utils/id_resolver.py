"""
Traducteur d'IDs externes → slug Anime-Sama.

Flux supportés :
  tmdb:XXXXX      → TMDB API → titre → recherche Anime-Sama
  jikan:XXXXX     → Jikan API → titre → recherche Anime-Sama
  kitsu:XXXXX     → Kitsu API → titre → recherche Anime-Sama
  tt...           → OMDb API  → titre → recherche Anime-Sama

Le cache de résolution est maintenu 24h (les mappings sont stables).
"""

import re
from typing import Optional, Dict, Any, List

from astream.utils.logger import logger
from astream.utils.cache import CacheManager


# ===========================
# Constantes API
# ===========================
JIKAN_API_BASE  = "https://api.jikan.moe/v4"
KITSU_API_BASE  = "https://kitsu.app/api/edge"
TMDB_API_BASE   = "https://api.themoviedb.org/3"


# ===========================
# Résolution TMDB ID → titres
# ===========================
async def _resolve_tmdb_id(tmdb_id: str, http_client) -> Optional[Dict[str, str]]:
    """
    Interroge l'API TMDB pour récupérer les titres d'un anime via son ID numérique.
    Essaie d'abord en tant que série TV, puis film.
    """
    from astream.config.settings import settings
    api_key = settings.TMDB_API_KEY
    if not api_key:
        logger.warning(f"ID_RESOLVER: Pas de TMDB_API_KEY pour résoudre tmdb:{tmdb_id}")
        return None

    for media_type in ("tv", "movie"):
        try:
            url = f"{TMDB_API_BASE}/{media_type}/{tmdb_id}"
            response = await http_client.get(url, params={
                "api_key": api_key,
                "language": "en-US",
                "append_to_response": "alternative_titles"
            })
            if response.status_code == 404:
                continue
            response.raise_for_status()
            data = response.json()

            # Collecter tous les titres disponibles
            titles = set()
            for key in ("name", "title", "original_name", "original_title"):
                val = data.get(key, "")
                if val:
                    titles.add(val.strip())

            # Titres alternatifs (EN, FR, romaji)
            alt = data.get("alternative_titles", {})
            for entry in alt.get("results", alt.get("titles", [])):
                t = (entry.get("title") or "").strip()
                lang = entry.get("iso_3166_1", "")
                if t and lang in ("US", "GB", "FR", "JP", ""):
                    titles.add(t)

            if not titles:
                continue

            titles_list = list(titles)
            # Préférer titre anglais comme canonique
            canonical = data.get("name") or data.get("title") or titles_list[0]

            logger.log("ID_RESOLVER", f"TMDB {tmdb_id} ({media_type}): {len(titles_list)} titres trouvés")
            return {
                "canonical": canonical,
                "en": data.get("name") or data.get("title") or "",
                "original": data.get("original_name") or data.get("original_title") or "",
                "all_titles": titles_list,
            }
        except Exception as e:
            logger.warning(f"ID_RESOLVER: Erreur TMDB {media_type}/{tmdb_id}: {e}")

    return None


# ===========================
# Résolution Jikan MAL ID → titres
# ===========================
async def _resolve_jikan_mal_id(mal_id: str, http_client) -> Optional[Dict[str, str]]:
    try:
        url = f"{JIKAN_API_BASE}/anime/{mal_id}"
        response = await http_client.get(url)
        response.raise_for_status()
        data = response.json()
        anime = data.get("data", {})
        return {
            "canonical": anime.get("title_english") or anime.get("title") or "",
            "en": anime.get("title_english") or "",
            "original": anime.get("title") or "",
            "all_titles": [
                anime.get("title_english") or "",
                anime.get("title") or "",
            ],
        }
    except Exception as e:
        logger.warning(f"ID_RESOLVER: Erreur Jikan MAL {mal_id}: {e}")
        return None


# ===========================
# Résolution Kitsu ID → titres
# ===========================
async def _resolve_kitsu_id(kitsu_id: str, http_client) -> Optional[Dict[str, str]]:
    try:
        url = f"{KITSU_API_BASE}/anime/{kitsu_id}"
        response = await http_client.get(url, headers={"Accept": "application/vnd.api+json"})
        response.raise_for_status()
        data = response.json()
        attrs = data.get("data", {}).get("attributes", {})
        titles = attrs.get("titles", {})
        canonical = attrs.get("canonicalTitle", "")
        return {
            "canonical": canonical or titles.get("en_jp") or "",
            "en": titles.get("en") or titles.get("en_jp") or "",
            "original": titles.get("ja_jp") or "",
            "all_titles": [v for v in titles.values() if v],
        }
    except Exception as e:
        logger.warning(f"ID_RESOLVER: Erreur Kitsu {kitsu_id}: {e}")
        return None


# ===========================
# Résolution IMDb ID → titres (via OMDb)
# ===========================
async def _resolve_imdb_id(imdb_id: str, http_client) -> Optional[Dict[str, str]]:
    try:
        response = await http_client.get(
            "https://www.omdbapi.com/",
            params={"i": imdb_id, "apikey": "trilogy"}
        )
        if response.status_code == 200:
            data = response.json()
            if data.get("Response") == "True":
                title = data.get("Title", "")
                return {
                    "canonical": title,
                    "en": title,
                    "original": "",
                    "all_titles": [title],
                }
    except Exception as e:
        logger.warning(f"ID_RESOLVER: Erreur IMDb {imdb_id}: {e}")
    return None


# ===========================
# Recherche du slug sur Anime-Sama
# ===========================
def _strip_season_suffix(title: str) -> str:
    """
    Supprime les suffixes de saison pour trouver la franchise racine.
    Ex: "Re:Zero 2nd Season Part 2" → "Re:Zero"
         "Attack on Titan Season 3" → "Attack on Titan"
    """
    import re as _re
    # Ordre important : du plus spécifique au plus général
    patterns = [
        r"[:\s]+(?:\d+(?:st|nd|rd|th)?\s+)?season\s+(?:part\s+\d+|cour\s+\d+|\d+)?$",
        r"[:\s]+(?:season|saison)\s+\d+$",
        r"\s+\d+(?:st|nd|rd|th)?\s+season.*$",
        r"\s+season\s+\d+.*$",
        r"\s+part\s+\d+$",
        r"\s+cour\s+\d+$",
        r"\s+(?:ii|iii|iv|2nd|3rd|4th|5th)\s*(?:season|part|cour).*$",
        r"\s+(?:second|third|fourth|fifth)\s+(?:season|part|cour).*$",
        r"\s+\(\d{4}\)$",     # "(2020)"
        r"\s+\d{4}$",          # trailing year
    ]
    cleaned = title.strip()
    for pat in patterns:
        cleaned = _re.sub(pat, "", cleaned, flags=_re.IGNORECASE).strip()
    return cleaned


def _normalize_for_length(title: str) -> str:
    """
    Normalise un titre pour la comparaison de longueur :
    - passe en minuscules
    - supprime les suffixes Part X / Season X / Saison X / Cour X
    - supprime la ponctuation de fin
    - strip les espaces
    Utilisé pour comparer deux titres "à iso-franchise".
    """
    import re as _re
    t = title.strip().lower()
    # Supprime les suffixes numériques de saison / partie / cour
    strip_patterns = [
        r"\s+part\s+\d+$",
        r"\s+cour\s+\d+$",
        r"[:\s]+(?:season|saison)\s+\d+$",
        r"\s+\d+(?:st|nd|rd|th)?\s+season.*$",
        r"\s+season\s+\d+.*$",
        r"\s+(?:ii|iii|iv|2nd|3rd|4th|5th)\s*(?:season|part|cour).*$",
        r"\s+(?:second|third|fourth|fifth)\s+(?:season|part|cour).*$",
        r"\s+\(\d{4}\)$",
        r"\s+\d{4}$",
    ]
    for pat in strip_patterns:
        t = _re.sub(pat, "", t, flags=_re.IGNORECASE).strip()
    # Supprime la ponctuation finale SAUF ! (qui fait partie de certains titres ex: Haikyu!!)
    t = t.rstrip(":.,-?")
    return t.strip()


def _pick_best_result(query: str, results: list) -> Optional[dict]:
    """
    Sélectionne le meilleur résultat AS parmi une liste, en comparant les
    longueurs de titres après avoir strippé Part X / Season X / Saison X.

    Logique de scoring (plus le score est bas, meilleur c'est) :
      score = abs(len(query_norm) - len(result_norm))

    En cas d'égalité, on préfère le résultat dont le titre normalisé
    commence par le query normalisé (préfixe).
    """
    if not results:
        return None
    if len(results) == 1:
        return results[0]

    query_norm = _normalize_for_length(query)
    best = None
    best_score = float("inf")

    for r in results:
        # Le titre du résultat peut être dans "name", "title" ou "slug"
        r_title = r.get("name") or r.get("title") or r.get("slug", "")
        r_norm = _normalize_for_length(r_title)

        # Score = différence de longueur après normalisation
        score = abs(len(query_norm) - len(r_norm))

        # Bonus si le résultat commence par le query normalisé
        if r_norm.startswith(query_norm) or query_norm.startswith(r_norm):
            score -= 1

        if score < best_score:
            best_score = score
            best = r

    return best


async def _find_slug_from_titles(titles: Dict[str, Any], animesama_api) -> Optional[str]:
    """
    Cherche le slug Anime-Sama en prioritisant la franchise RACINE.
    Stratégie :
      1. Titre racine (sans suffixe de saison) → meilleure chance de matcher AS
      2. Titre canonical complet
      3. Tous les titres alternatifs

    Quand AS renvoie plusieurs résultats, on sélectionne le meilleur via
    _pick_best_result() qui compare les longueurs après avoir strippé
    Part X / Season X / Saison X des deux côtés.
    """
    candidates = []

    # Priorité 1 : racine franchise (strip season suffix)
    for key in ("canonical", "en", "original"):
        t = titles.get(key, "")
        if t:
            root = _strip_season_suffix(t)
            if root and root != t and root not in candidates:
                candidates.insert(0, root)  # en tête de liste

    # Priorité 2 : titres complets
    if titles.get("canonical"):
        candidates.append(titles["canonical"])
    if titles.get("en") and titles["en"] not in candidates:
        candidates.append(titles["en"])
    if titles.get("original") and titles["original"] not in candidates:
        candidates.append(titles["original"])
    for t in titles.get("all_titles", []):
        if t and t not in candidates:
            candidates.append(t)
            # Ajouter aussi la version racine de chaque titre alternatif
            root = _strip_season_suffix(t)
            if root and root != t and root not in candidates:
                candidates.append(root)

    # Dédupliqer en conservant l'ordre
    seen = set()
    unique = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            unique.append(c)

    for title in unique:
        if not title or len(title) < 2:
            continue
        try:
            logger.log("ID_RESOLVER", f"Recherche AS pour: '{title}'")
            results = await animesama_api.search_anime(title)
            if results:
                # ── Moulinette de sélection ──────────────────────────────────
                # On compare les longueurs APRÈS avoir strippé Part X / Season X
                # des deux côtés pour éviter de rater la franchise racine.
                best = _pick_best_result(title, results)
                if best:
                    slug = best.get("slug") or best.get("id", "").replace("as:", "")
                    if slug:
                        logger.log("ID_RESOLVER", f"Slug trouvé: '{slug}' via '{title}' (score longueur normalisée)")
                        return slug
        except Exception as e:
            logger.warning(f"ID_RESOLVER: Erreur recherche '{title}': {e}")

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
    Traduit n'importe quel ID externe en slug Anime-Sama.

    Supporte :
      tmdb:XXXXX   — ID TMDB numérique (Cinemeta / architecture Stateless UI)
      jikan:XXXXX  — ID MAL via Jikan
      kitsu:XXXXX  — ID Kitsu
      kitsuXXXXX   — ID Kitsu sans séparateur
      tt...        — ID IMDb
    """
    cache_key = f"as:id_resolve:{external_id}"

    async def do_resolve():
        # --- Vérification cross_ref DB en premier (évite les appels API répétés) ---
        try:
            from astream.utils.cross_ref import get_xref_by_tmdb, get_xref_by_mal, get_xref_by_imdb
            _xref = None

            tmdb_chk = re.match(r'^tmdb:(\d+)$', external_id)
            if tmdb_chk:
                _xref = await get_xref_by_tmdb(int(tmdb_chk.group(1)))
            elif re.match(r'^jikan:(\d+)$', external_id):
                mal_id_chk = re.match(r'^jikan:(\d+)$', external_id).group(1)
                _xref = await get_xref_by_mal(int(mal_id_chk))
            elif re.match(r'^tt\d+$', external_id):
                _xref = await get_xref_by_imdb(external_id)

            if _xref and _xref.get("as_slug"):
                logger.log("ID_RESOLVER", f"xref DB hit: {external_id} → {_xref['as_slug']}")
                return {"slug": _xref["as_slug"]}
        except Exception as _e:
            logger.debug(f"ID_RESOLVER xref lookup: {_e}")

        # --- TMDB ID (tmdb:12345) ---
        tmdb_match = re.match(r'^tmdb:(\d+)$', external_id)
        if tmdb_match:
            tmdb_id = tmdb_match.group(1)
            logger.log("ID_RESOLVER", f"Résolution TMDB ID: {tmdb_id}")
            titles = await _resolve_tmdb_id(tmdb_id, http_client)
            if not titles:
                return None
            slug = await _find_slug_from_titles(titles, animesama_api)
            return {"slug": slug} if slug else None

        # --- Jikan ID (jikan:12345) ---
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
            ttl=86400,
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

    Supporte :
      tmdb:XXXXX:season:episode   (Cinemeta / Stateless UI)
      jikan:XXXXX:season:episode
      kitsu:XXXXX:season:episode
      tt...:season:episode
      as:slug:sSe E              (natif — géré par MediaIdParser, pas ici)

    Retourne (external_id, season, episode) ou None.
    """
    parts = episode_id.split(":")

    # --- tmdb:XXXXX:season:episode (4 parties) ---
    if parts[0] == "tmdb" and len(parts) == 4:
        external_id = f"tmdb:{parts[1]}"
        try:
            return external_id, int(parts[2]), int(parts[3])
        except ValueError:
            return None

    # --- tmdb:XXXXX:episode (3 parties — film ou saison implicite 1) ---
    if parts[0] == "tmdb" and len(parts) == 3:
        external_id = f"tmdb:{parts[1]}"
        try:
            return external_id, 1, int(parts[2])
        except ValueError:
            return None

    # --- tmdb:XXXXX seul (film) ---
    if re.match(r'^tmdb:\d+$', episode_id):
        return episode_id, 1, 1

    # --- jikan:XXXXX:season:episode (4 parties) ---
    if parts[0] == "jikan" and len(parts) == 4:
        external_id = f"jikan:{parts[1]}"
        try:
            return external_id, int(parts[2]), int(parts[3])
        except ValueError:
            return None

    # --- jikan:XXXXX:episode (3 parties) ---
    if parts[0] == "jikan" and len(parts) == 3:
        external_id = f"jikan:{parts[1]}"
        try:
            return external_id, 1, int(parts[2])
        except ValueError:
            return None

    # --- jikan:XXXXX seul ---
    if re.match(r'^jikan:\d+$', episode_id):
        return episode_id, 1, 1

    # --- kitsu:XXXXX:season:episode (4 parties) ---
    if parts[0] == "kitsu" and len(parts) == 4:
        external_id = f"kitsu:{parts[1]}"
        try:
            return external_id, int(parts[2]), int(parts[3])
        except ValueError:
            return None

    # --- kitsu:XXXXX:episode (3 parties) ---
    if parts[0] == "kitsu" and len(parts) == 3:
        external_id = f"kitsu:{parts[1]}"
        try:
            return external_id, 1, int(parts[2])
        except ValueError:
            return None

    # --- tt...:season:episode (3 parties) ---
    if re.match(r'^tt\d+$', parts[0]) and len(parts) == 3:
        try:
            return parts[0], int(parts[1]), int(parts[2])
        except ValueError:
            return None

    # --- kitsuXXXXX:season:episode (sans séparateur) ---
    if re.match(r'^kitsu\d+$', parts[0]) and len(parts) == 3:
        try:
            return parts[0], int(parts[1]), int(parts[2])
        except ValueError:
            return None

    # --- Films (ID seul sans épisode) ---
    if re.match(r'^tt\d+$', episode_id) or re.match(r'^kitsu[:\-]?\d+$', episode_id):
        return episode_id, 1, 1

    return None
