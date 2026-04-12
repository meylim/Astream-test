"""
Validation Kitsu — Filtre croisé universel AStream.

Portage async du script anim.py — appliqué à TOUS les catalogues :
  - Résultats Cinemeta (search)        → is_valid_anime_kitsu(query, item)
  - Résultats Jikan (genres, top…)     → filter_jikan_items(items)

Règles communes :
  1. Longueur : refuse si le titre est plus court que la requête
  2. Segmentation : choisit le bon segment sur ':' selon la requête
  3. Anti-parasites : reaction, abridged, vf, live action…
  4. Nettoyage du terme : supprime movie/season/part…
  5. Lookup Kitsu par IMDb ID (si disponible)
  6. Recherche textuelle + fallback segment court
  7. Anti-music : ignore les résultats de type 'music'
"""
import asyncio
import re
from typing import Tuple, Optional, Dict, Any, List

from astream.utils.http_client import http_client, safe_json_decode
from astream.utils.cache import CacheManager
from astream.utils.logger import logger

KITSU_BASE = "https://kitsu.io/api/edge"

_PARASITES = [
    "reaction", "abridged", "fan made", "review",
    "blind wave", "vostfr", "vf", "live action", "trailer",
]


# ===========================
# Helpers textuels
# ===========================

def _normalize(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]", "", text.lower())


def _get_best_segment(full_title: str, query: str) -> str:
    if ":" not in full_title:
        return full_title
    query_words = set(query.lower().split())
    segments = full_title.split(":")
    best = max(segments, key=lambda s: len(set(s.lower().split()) & query_words))
    return best.strip()


def _clean_search_term(segment: str) -> str:
    return re.sub(
        r"(?i)\s*(the movie|movie|film|part|cour|season|series|version|memories|special).*",
        "",
        segment,
    ).strip()


# ===========================
# Requêtes Kitsu (async + cache 24h)
# ===========================

async def _kitsu_get(url: str, cache_key: str, ttl: int = 86400) -> Optional[Dict]:
    async def fetch():
        try:
            resp = await http_client.get(url, headers={"Accept": "application/vnd.api+json"})
            if resp.status_code != 200:
                return None
            return safe_json_decode(resp, f"Kitsu {url}", default=None)
        except Exception as e:
            logger.warning(f"KITSU: {url} -> {e}")
            return None
    try:
        return await CacheManager.get_or_fetch(
            cache_key=cache_key, fetch_func=fetch,
            lock_key=f"lock:{cache_key}", ttl=ttl,
        )
    except Exception as e:
        logger.error(f"KITSU cache: {e}")
        return None


async def _fetch_by_imdb(imdb_id: str) -> List[Dict]:
    url = f"{KITSU_BASE}/anime?filter[external_links]=https://www.imdb.com/title/{imdb_id}"
    data = await _kitsu_get(url, f"kitsu:imdb:{imdb_id}")
    return (data or {}).get("data", [])


async def _fetch_by_text(query: str, limit: int = 5) -> List[Dict]:
    safe_q = query.replace(" ", "%20")
    url = f"{KITSU_BASE}/anime?filter[text]={safe_q}&page[limit]={limit}"
    data = await _kitsu_get(url, f"kitsu:text:{_normalize(query)}")
    return (data or {}).get("data", [])


def _match_kitsu_results(kitsu_results: List[Dict], norm_search: str) -> Tuple[bool, str]:
    for anime in kitsu_results:
        attr = anime.get("attributes", {})
        if str(attr.get("subtype", "")).lower() == "music":
            continue
        k_titles: List[Optional[str]] = [
            attr.get("canonicalTitle"),
            (attr.get("titles") or {}).get("en"),
            (attr.get("titles") or {}).get("en_jp"),
            (attr.get("slug") or "").replace("-", " "),
        ]
        abbr = attr.get("abbreviatedTitles")
        if abbr:
            k_titles.extend(abbr)
        for kt in k_titles:
            if kt:
                norm_kt = _normalize(kt)
                if norm_search and (norm_search in norm_kt or norm_kt in norm_search):
                    k_type = attr.get("subtype", "Unknown")
                    canonical = attr.get("canonicalTitle", "?")
                    return True, f"Valide Kitsu: '{canonical}' [type={k_type}]"
    return False, "Inconnu de la base Kitsu"


# ===========================
# Validation item Cinemeta
# (search — cinemeta/client.py)
# ===========================

async def is_valid_anime_kitsu(query: str, item: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Valide qu'un résultat Cinemeta est bien un anime Kitsu.
    query : terme utilisateur / item : meta Cinemeta {'name':..., 'id': 'tt...'}
    """
    cinemeta_name: str = item.get("name", "")
    query_clean = query.lower().strip()

    if len(cinemeta_name.replace(" ", "")) < len(query_clean.replace(" ", "")):
        return False, f"Refus: titre trop court"

    target_segment = _get_best_segment(cinemeta_name, query_clean)

    for p in _PARASITES:
        if p in cinemeta_name.lower():
            return False, f"Rejeté: parasite ({p!r})"

    search_term = _clean_search_term(target_segment.lower())
    norm_search = _normalize(search_term)
    kitsu_results: List[Dict] = []

    imdb_id: Optional[str] = item.get("id")
    if imdb_id and imdb_id.startswith("tt"):
        try:
            kitsu_results = await _fetch_by_imdb(imdb_id)
        except Exception as e:
            logger.warning(f"KITSU imdb {imdb_id}: {e}")

    if not kitsu_results:
        try:
            kitsu_results = await _fetch_by_text(search_term)
            if not kitsu_results and ":" in target_segment:
                short_term = target_segment.split(":")[0].strip()
                kitsu_results = await _fetch_by_text(short_term)
        except Exception as e:
            logger.warning(f"KITSU text '{search_term}': {e}")

    return _match_kitsu_results(kitsu_results, norm_search)


# ===========================
# Validation item Jikan
# (tous les catalogues — catalog.py)
# ===========================

async def is_valid_jikan_item(title: str, imdb_id: Optional[str] = None) -> Tuple[bool, str]:
    """
    Valide un anime Jikan via Kitsu.
    Le titre joue le rôle de query ET de nom (les items Jikan sont déjà des anime).
    """
    if not title:
        return False, "Titre vide"

    title_clean = title.strip()
    for p in _PARASITES:
        if p in title_clean.lower():
            return False, f"Rejeté: parasite ({p!r})"

    target_segment = _get_best_segment(title_clean, title_clean)
    search_term = _clean_search_term(target_segment.lower())
    norm_search = _normalize(search_term)

    if not norm_search:
        return False, "Terme vide après nettoyage"

    kitsu_results: List[Dict] = []

    if imdb_id and imdb_id.startswith("tt"):
        try:
            kitsu_results = await _fetch_by_imdb(imdb_id)
        except Exception as e:
            logger.warning(f"KITSU imdb {imdb_id}: {e}")

    if not kitsu_results:
        try:
            kitsu_results = await _fetch_by_text(search_term)
            if not kitsu_results and ":" in target_segment:
                short_term = target_segment.split(":")[0].strip()
                kitsu_results = await _fetch_by_text(short_term)
        except Exception as e:
            logger.warning(f"KITSU text '{search_term}': {e}")

    return _match_kitsu_results(kitsu_results, norm_search)


async def filter_jikan_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filtre une liste d'items Jikan (format interne AStream) via Kitsu.
    Validations en parallèle. En cas d'erreur sur un item, il est conservé.
    """
    if not items:
        return []

    async def _check(item: Dict) -> Tuple[Dict, bool, str]:
        title = item.get("title", "")
        imdb_id = item.get("imdb_id")
        try:
            ok, reason = await is_valid_jikan_item(title, imdb_id)
        except Exception as e:
            logger.warning(f"KITSU filter_jikan '{title}': {e}")
            ok, reason = True, "Erreur — conservé par défaut"
        return item, ok, reason

    results = await asyncio.gather(*[_check(i) for i in items], return_exceptions=True)

    valid: List[Dict] = []
    rejected = 0
    for r in results:
        if isinstance(r, Exception):
            continue
        item, ok, reason = r
        if ok:
            valid.append(item)
            logger.debug(f"KITSU OK  {item.get('title','?')} — {reason}")
        else:
            rejected += 1
            logger.debug(f"KITSU NOK {item.get('title','?')} — {reason}")

    logger.debug(f"filter_jikan: {len(valid)} OK / {rejected} rejetés sur {len(items)}")
    return valid
