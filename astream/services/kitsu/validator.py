"""
Validation Kitsu — Filtre croisé pour les résultats Cinemeta.

Portage async du script anim.py :
  - Segmentation du titre (split sur ':')
  - Nettoyage/normalisation textuelle
  - Validation via API Kitsu (ID IMDb direct ou recherche textuelle + fallback)
  - Filtre anti-parasites et anti-music

Ce module remplace le filtre heuristique `_is_likely_anime()` de Cinemeta
par une vérification réelle dans la base de données Kitsu.
"""
import re
from typing import Tuple, Optional, Dict, Any, List

from astream.utils.http_client import http_client, safe_json_decode
from astream.utils.cache import CacheManager
from astream.utils.logger import logger

KITSU_BASE = "https://kitsu.io/api/edge"

# Mots-clés parasites : contenus à exclure (réactions, doublages, live actions…)
_PARASITES = [
    "reaction", "abridged", "fan made", "review",
    "blind wave", "vostfr", "vf", "live action", "trailer",
]


# ===========================
# Helpers textuels
# ===========================

def _normalize(text: str) -> str:
    """Minuscules + suppression de tout ce qui n'est pas alphanumérique."""
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]", "", text.lower())


def _get_best_segment(full_title: str, query: str) -> str:
    """
    Quand un titre contient ':', choisit le segment le plus proche de la
    recherche utilisateur (mesure par mots communs).
    Ex : 'Dragon Ball Z: Resurrection F' + query 'dragon ball' → 'Dragon Ball Z'
    """
    if ":" not in full_title:
        return full_title
    query_words = set(query.lower().split())
    segments = full_title.split(":")
    best = max(segments, key=lambda s: len(set(s.lower().split()) & query_words))
    return best.strip()


def _clean_search_term(segment: str) -> str:
    """
    Supprime les suffixes génériques pour obtenir un terme de recherche API propre.
    Ex : 'Sword Art Online: The Movie' → 'Sword Art Online'
    """
    cleaned = re.sub(
        r"(?i)\s*(the movie|movie|film|part|cour|season|series|version|memories|special).*",
        "",
        segment,
    ).strip()
    return cleaned


# ===========================
# Requêtes Kitsu (async)
# ===========================

async def _kitsu_get(url: str, cache_key: str, ttl: int = 86400) -> Optional[Dict]:
    """GET Kitsu avec cache 24h."""
    async def fetch():
        try:
            resp = await http_client.get(url, headers={"Accept": "application/vnd.api+json"})
            if resp.status_code != 200:
                return None
            return safe_json_decode(resp, f"Kitsu {url}", default=None)
        except Exception as e:
            logger.warning(f"KITSU: {url} → {e}")
            return None

    try:
        return await CacheManager.get_or_fetch(
            cache_key=cache_key,
            fetch_func=fetch,
            lock_key=f"lock:{cache_key}",
            ttl=ttl,
        )
    except Exception as e:
        logger.error(f"KITSU cache: {e}")
        return None


async def _fetch_by_imdb(imdb_id: str) -> List[Dict]:
    """Recherche Kitsu par ID IMDb (lien externe)."""
    url = f"{KITSU_BASE}/anime?filter[external_links]=https://www.imdb.com/title/{imdb_id}"
    cache_key = f"kitsu:imdb:{imdb_id}"
    data = await _kitsu_get(url, cache_key)
    return (data or {}).get("data", [])


async def _fetch_by_text(query: str, limit: int = 5) -> List[Dict]:
    """Recherche Kitsu par texte."""
    safe_q = query.replace(" ", "%20")
    url = f"{KITSU_BASE}/anime?filter[text]={safe_q}&page[limit]={limit}"
    cache_key = f"kitsu:text:{_normalize(query)}"
    data = await _kitsu_get(url, cache_key)
    return (data or {}).get("data", [])


# ===========================
# Validation principale
# ===========================

async def is_valid_anime_kitsu(query: str, item: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Valide qu'un résultat Cinemeta est bien un anime connu de Kitsu.

    Args:
        query:  Terme de recherche original de l'utilisateur.
        item:   Un élément 'meta' retourné par Cinemeta (doit avoir 'name' et 'id').

    Returns:
        (True,  raison)  si l'item est un anime validé par Kitsu.
        (False, raison)  sinon.
    """
    cinemeta_name: str = item.get("name", "")
    query_clean = query.lower().strip()

    # --- RÈGLE 1 : LONGUEUR ---
    # Refus si le nom Cinemeta est plus court que la requête (ex : abréviation)
    if len(cinemeta_name.replace(" ", "")) < len(query_clean.replace(" ", "")):
        return False, f"Refus : titre trop court ({len(cinemeta_name)} < {len(query_clean)})"

    # --- RÈGLE 2 : SEGMENTATION ---
    target_segment = _get_best_segment(cinemeta_name, query_clean)

    # --- RÈGLE 3 : PARASITES ---
    name_lower = cinemeta_name.lower()
    for p in _PARASITES:
        if p in name_lower:
            return False, f"Rejeté : contenu parasite ({p!r})"

    # --- RÈGLE 4 : NETTOYAGE DU TERME DE RECHERCHE ---
    search_term = _clean_search_term(target_segment.lower())
    norm_search = _normalize(search_term)

    imdb_id: Optional[str] = item.get("id")
    kitsu_results: List[Dict] = []

    # --- RÈGLE 5 : VALIDATION PAR ID IMDB ---
    if imdb_id and imdb_id.startswith("tt"):
        try:
            kitsu_results = await _fetch_by_imdb(imdb_id)
        except Exception as e:
            logger.warning(f"KITSU: lookup imdb {imdb_id} → {e}")

    # --- RÈGLE 6 : VALIDATION TEXTUELLE + FALLBACK ---
    if not kitsu_results:
        try:
            kitsu_results = await _fetch_by_text(search_term)

            # Fallback : si aucun résultat et le titre est composé (ex: "DBZ: Gaiden Trunks")
            if not kitsu_results and ":" in target_segment:
                short_term = target_segment.split(":")[0].strip()
                kitsu_results = await _fetch_by_text(short_term)
        except Exception as e:
            logger.warning(f"KITSU: text search '{search_term}' → {e}")

    # --- ANALYSE DES RÉSULTATS ---
    for anime in kitsu_results:
        attr = anime.get("attributes", {})

        # Filtre anti-music (clips, openings…)
        if str(attr.get("subtype", "")).lower() == "music":
            continue

        # Collecte tous les titres disponibles
        k_titles: List[Optional[str]] = [
            attr.get("canonicalTitle"),
            (attr.get("titles") or {}).get("en"),
            (attr.get("titles") or {}).get("en_jp"),
            (attr.get("slug") or "").replace("-", " "),
        ]
        abbr = attr.get("abbreviatedTitles")
        if abbr:
            k_titles.extend(abbr)

        # Comparaison normalisée (inclusion dans les deux sens)
        for kt in k_titles:
            if kt:
                norm_kt = _normalize(kt)
                if norm_search and (norm_search in norm_kt or norm_kt in norm_search):
                    k_type = attr.get("subtype", "Unknown")
                    canonical = attr.get("canonicalTitle", "?")
                    return True, f"Validé Kitsu : '{canonical}' [type={k_type}]"

    return False, "Inconnu de la base Kitsu"
