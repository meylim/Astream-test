"""
Anime Offline Database (manami-project)
https://github.com/manami-project/anime-offline-database

Base de données JSON (JSONL minifié) qui cross-référence les anime entre :
  MyAnimeList, AniList, Kitsu, AniSearch, AniDB, LiveChart, Anime-Planet,
  AnimeCounting, ANN, SimKL…

Format d'une entrée :
  {
    "sources": ["https://myanimelist.net/anime/31240", ...],
    "title": "Re:Zero kara Hajimeru Isekai Seikatsu",
    "type": "TV",
    "episodes": 25,
    "status": "FINISHED",
    "animeSeason": {"season": "SPRING", "year": 2016},
    "picture": "https://...",
    "score": {...},
    "synonyms": [...],
    "relatedAnime": ["https://myanimelist.net/anime/39587", ...]
  }

On s'en sert pour :
  1. Peupler anime_xref avec les IDs MAL/AniList/Kitsu sans appels API
  2. Détecter les split-cours (Part 2, Cour 2…) pour le season mapper
  3. Lister les entrées d'une franchise (relatedAnime)
"""

import re
import json
import asyncio
import os
from typing import Optional, Dict, List, Any, Set

from astream.utils.logger import logger
from astream.utils.http_client import http_client
from astream.config.settings import settings

# URL de la base (minifiée pour économiser la bande passante)
ANIME_DB_URL = (
    "https://raw.githubusercontent.com/manami-project/anime-offline-database"
    "/master/anime-offline-database-minified.json"
)

# Chemin local de stockage
_DB_PATH = os.path.join("data", "anime-offline-db.json")

# Cache en mémoire (chargé une fois au démarrage)
_db_by_mal:     Dict[int, Dict]  = {}
_db_by_anilist: Dict[int, Dict]  = {}
_db_by_kitsu:   Dict[int, Dict]  = {}
_db_loaded = False


# ===========================
# Extraction d'IDs depuis les sources
# ===========================
def _extract_ids(sources: List[str]) -> Dict[str, Optional[int]]:
    ids: Dict[str, Optional[int]] = {
        "mal_id": None, "anilist_id": None, "kitsu_id": None,
        "anisearch_id": None, "anidb_id": None,
    }
    for src in sources:
        if m := re.search(r"myanimelist\.net/anime/(\d+)", src):
            ids["mal_id"] = int(m.group(1))
        elif m := re.search(r"anilist\.co/anime/(\d+)", src):
            ids["anilist_id"] = int(m.group(1))
        elif m := re.search(r"kitsu\.app/anime/(\d+)", src):
            ids["kitsu_id"] = int(m.group(1))
        elif m := re.search(r"anisearch\.com/anime/(\d+)", src):
            ids["anisearch_id"] = int(m.group(1))
        elif m := re.search(r"anidb\.net/anime/(\d+)", src):
            ids["anidb_id"] = int(m.group(1))
    return ids


# ===========================
# Chargement de la base
# ===========================
async def load_anime_db(force_refresh: bool = False) -> bool:
    """
    Charge la base en mémoire.
    - Si le fichier local existe et est < 24h → utilise le cache local
    - Sinon → télécharge depuis GitHub et sauvegarde
    Returns True si succès.
    """
    global _db_loaded, _db_by_mal, _db_by_anilist, _db_by_kitsu

    if _db_loaded and not force_refresh:
        return True

    data = None

    # Essayer le cache local d'abord
    if not force_refresh and os.path.exists(_DB_PATH):
        age = (asyncio.get_event_loop().time() -
               os.path.getmtime(_DB_PATH)) if hasattr(asyncio, 'get_event_loop') else 999999
        try:
            mtime = os.path.getmtime(_DB_PATH)
            import time
            age = time.time() - mtime
        except Exception:
            age = 999999

        if age < 86400:  # Moins de 24h
            try:
                with open(_DB_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.log("XREF", f"Anime-DB chargée depuis le cache local ({len(data.get('data',[]))} entrées)")
            except Exception as e:
                logger.warning(f"Anime-DB cache corrompu: {e}")
                data = None

    # Télécharger si pas de cache valide
    if data is None:
        try:
            logger.log("XREF", "Téléchargement de l'Anime Offline Database…")
            resp = await http_client.get(ANIME_DB_URL)
            resp.raise_for_status()
            data = resp.json()
            os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
            with open(_DB_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f)
            logger.log("XREF", f"Anime-DB téléchargée: {len(data.get('data',[]))} entrées")
        except Exception as e:
            logger.error(f"Anime-DB téléchargement échoué: {e}")
            return False

    # Construire les index
    entries = data.get("data", [])
    _db_by_mal.clear()
    _db_by_anilist.clear()
    _db_by_kitsu.clear()

    for entry in entries:
        entry["_ids"] = _extract_ids(entry.get("sources", []))
        mal = entry["_ids"]["mal_id"]
        ali = entry["_ids"]["anilist_id"]
        kit = entry["_ids"]["kitsu_id"]
        if mal:
            _db_by_mal[mal]     = entry
        if ali:
            _db_by_anilist[ali] = entry
        if kit:
            _db_by_kitsu[kit]   = entry

    _db_loaded = True
    logger.log("XREF", f"Anime-DB indexée: {len(_db_by_mal)} MAL, {len(_db_by_anilist)} AniList, {len(_db_by_kitsu)} Kitsu")
    return True


# ===========================
# Recherche par ID
# ===========================
def get_by_mal(mal_id: int) -> Optional[Dict]:
    return _db_by_mal.get(mal_id)


def get_by_anilist(anilist_id: int) -> Optional[Dict]:
    return _db_by_anilist.get(anilist_id)


def get_by_kitsu(kitsu_id: int) -> Optional[Dict]:
    return _db_by_kitsu.get(kitsu_id)


# ===========================
# Recherche par titre
# ===========================
def search_by_title(query: str, max_results: int = 5) -> List[Dict]:
    """Recherche insensible à la casse dans titres principaux + synonymes."""
    if not _db_loaded:
        return []

    query_lower = query.lower().strip()
    results: List[Dict] = []

    for entry in _db_by_mal.values():
        title_lower = entry.get("title", "").lower()
        if query_lower in title_lower:
            results.append(entry)
            continue
        for syn in entry.get("synonyms", []):
            if query_lower in syn.lower():
                results.append(entry)
                break

    # Trier : titre exact en premier, puis par score
    def sort_key(e):
        exact = 0 if e.get("title", "").lower() == query_lower else 1
        score = -(e.get("score", {}).get("arithmeticMean") or 0)
        return (exact, score)

    results.sort(key=sort_key)
    return results[:max_results]


# ===========================
# Détection split-cour
# ===========================

# Patterns qui signalent qu'une entrée est la suite directe de la précédente
# (même saison TMDB, épisodes consécutifs)
_SPLIT_COUR_PATTERNS = [
    r"part\s*2",
    r"part\s*ii\b",
    r"2(?:nd|ème|e)\s*part",
    r"cour\s*2",
    r"partie\s*2",
    r"second\s*cour",
    r"part\s*two",
    r"后半",   # "second half" en japonais
    r"後半クール",
]

_SPLIT_COUR_RE = re.compile(
    "|".join(_SPLIT_COUR_PATTERNS),
    re.IGNORECASE,
)


def is_split_cour(entry: Dict) -> bool:
    """
    Retourne True si cette entrée est la 2e partie (cour) d'une saison fractionnée.
    Ex. : "Re:Zero 2nd Season Part 2", "Attack on Titan: The Final Season Part 2"
    """
    title = entry.get("title", "")
    synonyms = entry.get("synonyms", [])
    texts = [title] + synonyms

    for text in texts:
        if _SPLIT_COUR_RE.search(text):
            return True
    return False


def get_franchise_tv_sequence(mal_id: int) -> List[Dict]:
    """
    Retourne la séquence chronologique des entrées TV d'une franchise
    à partir de n'importe quel MAL ID de la franchise.

    Utilise relatedAnime pour trouver toutes les entrées liées,
    filtre aux types TV/ONA principaux (exclut SPECIAL, OVA, MOVIE),
    trie par année.

    Ex. pour Re:Zero MAL 31240 :
      → [S1 (25ep), S2P1 (13ep), S2P2 (12ep), S3 (16ep), S4 (19ep)]
    """
    if not _db_loaded:
        return []

    root = _db_by_mal.get(mal_id)
    if not root:
        return []

    # BFS sur relatedAnime pour trouver tout le graphe
    visited: Set[int] = set()
    queue = [mal_id]
    all_entries: List[Dict] = []

    while queue:
        current_mal = queue.pop(0)
        if current_mal in visited:
            continue
        visited.add(current_mal)

        entry = _db_by_mal.get(current_mal)
        if not entry:
            continue

        entry_type = entry.get("type", "")
        if entry_type in ("TV", "ONA"):
            # Exclure les Director's Cut / Re-Edit (même contenu, différent format)
            title = entry.get("title", "").lower()
            if any(k in title for k in ("director's cut", "director", "shin henshu", "re-edit", "new edit", "new version")):
                pass  # On les garde mais on les marque
            all_entries.append(entry)

        # Explorer les voisins
        for rel_url in entry.get("relatedAnime", []):
            if m := re.search(r"myanimelist\.net/anime/(\d+)", rel_url):
                neighbor_id = int(m.group(1))
                if neighbor_id not in visited:
                    queue.append(neighbor_id)

    # Filtrer : garder uniquement les TV principaux, exclure les Director's Cut
    main_entries = []
    for e in all_entries:
        title_low = (e.get("title") or "").lower()
        is_recut = any(k in title_low for k in (
            "director", "shin henshu", "re-edit", "new edit",
            "new version", "shin henshuu", "break time", "petit",
        ))
        if not is_recut:
            main_entries.append(e)

    # Trier par année puis saison
    season_order = {"WINTER": 0, "SPRING": 1, "SUMMER": 2, "FALL": 3, "UNDEFINED": 4}

    def sort_key(e):
        s = e.get("animeSeason", {})
        year = s.get("year") or 9999
        season = season_order.get(s.get("season", "UNDEFINED"), 4)
        return (year, season)

    main_entries.sort(key=sort_key)
    return main_entries


# ===========================
# Construire la table de concordance saisonnière
# (Anime-Sama saison_number → Cinemeta saison+épisode)
# ===========================

def build_season_concordance(
    mal_id: int,
    as_season_episodes: Dict[int, int],
) -> Dict[int, Dict[str, Any]]:
    """
    Construit la table de concordance entre les saisons Anime-Sama
    et les saisons Cinemeta/TMDB.

    Args:
        mal_id: MAL ID de la série principale (S1)
        as_season_episodes: {as_season_number: episode_count}
          ex. {1: 25, 2: 13, 3: 12, 4: 16}

    Returns:
        Concordance par as_season_number:
        {
          1: {"cinemeta_season": 1, "cinemeta_ep_offset": 0,  "mal_id": 31240},
          2: {"cinemeta_season": 2, "cinemeta_ep_offset": 0,  "mal_id": 39587},
          3: {"cinemeta_season": 2, "cinemeta_ep_offset": 13, "mal_id": 42203},  ← split cour
          4: {"cinemeta_season": 3, "cinemeta_ep_offset": 0,  "mal_id": 54857},
        }
    """
    if not _db_loaded or not as_season_episodes:
        return {}

    # 1. Récupérer la séquence TV de la franchise
    franchise = get_franchise_tv_sequence(mal_id)
    if not franchise:
        # Fallback : mapping direct 1:1
        return {
            s: {"cinemeta_season": s, "cinemeta_ep_offset": 0, "mal_id": None}
            for s in as_season_episodes
        }

    # 2. Construire le mapping as_season → entrée db
    #    On associe chaque AS season à la prochaine entrée franchise non déjà utilisée
    as_seasons_sorted = sorted(as_season_episodes.keys())
    concordance: Dict[int, Dict] = {}

    cinemeta_season = 1
    cinemeta_ep_offset = 0

    for i, as_season in enumerate(as_seasons_sorted):
        as_eps = as_season_episodes[as_season]

        # Entrée DB correspondante (si disponible)
        db_entry = franchise[i] if i < len(franchise) else None
        mal_id_for_season = (db_entry["_ids"]["mal_id"] if db_entry else None)

        # Détecter si cette entrée est un split-cour (suite de la précédente)
        if db_entry and is_split_cour(db_entry):
            # Même saison Cinemeta, offset = épisodes de la partie précédente
            # (offset déjà mis à jour depuis l'itération précédente)
            concordance[as_season] = {
                "cinemeta_season":    cinemeta_season,
                "cinemeta_ep_offset": cinemeta_ep_offset,
                "mal_id":             mal_id_for_season,
                "is_split_cour":      True,
            }
            cinemeta_ep_offset += as_eps  # Pour une éventuelle Part 3

            # Fin de cette saison Cinemeta si on n'attend plus de parties
            # (heuristique : vérifier si l'entrée suivante est encore un split)
            next_db = franchise[i + 1] if (i + 1) < len(franchise) else None
            if not (next_db and is_split_cour(next_db)):
                cinemeta_season += 1
                cinemeta_ep_offset = 0
        else:
            concordance[as_season] = {
                "cinemeta_season":    cinemeta_season,
                "cinemeta_ep_offset": 0,
                "mal_id":             mal_id_for_season,
                "is_split_cour":      False,
            }
            cinemeta_ep_offset = as_eps  # Préparer pour un éventuel split suivant

            # Avancer dans les saisons Cinemeta seulement si la suivante n'est pas un split
            next_db = franchise[i + 1] if (i + 1) < len(franchise) else None
            if not (next_db and is_split_cour(next_db)):
                cinemeta_season += 1
                cinemeta_ep_offset = 0

    return concordance
