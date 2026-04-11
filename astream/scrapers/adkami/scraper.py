"""
Adkami Scraper — Port async de generateur_catalogue.py.

Utilise curl_cffi avec impersonation Chrome (sans proxy AStream) pour
contourner les protections Adkami, exactement comme le script original
utilisait requests avec un User-Agent Chrome.

Fichiers générés dans data/catalogues/ :
  simulcast.json              — saison en cours (rechargé à chaque appel client)
  final_{genre}.json          — un fichier par genre (rechargé à 3h)
  cache_jikan.json            — cache Jikan partagé (score, members, image, mal_id)
"""

import asyncio
import json
import os
import re
import time
from typing import Dict, List, Optional, Any

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from astream.utils.logger import logger

# ===========================
# Répertoire des catalogues
# ===========================
CATALOGUE_DIR = os.path.join("data", "catalogues")
CACHE_FILE = os.path.join(CATALOGUE_DIR, "cache_jikan.json")

os.makedirs(CATALOGUE_DIR, exist_ok=True)

# ===========================
# 45 genres Adkami (identique à generateur_catalogue.py)
# ===========================
ADKAMI_GENRES: Dict[str, int] = {
    "Action": 1,
    "Amitie": 3,
    "Aventure": 2,
    "Combat": 4,
    "Comedie": 5,
    "Contes_et_Recits": 6,
    "Cyber_et_Mecha": 7,
    "Dark_Fantasy": 8,
    "Drame": 9,
    "Ecchi": 10,
    "Educatif": 11,
    "Enigme_et_Policier": 12,
    "Epique_et_Heroique": 13,
    "Espace_et_Sci_Fiction": 14,
    "Familial_et_Jeunesse": 15,
    "Fantastique_et_Mythe": 16,
    "Fantasy": 30,
    "Gastronomie": 39,
    "Gender_Bender": 61,
    "Harem": 32,
    "Historique": 18,
    "Horreur": 19,
    "Idols": 38,
    "Inceste": 36,
    "Magical_Girl": 20,
    "Mature": 26,
    "Moe": 25,
    "Monster_Girl": 71,
    "Musical": 21,
    "Mystere": 31,
    "Psychologique": 22,
    "Romance": 34,
    "School_Life": 29,
    "Sport": 23,
    "Surnaturel": 33,
    "Survival_Game": 40,
    "Thriller": 35,
    "Tokusatsu": 41,
    "Tranche_de_vie": 24,
    "Triangle_Amoureux": 37,
    "Yaoi": 27,
    "Yuri": 28,
    "Gyaru": 70,
    "Isekai": 42,
    "Magie": 43,
}

# Headers identiques au script original
_ADKAMI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

_JIKAN_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    )
}

# Session curl_cffi dédiée Adkami — SANS proxy, avec impersonation Chrome
# (le proxy AStream est configuré sur http_client global, pas ici)
_adkami_session: Optional[AsyncSession] = None
_session_lock = asyncio.Lock()


async def _get_session() -> AsyncSession:
    """Retourne la session curl_cffi dédiée Adkami (création lazy)."""
    global _adkami_session
    if _adkami_session is None:
        async with _session_lock:
            if _adkami_session is None:
                _adkami_session = AsyncSession(impersonate="chrome110", timeout=15)
    return _adkami_session


async def _get(url: str, headers: dict, retries: int = 3, delay: float = 2.0) -> Optional[str]:
    """
    GET HTTP via curl_cffi Chrome impersonation.
    Retry automatique sur 403/429/5xx avec backoff.
    """
    session = await _get_session()
    for attempt in range(retries):
        try:
            resp = await session.get(url, headers=headers)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (403, 429, 503):
                wait = delay * (attempt + 1)
                logger.log("ANIMESAMA", f"ADKAMI: HTTP {resp.status_code} sur {url} — attente {wait:.0f}s")
                await asyncio.sleep(wait)
                continue
            logger.log("ANIMESAMA", f"ADKAMI: HTTP {resp.status_code} sur {url}")
            return None
        except Exception as e:
            wait = delay * (attempt + 1)
            logger.log("ANIMESAMA", f"ADKAMI: Erreur réseau ({e}) — attente {wait:.0f}s")
            await asyncio.sleep(wait)

    logger.log("ANIMESAMA", f"ADKAMI: Échec après {retries} tentatives pour {url}")
    return None


# ===========================
# Cache Jikan (JSON sur disque + mémoire)
# ===========================

def _load_cache() -> Dict[str, Any]:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"ADKAMI: Erreur lecture cache Jikan : {e}")
    return {}


def _save_cache(cache: Dict[str, Any]) -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"ADKAMI: Erreur sauvegarde cache Jikan : {e}")


# Cache en mémoire — chargé une fois au démarrage du module
_jikan_cache: Dict[str, Any] = _load_cache()
_cache_lock = asyncio.Lock()


# ===========================
# Enrichissement Jikan avec cache
# ===========================

async def _jikan_enrich(titre: str, url_adkami: str) -> Dict[str, Any]:
    """
    Cherche l'anime sur Jikan pour obtenir score, members, image_url, mal_id.
    Clé de cache = url_adkami si disponible, sinon titre.
    Rate-limit : appelant doit attendre 1.1s entre appels.
    """
    global _jikan_cache

    cle = url_adkami if url_adkami else titre

    # Vérification cache (url puis titre)
    async with _cache_lock:
        if cle in _jikan_cache:
            return _jikan_cache[cle]
        if titre in _jikan_cache:
            data = _jikan_cache[titre]
            _jikan_cache[cle] = data
            return data

    defaults: Dict[str, Any] = {
        "score_mal": None,
        "popularite_members": 0,
        "image_url": "",
        "mal_id": None,
    }

    try:
        session = await _get_session()
        resp = await session.get(
            "https://api.jikan.moe/v4/anime",
            params={"q": titre, "limit": 1},
            headers=_JIKAN_HEADERS,
            timeout=10,
        )
        if resp.status_code == 200:
            data_list = resp.json().get("data", [])
            if data_list:
                d = data_list[0]
                images = d.get("images", {}).get("jpg", {})
                result: Dict[str, Any] = {
                    "score_mal": d.get("score"),
                    "popularite_members": d.get("members", 0),
                    "image_url": images.get("image_url", ""),
                    "mal_id": d.get("mal_id"),
                }
                async with _cache_lock:
                    _jikan_cache[cle] = result
                return result
        elif resp.status_code == 429:
            logger.log("ANIMESAMA", f"ADKAMI: Jikan rate-limit pour '{titre}', skip")
    except Exception as e:
        logger.log("ANIMESAMA", f"ADKAMI: Jikan erreur pour '{titre}' : {e}")

    async with _cache_lock:
        _jikan_cache[cle] = defaults
    return defaults


# ===========================
# Scraping Adkami — Simulcasts
# ===========================

async def scan_simulcasts() -> List[Dict[str, Any]]:
    """
    Scrape la page saison Adkami et enrichit via Jikan.
    Sauvegarde dans data/catalogues/simulcast.json.
    """
    logger.log("ANIMESAMA", "ADKAMI: Scan simulcasts...")
    animes: List[Dict[str, Any]] = []

    html = await _get("https://www.adkami.com/anime/season", _ADKAMI_HEADERS)
    if not html:
        logger.log("ANIMESAMA", "ADKAMI: Impossible de récupérer la page simulcasts")
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = soup.find_all("div", class_="fiche-info")

    for item in items:
        a_tag = item.find("a")
        if not a_tag:
            continue
        title_tag = a_tag.find("h4")
        if not title_tag:
            continue

        titre = title_tag.text.strip()
        is_suite = bool(re.search(r"saison|season|part", titre.lower()))

        image_url = ""
        img_tag = a_tag.find("img")
        if img_tag and img_tag.get("data-original"):
            image_url = img_tag["data-original"]

        animes.append({
            "titre_affiche": titre,
            "titre_recherche": titre,
            "est_suite": is_suite,
            "image_url": image_url,
            "score_mal": None,
            "popularite_members": 0,
            "mal_id": None,
            "source": "Simulcast",
        })

    logger.log("ANIMESAMA", f"ADKAMI: {len(animes)} simulcasts trouvés — enrichissement Jikan...")

    for anime in animes:
        jikan_data = await _jikan_enrich(anime["titre_recherche"], "")
        # Jikan image prioritaire, sinon garder celle d'Adkami
        if jikan_data.get("image_url"):
            anime["image_url"] = jikan_data["image_url"]
        anime["score_mal"] = jikan_data["score_mal"]
        anime["popularite_members"] = jikan_data["popularite_members"]
        anime["mal_id"] = jikan_data["mal_id"]
        await asyncio.sleep(1.1)   # Rate-limit Jikan

    _save_cache(_jikan_cache)
    _save_json(os.path.join(CATALOGUE_DIR, "simulcast.json"), animes)
    logger.log("ANIMESAMA", f"ADKAMI: ✅ Simulcasts → {len(animes)} entrées sauvegardées")
    return animes


# ===========================
# Scraping Adkami — Genre (paginé)
# ===========================

async def scan_genre(nom_genre: str, id_genre: int) -> List[Dict[str, Any]]:
    """
    Scrape toutes les pages d'un genre Adkami avec pagination.
    Enrichit via Jikan. Sauvegarde dans data/catalogues/final_{genre}.json.
    """
    logger.log("ANIMESAMA", f"ADKAMI: Scan genre '{nom_genre}'...")
    animes_bruts: List[Dict[str, Any]] = []
    page = 0

    # — Étape A : Extraction paginée (identique à generateur_catalogue.py) —
    while True:
        url = (
            f"https://www.adkami.com/video"
            f"?search=&genres%5B%5D={id_genre}&n=&n2=10&t=0"
            f"&s=&g=&p={page}&order=0&e=&d1=&d2=&q="
        )

        html = await _get(url, _ADKAMI_HEADERS)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        items = soup.find_all("div", class_="video-item-list")

        if not items:
            break   # Fin de pagination

        for item in items:
            title_tag = item.find("span", class_="title") or item.find("h3")
            if not title_tag:
                continue
            a_tag = item.find("a")
            url_adkami = a_tag.get("href", "") if a_tag else ""
            animes_bruts.append({
                "titre_affiche": title_tag.text.strip(),
                "titre_recherche": title_tag.text.strip(),
                "url_adkami": url_adkami,
                "source": nom_genre,
            })

        page += 1
        await asyncio.sleep(1.0)   # Politesse Adkami

    logger.log("ANIMESAMA", f"ADKAMI: {len(animes_bruts)} entrées brutes pour '{nom_genre}' — enrichissement Jikan...")

    # — Étape B : Enrichissement Jikan —
    resultats: List[Dict[str, Any]] = []
    for anime in animes_bruts:
        titre = anime["titre_recherche"]
        url_a = anime.get("url_adkami", "")

        # Valeurs par défaut sécurisées
        anime.update({
            "score_mal": None,
            "popularite_members": 0,
            "image_url": "",
            "mal_id": None,
        })

        jikan_data = await _jikan_enrich(titre, url_a)
        anime.update(jikan_data)
        resultats.append(anime)
        await asyncio.sleep(1.1)   # Rate-limit Jikan

    # — Étape C : Tri par popularité décroissante —
    resultats.sort(key=lambda x: x.get("popularite_members", 0) or 0, reverse=True)

    _save_cache(_jikan_cache)
    _save_json(os.path.join(CATALOGUE_DIR, f"final_{nom_genre.lower()}.json"), resultats)
    logger.log("ANIMESAMA", f"ADKAMI: ✅ Genre '{nom_genre}' → {len(resultats)} entrées sauvegardées")
    return resultats


# ===========================
# Build complet de tous les catalogues
# ===========================

async def build_all_catalogs(force: bool = False) -> None:
    """
    Construit tous les fichiers JSON (genres + simulcasts).
    force=False : ne reconstruit que les fichiers manquants.
    force=True  : reconstruit tout (run quotidien à 3h).
    """
    logger.log("ANIMESAMA", "ADKAMI: ═══ Build catalogues Adkami ═══")

    # Simulcasts
    sim_path = os.path.join(CATALOGUE_DIR, "simulcast.json")
    if force or not os.path.exists(sim_path):
        await scan_simulcasts()
    else:
        logger.log("ANIMESAMA", "ADKAMI: ⏭ simulcast.json déjà présent")

    # Genres (séquentiels pour respecter les rate-limits)
    for nom, id_genre in ADKAMI_GENRES.items():
        genre_path = os.path.join(CATALOGUE_DIR, f"final_{nom.lower()}.json")
        if force or not os.path.exists(genre_path):
            await scan_genre(nom, id_genre)
            await asyncio.sleep(2.0)   # Pause entre genres
        else:
            logger.log("ANIMESAMA", f"ADKAMI: ⏭ final_{nom.lower()}.json déjà présent")

    logger.log("ANIMESAMA", "ADKAMI: ═══ Build catalogues terminé ═══")


# ===========================
# Simulcast EN DIRECT (appelé à chaque requête client)
# — 1 seul appel HTTP Adkami, enrichissement depuis cache mémoire uniquement —
# ===========================

async def scan_simulcasts_cached() -> List[Dict[str, Any]]:
    """
    Scrape la page simulcast Adkami EN DIRECT.
    Enrichissement 0-latence : uniquement depuis _jikan_cache en mémoire.
    Les titres inconnus sont retournés avec l'image Adkami + score None.
    Sauvegarde le résultat dans simulcast.json.
    Fallback sur simulcast.json si Adkami inaccessible.
    """
    html = await _get("https://www.adkami.com/anime/season", _ADKAMI_HEADERS)
    if not html:
        logger.log("ANIMESAMA", "ADKAMI: Simulcast live inaccessible — fallback JSON")
        return load_simulcast_catalog()

    soup = BeautifulSoup(html, "html.parser")
    items = soup.find_all("div", class_="fiche-info")

    if not items:
        logger.log("ANIMESAMA", "ADKAMI: Simulcast live vide — fallback JSON")
        return load_simulcast_catalog()

    animes: List[Dict[str, Any]] = []

    for item in items:
        a_tag = item.find("a")
        if not a_tag:
            continue
        title_tag = a_tag.find("h4")
        if not title_tag:
            continue

        titre = title_tag.text.strip()
        is_suite = bool(re.search(r"saison|season|part", titre.lower()))

        # Image depuis la page Adkami (toujours disponible)
        image_adkami = ""
        img_tag = a_tag.find("img")
        if img_tag and img_tag.get("data-original"):
            image_adkami = img_tag["data-original"]

        # Enrichissement depuis le cache mémoire (0 latence, 0 appel Jikan)
        cached = _jikan_cache.get(titre) or {}

        animes.append({
            "titre_affiche":      titre,
            "titre_recherche":    titre,
            "est_suite":          is_suite,
            "image_url":          cached.get("image_url") or image_adkami,
            "score_mal":          cached.get("score_mal"),
            "popularite_members": cached.get("popularite_members", 0),
            "mal_id":             cached.get("mal_id"),
            "source":             "Simulcast",
        })

    # Sauvegarde asynchrone (mise à jour du fichier en arrière-plan)
    _save_json(os.path.join(CATALOGUE_DIR, "simulcast.json"), animes)
    logger.log("ANIMESAMA", f"ADKAMI: Simulcast live → {len(animes)} entrées (cache mémoire)")
    return animes


# ===========================
# Helpers I/O
# ===========================

def _save_json(path: str, data: Any) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"ADKAMI: Erreur sauvegarde {path}: {e}")


def load_genre_catalog(nom_genre: str) -> List[Dict[str, Any]]:
    """Charge le catalogue JSON d'un genre depuis le disque."""
    path = os.path.join(CATALOGUE_DIR, f"final_{nom_genre.lower()}.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"ADKAMI: Erreur lecture {path}: {e}")
        return []


def load_simulcast_catalog() -> List[Dict[str, Any]]:
    """Charge le catalogue simulcast JSON depuis le disque."""
    path = os.path.join(CATALOGUE_DIR, "simulcast.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"ADKAMI: Erreur lecture simulcast: {e}")
        return []
