"""
Adkami Scraper — Port async de generateur_catalogue.py.

Scrape les catalogues Adkami par genre + simulcasts,
enrichit chaque entrée via Jikan (score, image, mal_id),
et sauvegarde les résultats en JSON dans data/catalogues/.

Fichiers générés :
  data/catalogues/simulcast.json
  data/catalogues/final_{genre}.json   (un par genre)
  data/catalogues/cache_jikan.json     (cache Jikan partagé)
"""

import asyncio
import json
import os
import re
import time
from typing import Dict, List, Optional, Any

from bs4 import BeautifulSoup

from astream.utils.http_client import http_client
from astream.utils.logger import logger

# ===========================
# Répertoire des catalogues
# ===========================
CATALOGUE_DIR = os.path.join("data", "catalogues")
CACHE_FILE = os.path.join(CATALOGUE_DIR, "cache_jikan.json")

os.makedirs(CATALOGUE_DIR, exist_ok=True)

# ===========================
# 45 genres Adkami
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

ADKAMI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

# ===========================
# Cache Jikan (JSON sur disque)
# ===========================

def _load_cache() -> Dict[str, Any]:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"ADKAMI: Erreur lecture cache : {e}")
    return {}


def _save_cache(cache: Dict[str, Any]) -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"ADKAMI: Erreur sauvegarde cache : {e}")


# Cache Jikan partagé (chargé une fois au démarrage du module)
_jikan_cache: Dict[str, Any] = _load_cache()
_cache_lock = asyncio.Lock()


# ===========================
# Enrichissement Jikan
# ===========================

async def _jikan_enrich(titre: str, url_adkami: str) -> Dict[str, Any]:
    """
    Cherche un anime sur Jikan et retourne score, members, image_url, mal_id.
    Utilise le cache (clé = url_adkami si dispo, sinon titre).
    """
    global _jikan_cache

    cle = url_adkami if url_adkami else titre

    async with _cache_lock:
        if cle in _jikan_cache:
            return _jikan_cache[cle]
        # Fallback sur le titre seul
        if titre in _jikan_cache:
            data = _jikan_cache[titre]
            _jikan_cache[cle] = data
            return data

    # Appel Jikan
    defaults = {"score_mal": None, "popularite_members": 0, "image_url": "", "mal_id": None}
    try:
        resp = await http_client.get(
            f"https://api.jikan.moe/v4/anime",
            params={"q": titre, "limit": 1},
            headers=ADKAMI_HEADERS,
        )
        if resp.status_code == 200:
            data_list = resp.json().get("data", [])
            if data_list:
                d = data_list[0]
                images = d.get("images", {}).get("jpg", {})
                result = {
                    "score_mal": d.get("score"),
                    "popularite_members": d.get("members", 0),
                    "image_url": images.get("image_url", ""),
                    "mal_id": d.get("mal_id"),
                }
                async with _cache_lock:
                    _jikan_cache[cle] = result
                return result
    except Exception as e:
        logger.warning(f"ADKAMI: Jikan erreur pour '{titre}': {e}")

    async with _cache_lock:
        _jikan_cache[cle] = defaults
    return defaults


# ===========================
# Scraping Adkami par genre
# ===========================

async def _scrape_genre_pages(nom_genre: str, id_genre: int) -> List[Dict[str, Any]]:
    """Scrape toutes les pages d'un genre Adkami."""
    animes: List[Dict[str, Any]] = []
    page = 0

    while True:
        url = (
            f"https://www.adkami.com/video"
            f"?search=&genres%5B%5D={id_genre}&n=&n2=10&t=0&s=&g=&p={page}&order=0&e=&d1=&d2=&q="
        )
        try:
            resp = await http_client.get(url, headers=ADKAMI_HEADERS)
            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all("div", class_="video-item-list")

            if not items:
                break

            for item in items:
                title_tag = item.find("span", class_="title") or item.find("h3")
                if not title_tag:
                    continue
                a_tag = item.find("a")
                url_adkami = a_tag.get("href", "") if a_tag else ""
                animes.append({
                    "titre_affiche": title_tag.text.strip(),
                    "titre_recherche": title_tag.text.strip(),
                    "url_adkami": url_adkami,
                    "source": nom_genre,
                })

            page += 1
            await asyncio.sleep(1.0)

        except Exception as e:
            logger.error(f"ADKAMI [{nom_genre}] page {page}: {e}")
            break

    return animes


async def _enrich_genre_list(animes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enrichit chaque entrée via Jikan (avec rate-limit 1 req/s)."""
    enriched: List[Dict[str, Any]] = []

    for anime in animes:
        titre = anime["titre_recherche"]
        url = anime.get("url_adkami", "")

        jikan_data = await _jikan_enrich(titre, url)
        anime.update(jikan_data)
        enriched.append(anime)
        await asyncio.sleep(1.1)   # Rate limit Jikan

    return enriched


async def scan_genre(nom_genre: str, id_genre: int) -> List[Dict[str, Any]]:
    """
    Scrape + enrichit un genre complet.
    Sauvegarde dans data/catalogues/final_{genre}.json.
    """
    logger.log("ADKAMI", f"Scan genre : {nom_genre}")

    animes = await _scrape_genre_pages(nom_genre, id_genre)
    logger.log("ADKAMI", f"  {len(animes)} entrées brutes pour {nom_genre}")

    animes = await _enrich_genre_list(animes)

    # Tri par popularité décroissante
    animes.sort(key=lambda x: x.get("popularite_members", 0) or 0, reverse=True)

    # Sauvegarde
    path = os.path.join(CATALOGUE_DIR, f"final_{nom_genre.lower()}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(animes, f, ensure_ascii=False, indent=2)

    # Persistance cache
    _save_cache(_jikan_cache)

    logger.log("ADKAMI", f"  ✅ {nom_genre} → {len(animes)} entrées sauvegardées")
    return animes


# ===========================
# Scraping Simulcasts Adkami
# ===========================

async def scan_simulcasts() -> List[Dict[str, Any]]:
    """
    Scrape la page saison Adkami (simulcasts).
    Sauvegarde dans data/catalogues/simulcast.json.
    """
    logger.log("ADKAMI", "Scan simulcasts...")
    animes: List[Dict[str, Any]] = []

    try:
        resp = await http_client.get(
            "https://www.adkami.com/anime/season",
            headers=ADKAMI_HEADERS,
        )
        soup = BeautifulSoup(resp.text, "html.parser")
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

    except Exception as e:
        logger.error(f"ADKAMI: Erreur simulcasts : {e}")

    # Enrichissement Jikan
    for anime in animes:
        jikan_data = await _jikan_enrich(anime["titre_recherche"], "")
        anime.update(jikan_data)
        await asyncio.sleep(1.1)

    path = os.path.join(CATALOGUE_DIR, "simulcast.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(animes, f, ensure_ascii=False, indent=2)

    _save_cache(_jikan_cache)
    logger.log("ADKAMI", f"✅ Simulcasts → {len(animes)} entrées sauvegardées")
    return animes


# ===========================
# Build complet de tous les catalogues
# ===========================

async def build_all_catalogs(force: bool = False) -> None:
    """
    Construit tous les fichiers JSON de catalogue (genres + simulcasts).
    Si force=False, ne reconstruit que les fichiers manquants.
    """
    logger.log("ADKAMI", "═══ Build catalogues Adkami ═══")

    # Simulcasts
    sim_path = os.path.join(CATALOGUE_DIR, "simulcast.json")
    if force or not os.path.exists(sim_path):
        await scan_simulcasts()
    else:
        logger.log("ADKAMI", "  ⏭ simulcast.json déjà présent")

    # Genres
    for nom, id_genre in ADKAMI_GENRES.items():
        genre_path = os.path.join(CATALOGUE_DIR, f"final_{nom.lower()}.json")
        if force or not os.path.exists(genre_path):
            await scan_genre(nom, id_genre)
            await asyncio.sleep(2.0)   # Pause inter-genre
        else:
            logger.log("ADKAMI", f"  ⏭ final_{nom.lower()}.json déjà présent")

    logger.log("ADKAMI", "═══ Build catalogues terminé ═══")


# ===========================
# Lecture des catalogues JSON
# ===========================

def load_genre_catalog(nom_genre: str) -> List[Dict[str, Any]]:
    """Charge le catalogue JSON d'un genre depuis le disque."""
    path = os.path.join(CATALOGUE_DIR, f"final_{nom_genre.lower()}.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"ADKAMI: Erreur lecture {path}: {e}")
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
        logger.error(f"ADKAMI: Erreur lecture simulcast: {e}")
        return []
