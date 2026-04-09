from typing import List, Optional, Dict, Any, Set
from urllib.parse import quote
from datetime import datetime
from bs4 import BeautifulSoup

from astream.utils.http_client import HttpClient
from astream.utils.logger import logger
from astream.scrapers.base import BaseScraper
from astream.utils.cache import CacheManager
from astream.config.settings import settings
from astream.scrapers.animesama.card_parser import CardParser
from astream.scrapers.animesama.parser import is_valid_content_type

import asyncio


# ===========================
# Mapping des jours pour les containers HTML de la homepage
# ===========================
DAY_CONTAINERS = {
    0: "containerLundi",
    1: "containerMardi",
    2: "containerMercredi",
    3: "containerJeudi",
    4: "containerVendredi",
    5: "containerSamedi",
    6: "containerDimanche",
}

SCAN_KEYWORDS = {"scan", "manga", "light-novel", "novel", "webtoon"}


def _extract_slugs_from_container(soup: BeautifulSoup, container_id: str) -> List[str]:
    """Extrait les slugs depuis un container HTML de la homepage."""
    container = soup.find("div", id=container_id)
    if not container:
        return []
    slugs = []
    for link in container.find_all("a", href=lambda h: h and "/catalogue/" in h):
        href = link.get("href", "")
        parts = href.split("/catalogue/")
        if len(parts) > 1:
            slug = parts[1].strip("/").split("/")[0]
            if slug and slug not in slugs:
                slugs.append(slug)
    return slugs


def _is_scan_slug(slug: str) -> bool:
    """Retourne True si le slug correspond à un scan/manga/webtoon."""
    if not slug:
        return False
    slug_lower = slug.lower()
    return any(kw in slug_lower for kw in SCAN_KEYWORDS)


# ===========================
# Classe AnimeSamaCatalog
# ===========================
class AnimeSamaCatalog(BaseScraper):

    def __init__(self, client: HttpClient):
        super().__init__(client, settings.ANIMESAMA_URL)

    async def get_homepage_content(self) -> List[Dict[str, Any]]:
        cache_key = "as:homepage"
        lock_key = "lock:homepage"

        async def fetch_homepage():
            logger.debug(f"Cache miss {cache_key} - Scraping homepage")
            response = await self._internal_request('get', f"{self.base_url}/")
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            all_anime = []
            seen_slugs = set()

            new_releases = await self._scrape_new_releases(soup, seen_slugs)
            all_anime.extend(new_releases)

            classics = await self._scrape_classics(soup, seen_slugs)
            all_anime.extend(classics)

            pepites = await self._scrape_pepites(soup, seen_slugs)
            all_anime.extend(pepites)

            # ============================================================
            # Extraire planning + dernières sorties de la homepage.
            # containerSorties = anime récemment mis à jour sur le site
            # containerLundi..Dimanche = planning de diffusion par jour
            # ============================================================
            dernieres_sorties_slugs = _extract_slugs_from_container(soup, "containerSorties")

            planning_by_day = {}
            for day_index, container_id in DAY_CONTAINERS.items():
                day_slugs = _extract_slugs_from_container(soup, container_id)
                if day_slugs:
                    planning_by_day[str(day_index)] = day_slugs

            total_planning = sum(len(v) for v in planning_by_day.values())
            logger.log("ANIMESAMA", f"Homepage: {len(all_anime)} anime, {len(dernieres_sorties_slugs)} dernières sorties, planning {total_planning} sur {len(planning_by_day)} jours")

            if not all_anime:
                logger.log("DATABASE", "Aucun anime trouvé sur homepage - pas de cache")
                return None

            return {
                "anime": all_anime,
                "total": len(all_anime),
                "dernieres_sorties_slugs": dernieres_sorties_slugs,
                "planning_by_day": planning_by_day,
            }

        try:
            cached_data = await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch_homepage,
                lock_key=lock_key,
                ttl=settings.DYNAMIC_LIST_TTL
            )

            return cached_data.get("anime", []) if cached_data else []

        except Exception as e:
            logger.error(f"Échec récupération homepage: {e}")
            return []

    async def get_homepage_raw(self) -> Optional[Dict[str, Any]]:
        """Retourne les données brutes de la homepage (incluant planning + dernières sorties)."""
        cache_key = "as:homepage"
        cached_data = await CacheManager.get(cache_key)
        return cached_data

    async def get_today_releases(self) -> List[str]:
        """
        VRAIS sorties du jour = intersection(planning du jour, dernières sorties).
        Garantit qu'on ne montre que les anime RÉELLEMENT mis à jour aujourd'hui.
        """
        raw = await self.get_homepage_raw()
        if not raw:
            return []

        today_weekday = datetime.now().weekday()
        planning_by_day = raw.get("planning_by_day", {})
        dernieres_sorties = set(raw.get("dernieres_sorties_slugs", []))

        today_planned = planning_by_day.get(str(today_weekday), [])

        real_releases = [
            slug for slug in today_planned
            if slug in dernieres_sorties and not _is_scan_slug(slug)
        ]

        logger.log(
            "ANIMESAMA",
            f"Sorties du jour (weekday={today_weekday}): "
            f"{len(today_planned)} planifiés, {len(dernieres_sorties)} dernières sorties, "
            f"{len(real_releases)} réelles"
        )

        return real_releases

    async def get_planning_slugs(self) -> Set[str]:
        """Tous les slugs du planning (tous jours). Utilisé pour 'en cours'."""
        raw = await self.get_homepage_raw()
        if not raw:
            return set()

        planning_by_day = raw.get("planning_by_day", {})
        all_slugs = set()
        for day_slugs in planning_by_day.values():
            all_slugs.update(day_slugs)

        return {s for s in all_slugs if not _is_scan_slug(s)}

    async def search_anime(self, query: str, language: Optional[str] = None, genre: Optional[str] = None) -> List[Dict[str, Any]]:
        cache_key = f"as:search:{query}"
        lock_key = f"lock:search:{query}"

        async def fetch_search_results():
            logger.log("DATABASE", f"Cache miss {cache_key} - Recherche live")
            all_results = []

            types_to_search = ["Anime", "Film", "Autres"]

            async def search_one_type(content_type):
                try:
                    search_url = f"{self.base_url}/catalogue/?search={quote(query)}"
                    if language and language in ["VOSTFR", "VF"]:
                        search_url += f"&langue[]={language}"
                    if genre:
                        search_url += f"&genre[]={quote(genre)}"
                    search_url += f"&type[]={content_type}"

                    logger.debug(f"Recherche {content_type.lower()}: {search_url}")
                    response = await self._internal_request('get', search_url)
                    response.raise_for_status()

                    soup = BeautifulSoup(response.text, 'html.parser')
                    anime_cards = soup.find_all('a', href=lambda x: x and '/catalogue/' in x)
                    results = []
                    for card in anime_cards:
                        anime_data = CardParser.parse_anime_card(card)
                        if anime_data:
                            results.append(anime_data)
                    return results
                except Exception as e:
                    logger.warning(f"Erreur recherche {content_type}: {e}")
                    return []

            type_results = await asyncio.gather(
                *[search_one_type(ct) for ct in types_to_search],
                return_exceptions=True
            )

            for result in type_results:
                if isinstance(result, Exception):
                    logger.warning(f"Erreur recherche type: {result}")
                    continue
                all_results.extend(result)

            logger.log("ANIMESAMA", f"Trouvé {len(all_results)} résultats pour '{query}'")

            if not all_results:
                logger.log("DATABASE", f"Cache set {cache_key} - 0 résultats (cache négatif)")
                return {"results": [], "query": query, "total_found": 0, "empty": True}

            cache_data = {"results": all_results, "query": query, "total_found": len(all_results)}
            logger.log("DATABASE", f"Cache set {cache_key} - {len(all_results)} résultats")
            return cache_data

        try:
            cached_data = await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch_search_results,
                lock_key=lock_key,
                ttl=settings.DYNAMIC_LIST_TTL
            )

            return cached_data.get("results", []) if cached_data else []

        except Exception as e:
            logger.error(f"Échec recherche anime: {e}")
            return []

    async def _scrape_container(self, soup: BeautifulSoup, container_id: str, parser_method, seen_slugs: set, section_name: str) -> List[Dict[str, Any]]:
        try:
            anime = []
            container = soup.find('div', id=container_id)
            if not container:
                return []

            anime_cards = container.find_all('div', class_='shrink-0')

            for card in anime_cards:
                link = card.find('a', href=lambda x: x and '/catalogue/' in x)
                if not link:
                    continue

                anime_data = parser_method(link)
                if anime_data and is_valid_content_type(anime_data.get('type', '')) and anime_data['slug'] not in seen_slugs:
                    seen_slugs.add(anime_data['slug'])
                    anime.append(anime_data)

            return anime

        except Exception as e:
            logger.warning(f"Erreur scraping {section_name}: {e}")
            return []

    async def _scrape_new_releases(self, soup: BeautifulSoup, seen_slugs: set) -> List[Dict[str, Any]]:
        return await self._scrape_container(soup, 'containerSorties', CardParser.parse_anime_card, seen_slugs, 'nouveaux contenus')

    async def _scrape_classics(self, soup: BeautifulSoup, seen_slugs: set) -> List[Dict[str, Any]]:
        return await self._scrape_container(soup, 'containerClassiques', CardParser.parse_anime_card, seen_slugs, 'classiques')

    async def _scrape_pepites(self, soup: BeautifulSoup, seen_slugs: set) -> List[Dict[str, Any]]:
        return await self._scrape_container(soup, 'containerPepites', CardParser.parse_pepites_card, seen_slugs, 'pépites')
        
