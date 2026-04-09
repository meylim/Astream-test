import re
import asyncio
from datetime import datetime
from typing import Set, Dict, List, Optional
from bs4 import BeautifulSoup
from astream.utils.logger import logger
from astream.scrapers.base import BaseScraper
from astream.utils.cache import CacheManager, CacheKeys
from astream.config.settings import settings

# Mapping jours FR/EN → index Python (0=lundi)
DAYS_FR = {
    "lundi": 0, "mardi": 1, "mercredi": 2, "jeudi": 3,
    "vendredi": 4, "samedi": 5, "dimanche": 6,
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


SCAN_KEYWORDS = {"scan", "manga", "light-novel", "novel", "webtoon"}


def _clean_slug(raw: str) -> str:
    """Retourne uniquement le premier segment du slug (avant tout slash)."""
    if not raw:
        return ""
    raw = raw.lstrip("/")
    return raw.split("/")[0].strip()


def _is_scan_path(raw: str) -> bool:
    """Retourne True si le chemin contient un segment scan/manga/novel/webtoon."""
    if not raw:
        return False
    segments = raw.lower().lstrip("/").split("/")
    return any(seg in SCAN_KEYWORDS for seg in segments)


# ===========================
# Classe AnimeSamaPlanning
# ===========================
class AnimeSamaPlanning(BaseScraper):

    def __init__(self, client):
        super().__init__(client, settings.ANIMESAMA_URL)
        self.planning_url = f"{settings.ANIMESAMA_URL}/planning/"

    async def get_current_planning_anime(self) -> Set[str]:

        cache_key = CacheKeys.planning()
        lock_key = "lock:planning"

        async def fetch_planning():
            logger.log("ANIMESAMA", "Scraping du planning en cours")
            response = await self._internal_request('get', self.planning_url)
            if not response:
                logger.warning("Impossible de récupérer le planning")
                return None

            anime_slugs = self._extract_anime_slugs_from_planning(response.text)

            if not anime_slugs:
                logger.log("DATABASE", "Planning vide après extraction - pas de cache")
                return None

            planning_data = {"anime_slugs": list(anime_slugs)}
            logger.log("ANIMESAMA", f"Planning mis à jour: {len(anime_slugs)} anime actifs")
            return planning_data

        try:
            cached_planning = await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch_planning,
                lock_key=lock_key,
                ttl=settings.PLANNING_TTL
            )

            if cached_planning:
                logger.log("PERFORMANCE", "Planning récupéré depuis le cache")
                return set(cached_planning.get("anime_slugs", []))

            return set()

        except Exception as e:
            logger.error(f"Erreur scraping planning: {e}")
            return set()

    def _extract_anime_slugs_from_planning(self, html_content: str) -> Set[str]:

        anime_slugs = set()

        try:
            pattern = r'anime-card[^"]*planning-card"[^>]*>[\s\S]*?href="/catalogue/([^/"]+)'
            matches = re.findall(pattern, html_content)

            # Récupérer aussi le contexte pour filtrer les scans
            scan_pattern = r'href="/catalogue/[^"]*?/scan/'
            scan_hrefs = set(re.findall(r'href="/catalogue/([^/"]+)', 
                ''.join(re.findall(r'href="/catalogue/[^"]*scan[^"]*"', html_content))))

            for slug in matches:
                if slug and slug not in scan_hrefs:
                    anime_slugs.add(slug)

            logger.debug(f"Slugs planning extraits: {sorted(anime_slugs)}")

        except Exception as e:
            logger.error(f"Erreur extraction slugs planning: {e}")

        return anime_slugs

    async def get_planning_by_day(self) -> Dict[int, List[str]]:
        """
        Retourne un dict {jour_index: [slugs]} où jour_index suit la convention Python
        (0=lundi, 6=dimanche). Utilisé pour savoir quels anime diffusent quel jour.
        """
        cache_key = "as:planning:by_day"
        lock_key = "lock:planning:by_day"

        async def fetch_by_day():
            logger.log("ANIMESAMA", "Scraping planning par jour")
            response = await self._internal_request('get', self.planning_url)
            if not response:
                return None

            result = self._extract_planning_by_day(response.text)
            if not result:
                return None

            total = sum(len(v) for v in result.values())
            logger.log("ANIMESAMA", f"Planning par jour: {total} anime répartis sur {len(result)} jours")
            return {"by_day": {str(k): v for k, v in result.items()}}

        try:
            cached = await CacheManager.get_or_fetch(
                cache_key=cache_key,
                fetch_func=fetch_by_day,
                lock_key=lock_key,
                ttl=settings.PLANNING_TTL
            )
            if cached:
                return {int(k): v for k, v in cached.get("by_day", {}).items()}
            return {}
        except Exception as e:
            logger.error(f"Erreur planning par jour: {e}")
            return {}

    async def get_today_anime(self) -> List[str]:
        """Retourne les slugs des anime qui diffusent aujourd'hui."""
        today = datetime.now().weekday()  # 0=lundi
        by_day = await self.get_planning_by_day()
        raw_slugs = by_day.get(today, [])
        # Sanitiser + filtrer les scans résiduels (slugs contenant "scan")
        slugs = [
            _clean_slug(s) for s in raw_slugs
            if _clean_slug(s) and 'scan' not in _clean_slug(s).lower()
        ]
        logger.log("ANIMESAMA", f"Anime du jour (weekday={today}): {len(slugs)} trouvés (scans filtrés)")
        return slugs

    def _extract_planning_by_day(self, html_content: str) -> Dict[int, List[str]]:
        """
        Parse la page planning d'Anime-Sama et retourne un dict {day_index: [slugs]}.
        Gère deux structures HTML possibles :
        - Sections avec titre jour : <h2>Lundi</h2> ... cartes ...
        - Attributs data : <div data-day="lundi">
        """
        result: Dict[int, List[str]] = {}
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            current_day: Optional[int] = None

            for element in soup.find_all(True):
                tag = element.name
                classes = element.get('class', [])
                text = element.get_text(strip=True).lower()

                # Détection d'un titre de jour (h2, h3, div.day-title, etc.)
                is_day_header = (
                    tag in ('h2', 'h3', 'h4') or
                    any(c in ' '.join(classes) for c in ['day', 'jour', 'weekday', 'planning-day'])
                )

                if is_day_header and text in DAYS_FR:
                    current_day = DAYS_FR[text]
                    if current_day not in result:
                        result[current_day] = []
                    continue

                # Attribut data-day
                data_day = element.get('data-day', '').lower()
                if data_day in DAYS_FR:
                    current_day = DAYS_FR[data_day]
                    if current_day not in result:
                        result[current_day] = []

                # Carte planning avec lien /catalogue/
                if 'planning-card' in ' '.join(classes):
                    # Ignorer les scans/mangas
                    card_text = element.get_text().lower()
                    if 'scan' in card_text and 'saison' not in card_text:
                        continue

                    link = element.find('a', href=lambda h: h and '/catalogue/' in h)
                    if not link:
                        link = element if element.name == 'a' and element.get('href', '') else None
                    if link:
                        href = link.get('href', '')
                        # Ignorer les URLs qui contiennent /scan/ (scans de manga)
                        if '/scan/' in href.lower():
                            continue
                        raw_path = href.split('/catalogue/')[-1]
                        if _is_scan_path(raw_path):
                            continue
                        slug = _clean_slug(raw_path)
                        if slug and current_day is not None:
                            if current_day not in result:
                                result[current_day] = []
                            if slug not in result[current_day]:
                                result[current_day].append(slug)

            # Fallback: si aucun jour détecté, essayer une approche plus large
            if not result:
                result = self._extract_planning_by_day_fallback(soup)

        except Exception as e:
            logger.error(f"Erreur parsing planning par jour: {e}")

        return result

    def _extract_planning_by_day_fallback(self, soup: BeautifulSoup) -> Dict[int, List[str]]:
        """
        Fallback: cherche tous les textes de jour dans la page et les cartes qui suivent.
        """
        result: Dict[int, List[str]] = {}
        all_text_nodes = soup.find_all(string=re.compile(
            r'^(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)$', re.IGNORECASE
        ))

        for text_node in all_text_nodes:
            day_text = text_node.strip().lower()
            if day_text not in DAYS_FR:
                continue
            day_idx = DAYS_FR[day_text]
            result[day_idx] = []

            # Chercher les cartes dans les siblings suivants
            parent = text_node.parent
            if parent:
                next_sib = parent.find_next_sibling()
                while next_sib:
                    links = next_sib.find_all('a', href=lambda h: h and '/catalogue/' in h)
                    for link in links:
                        href = link.get('href', '')
                        raw_path = href.split('/catalogue/')[-1]
                        if _is_scan_path(raw_path):
                            continue
                        slug = _clean_slug(raw_path)
                        if slug:
                            result[day_idx].append(slug)
                    # Arrêter si on trouve un autre jour
                    if next_sib.get_text(strip=True).lower() in DAYS_FR:
                        break
                    next_sib = next_sib.find_next_sibling()

        return result

    async def is_anime_ongoing(self, anime_slug: str) -> bool:

        current_planning = await self.get_current_planning_anime()

        is_ongoing = (
            anime_slug in current_planning or
            any(slug.startswith(anime_slug) for slug in current_planning) or
            any(anime_slug.startswith(slug) for slug in current_planning)
        )

        return is_ongoing


# ===========================
# Vérificateur de planning global
# ===========================
_planning_checker = None
_planning_checker_lock = asyncio.Lock()


# ===========================
# Fonctions d'aide
# ===========================
async def get_planning_checker():
    """
    Récupère ou initialise le planning checker avec protection contre les race conditions.
    """
    global _planning_checker
    async with _planning_checker_lock:
        if _planning_checker is None:
            from astream.scrapers.animesama.client import animesama_api
            _planning_checker = AnimeSamaPlanning(animesama_api.client)
        return _planning_checker


async def is_anime_ongoing(anime_slug: str) -> bool:

    checker = await get_planning_checker()
    return await checker.is_anime_ongoing(anime_slug)


async def get_today_anime_slugs() -> List[str]:
    """Retourne les slugs des anime qui diffusent aujourd'hui."""
    checker = await get_planning_checker()
    return await checker.get_today_anime()


async def get_planning_by_day() -> Dict[int, List[str]]:
    """Retourne le planning complet groupé par jour."""
    checker = await get_planning_checker()
    return await checker.get_planning_by_day()


async def get_smart_cache_ttl(anime_slug: str) -> int:
    try:
        if await is_anime_ongoing(anime_slug):
            ttl = settings.ONGOING_ANIME_TTL
            logger.log("PERFORMANCE", f"TTL anime EN COURS '{anime_slug}': {ttl}s")
        else:
            ttl = settings.FINISHED_ANIME_TTL
            logger.log("PERFORMANCE", f"TTL anime TERMINÉ '{anime_slug}': {ttl}s")

        return ttl

    except Exception as e:
        logger.warning(f"Erreur calcul TTL '{anime_slug}': {e}")
        return settings.ONGOING_ANIME_TTL
        
