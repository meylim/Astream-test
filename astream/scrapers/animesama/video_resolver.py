import re
import asyncio
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin

from astream.utils.logger import logger
from astream.utils.http_client import get_sibnet_headers
from astream.scrapers.base import BaseScraper
from astream.config.settings import settings
from astream.scrapers.animesama.helpers import extract_video_urls_from_text
from astream.utils.filters import filter_excluded_domains


# ===========================
# Classe AnimeSamaVideoResolver
# ===========================
class AnimeSamaVideoResolver(BaseScraper):

    def __init__(self, client):
        super().__init__(client, settings.ANIMESAMA_URL)

    async def extract_video_urls_from_players_with_language(self, player_urls_with_language: List[Dict[str, Any]], config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:

        logger.log("STREAM", f"Visite {len(player_urls_with_language)} players pour extraire URLs vidéo")

        async def extract_from_single_player_with_language(player_data: Dict[str, Any]) -> List[Dict[str, Any]]:

            try:
                player_url = player_data["url"]
                language = player_data["language"]

                if 'sibnet.ru' in player_url:
                    sibnet_url = await self._extract_sibnet_real_url(player_url)
                    if sibnet_url:
                        return [{"url": sibnet_url, "language": language}]
                    else:
                        logger.warning(f"Impossible extraire URL Sibnet depuis {player_url}")
                        return []

                response = await self.client.get(player_url)
                response.raise_for_status()
                player_html = response.text

                found_urls = self._extract_video_urls_from_html(player_html, player_url)

                results = []
                for url in found_urls:
                    results.append({"url": url, "language": language})

                return results

            except Exception as e:
                logger.warning(f"Échec visite {player_data['url']}: {e}")
                return []

        extraction_tasks = [extract_from_single_player_with_language(player_data) for player_data in player_urls_with_language]
        results = await asyncio.gather(*extraction_tasks)

        video_urls_with_language = []
        for urls in results:
            video_urls_with_language.extend(urls)

        seen_urls = set()
        unique_urls_with_language = []

        for item in video_urls_with_language:
            if item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                unique_urls_with_language.append(item)

        filtered_urls_list = filter_excluded_domains([item["url"] for item in unique_urls_with_language], config.get('userExcludedDomains', '') if config else '')
        final_urls_with_language = []

        for item in unique_urls_with_language:
            if item["url"] in filtered_urls_list:
                final_urls_with_language.append(item)

        logger.log("STREAM", f"Extrait {len(final_urls_with_language)} URLs vidéo uniques")
        return final_urls_with_language

    def _extract_video_urls_from_html(self, html: str, player_url: str) -> List[str]:

        video_urls = []

        found_urls = extract_video_urls_from_text(html, player_url)

        for match in found_urls:
            try:
                if match.startswith('http'):
                    video_url = match
                else:
                    video_url = urljoin(player_url, match)

                video_urls.append(video_url)

            except Exception as e:
                logger.debug(f"Erreur traitement URL {match}: {e}")
                continue

        return video_urls

    async def _extract_sibnet_real_url(self, player_url: str) -> Optional[str]:
        try:

            response = await self.client.get(player_url)
            response.raise_for_status()
            html = response.text

            # Plusieurs patterns car Sibnet change parfois sa structure HTML
            sibnet_patterns = [
                r'player\.src\(\[\{src:\s*["\']([^"\'\']+)["\']',
                r'src:\s*["\']([^"\']*\.mp4[^"\']*)["\']',
                r'src:\s*["\']([^"\']*\.m3u8[^"\']*)["\']',
                r'file:\s*["\']([^"\']+)["\']',
                r'"src":\s*"([^"]+)"',
            ]

            redirect_url = None
            for pat in sibnet_patterns:
                m = re.search(pat, html)
                if m:
                    redirect_url = m.group(1)
                    break

            if not redirect_url:
                logger.warning(f"Pattern player.src non trouvé dans {player_url}")
                return None


            if redirect_url.startswith('/'):
                redirect_url = f"https://video.sibnet.ru{redirect_url}"

            headers = get_sibnet_headers(player_url)

            try:
                response = await self.client.get(redirect_url, allow_redirects=False, headers=headers)

                if response.status_code in [301, 302, 303, 307, 308]:
                    real_url = response.headers.get('location')
                    if real_url:
                        if real_url.startswith('//'):
                            real_url = f"https:{real_url}"
                        return real_url
                    else:
                        logger.warning("Header Location manquant réponse Sibnet")
                        return None
                else:
                    logger.warning(f"Réponse Sibnet inattendue: {response.status_code}")
                    return None

            except Exception as redirect_error:
                if "Redirect location:" in str(redirect_error):
                    location_match = re.search(r"Redirect location: '([^']+)'", str(redirect_error))
                    if location_match:
                        real_url = location_match.group(1)
                        if real_url.startswith('//'):
                            real_url = f"https:{real_url}"
                        return real_url
                logger.warning(f"Erreur suivi redirection Sibnet: {redirect_error}")
                return None

        except Exception as e:
            logger.warning(f"Erreur extraction Sibnet: {e}")
            return None
                                        
