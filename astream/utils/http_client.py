from curl_cffi.requests import AsyncSession
import asyncio
import time
import random
import json
import re

from astream.config.settings import settings
from astream.utils.logger import logger


USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
]


# ===========================
# Gestion des User Agents
# ===========================
def get_random_user_agent():
    return random.choice(USER_AGENT_POOL)


# ===========================
# En-têtes par défaut
# ===========================
def get_default_headers():
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    }


# ===========================
# En-têtes Sibnet
# ===========================
def get_sibnet_headers(referer_url):
    return {
        "User-Agent": get_random_user_agent(),
        "Referer": referer_url,
        "Accept": "*/*",
        "Range": "bytes=0-",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    }


# ===========================
# Fonction d'aide JSON
# ===========================
def safe_json_decode(response, context: str = "", default=None):
    try:
        return response.json()
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Erreur décodage JSON{' ' + context if context else ''}: {e}")
        return default


# ===========================
# Classe CurlResponse
# ===========================
class CurlResponse:

    def __init__(self, response):
        self._response = response
        self.status_code = response.status_code
        self.headers = response.headers
        self.content = response.content
        self.text = response.text
        self.url = str(response.url)

    def json(self):
        try:
            return self._response.json()
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Erreur décodage JSON pour {self.url}: {e}")
            raise

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            raise CurlHTTPStatusError(f"HTTP {self.status_code}", response=self)


# ===========================
# Classe CurlHTTPStatusError
# ===========================
class CurlHTTPStatusError(Exception):
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response


# ===========================
# Classe CurlTimeoutException
# ===========================
class CurlTimeoutException(Exception):
    pass


# ===========================
# Classe BaseClient
# ===========================
class BaseClient:

    def __init__(self):
        self.client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        if hasattr(self, 'client') and self.client:
            await self.client.close()
            self.client = None


# ===========================
# Classe HttpClient
# ===========================
class HttpClient(BaseClient):

    def __init__(self, base_url: str = "", timeout: float = None, retries: int = 3):
        if timeout is None:
            timeout = settings.HTTP_TIMEOUT
        super().__init__()
        self.base_url = base_url
        self.timeout = timeout
        self.retries = retries
        self.client = None
        self._setup_clients()

    def _setup_clients(self):
        headers = get_default_headers()

        if settings.PROXY_URL:
            self.client = AsyncSession(
                timeout=self.timeout,
                headers=headers,
                impersonate="chrome120",
                proxies={"http": settings.PROXY_URL, "https": settings.PROXY_URL}
            )
            logger.log("PROXY", "Configuration du proxy activée")
        else:
            self.client = AsyncSession(
                timeout=self.timeout,
                headers=headers,
                impersonate="chrome120"
            )

    @property
    def is_closed(self) -> bool:
        return self.client is None

    async def get(self, url: str, **kwargs) -> CurlResponse:
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs) -> CurlResponse:
        return await self._request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs) -> CurlResponse:
        return await self._request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs) -> CurlResponse:
        return await self._request("DELETE", url, **kwargs)

    async def _request(self, method: str, url: str, **kwargs) -> CurlResponse:
        """
        Effectue une requête HTTP avec retry automatique, backoff exponentiel et gestion proxy.
        Normalise les URLs (vidmoly->moly), retente jusqu'à self.retries fois avec délai croissant,
        gère les timeouts et erreurs 5xx.
        """
        if not url.startswith('http'):
            url = f"{self.base_url.rstrip('/')}/{url.lstrip('/')}"
        # Normalisation de l'URL vidmoly -> moly pour compatibilité
        if "vidmoly.to" in url.lower():
            url = re.sub(r'vidmoly\.to', 'moly.to', url, flags=re.IGNORECASE)

        last_exception = None

        # Système de retry avec backoff exponentiel
        for attempt in range(self.retries):
            try:
                if self.is_closed:
                    self._setup_clients()

                if attempt > 0:
                    logger.debug(f"Retry {attempt}/{self.retries}: {method} {url}")

                t0 = time.monotonic()
                response = await self.client.request(method, url, **kwargs)
                elapsed = round((time.monotonic() - t0) * 1000)

                wrapped_response = CurlResponse(response)
                wrapped_response.raise_for_status()

                logger.log("PERFORMANCE", f"HTTP {method} {url[:100]} → {response.status_code} {elapsed}ms")
                return wrapped_response

            except asyncio.TimeoutError:
                last_exception = CurlTimeoutException(f"{method} {url} timeout")
                logger.warning(f"{method} {url} timeout (tentative {attempt + 1}/{self.retries})")
                if attempt < self.retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))

            except CurlHTTPStatusError as e:
                last_exception = e
                if e.response.status_code >= 500:
                    logger.warning(f"{method} {url} → {e.response.status_code} (tentative {attempt + 1}/{self.retries})")
                    if attempt < self.retries - 1:
                        await asyncio.sleep(1 * (attempt + 1))
                        continue
                else:
                    logger.error(f"{method} {url} → {e.response.status_code}")
                    raise

            except Exception as e:
                last_exception = e
                logger.error(f"{method} {url} erreur: {str(e)} (tentative {attempt + 1}/{self.retries})")
                if attempt < self.retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))

        logger.error(f"{method} {url} a échoué après {self.retries} tentatives")
        if last_exception:
            raise last_exception
        else:
            raise Exception(f"La requête a échoué après {self.retries} tentatives")


# ===========================
# Instance Singleton Globale
# ===========================
http_client = HttpClient()
            
