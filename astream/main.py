"""
main.py — Version modifiée avec pré-chauffage et scheduler journalier.

Changements par rapport à l'original :
  1. lifespan() appelle warmup_startup_caches() APRÈS l'init DB/dataset
  2. Lance 3 tâches background :
     - cleanup_expired_locks (existant)
     - daily_scheduler_task (NOUVEAU — rafraîchit à minuit Paris)
     - periodic_refresh_task (NOUVEAU — anticipe les expirations TTL)
"""

import asyncio
import os
import signal
import sys
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator, Any, Optional, Dict
from types import FrameType

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from astream.api.routes import main as router
from astream.config.settings import settings
from astream.utils.database import (
    setup_database,
    teardown_database,
    cleanup_expired_locks,
)
from astream.utils.http_client import http_client
from astream.utils.logger import logger
from astream.utils.error_handler import global_exception_handler
from astream.utils.data_loader import DatasetLoader, set_dataset_loader


# ===========================
# Classe LoguruMiddleware
# ===========================
class LoguruMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.time()
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception as e:
            logger.error(f"Exception durant le traitement de la requête: {e}")
            raise
        finally:
            process_time = time.time() - start_time
            log_level = "WARNING" if status_code >= 400 else "API"
            logger.log(log_level, f"{request.method} {request.url.path} [{status_code}] {process_time:.3f}s")


# ===========================
# Gestionnaire de cycle de vie
# ===========================
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await setup_database()

    try:
        logger.log("ASTREAM", "Client HTTP singleton initialisé")

        if settings.DATASET_ENABLED:
            dataset_loader = DatasetLoader(http_client)
            await dataset_loader.initialize()
            set_dataset_loader(dataset_loader)
            logger.log("ASTREAM", "Dataset loader initialisé")
        else:
            logger.log("ASTREAM", "Dataset désactivé")

        # ============================================================
        # NOUVEAU : Pré-chauffage des caches stables AVANT d'accepter
        # les connexions. Le serveur ne sera "healthy" qu'après.
        # ============================================================
        from astream.utils.scheduler import warmup_startup_caches
        await warmup_startup_caches()

        logger.log("ASTREAM", "Initialisation terminée - Prêt à scraper Anime-Sama")

    except Exception as e:
        logger.error(f"Échec de l'initialisation : {e}")
        raise RuntimeError(f"L'initialisation a échoué : {e}")

    # Tâches background
    cleanup_task = asyncio.create_task(cleanup_expired_locks())

    # ============================================================
    # NOUVEAU : Scheduler journalier (minuit Paris) + refresh périodique
    # ============================================================
    from astream.utils.scheduler import daily_scheduler_task, periodic_refresh_task
    daily_task = asyncio.create_task(daily_scheduler_task())
    periodic_task = asyncio.create_task(periodic_refresh_task())

    try:
        yield
    finally:
        # Annuler toutes les tâches background proprement
        for task in [cleanup_task, daily_task, periodic_task]:
            task.cancel()

        try:
            await asyncio.gather(
                cleanup_task, daily_task, periodic_task,
                return_exceptions=True
            )
        except asyncio.CancelledError:
            pass

        await http_client.close()
        await teardown_database()
        logger.log("ASTREAM", "Ressources nettoyées - Arrêt propre")


# ===========================
# Instance de l'application FastAPI
# ===========================
app = FastAPI(
    title=settings.ADDON_NAME,
    lifespan=lifespan,
    redoc_url=None,
)

app.add_middleware(LoguruMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_exception_handler(Exception, global_exception_handler)

static_dir = "astream/public"
if os.path.exists(static_dir) and os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
else:
    logger.warning(f"Répertoire statique manquant: {static_dir}")

app.include_router(router)


# ===========================
# Classe Server
# ===========================
class Server(uvicorn.Server):

    def install_signal_handlers(self) -> None:
        pass

    @contextmanager
    def run_in_thread(self) -> Generator[None, None, None]:
        thread = threading.Thread(target=self.run, name="AStream")
        thread.start()
        try:
            while not self.started:
                time.sleep(1e-3)
            yield
        except Exception as e:
            logger.error(f"Erreur dans le thread du serveur: {e}")
            raise e
        finally:
            self.should_exit = True


# ===========================
# Gestionnaire de signaux
# ===========================
def signal_handler(sig: int, frame: Optional[FrameType]) -> None:
    logger.log("ASTREAM", "Arret en cours...")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ===========================
# Affichage des logs de démarrage
# ===========================
def start_log() -> None:
    db_info = settings.DATABASE_PATH if settings.DATABASE_TYPE == 'sqlite' else 'PostgreSQL'
    logger.log("ASTREAM", f"Serveur: http://{settings.FASTAPI_HOST}:{settings.FASTAPI_PORT} ({settings.FASTAPI_WORKERS} workers)")
    logger.log("ASTREAM", f"Base: {settings.DATABASE_TYPE} ({db_info}) - TTL: {settings.EPISODE_TTL}s")
    update_status = 'OFF' if settings.DATASET_UPDATE_INTERVAL <= 0 else f'{settings.DATASET_UPDATE_INTERVAL}s'
    logger.log("ASTREAM", f"Dataset: {'ON' if settings.DATASET_ENABLED else 'OFF'} - Update: {update_status}")


# ===========================
# Exécution avec Uvicorn
# ===========================
def run_with_uvicorn() -> None:
    config = uvicorn.Config(
        app,
        host=settings.FASTAPI_HOST,
        port=settings.FASTAPI_PORT,
        proxy_headers=True,
        forwarded_allow_ips="*",
        workers=settings.FASTAPI_WORKERS,
        log_config=None,
    )
    server = Server(config=config)

    with server.run_in_thread():
        start_log()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.log("ASTREAM", "Arrêt manuel du serveur")
        except Exception as e:
            logger.error(f"Erreur inattendue: {e}")
        finally:
            logger.log("ASTREAM", "Serveur arrêté")


# ===========================
# Exécution avec Gunicorn
# ===========================
def run_with_gunicorn() -> None:
    import gunicorn.app.base

    # ===========================
    # Classe StandaloneApplication
    # ===========================
    class StandaloneApplication(gunicorn.app.base.BaseApplication):

        def __init__(self, app: Any, options: Optional[Dict[str, Any]] = None) -> None:
            self.options = options or {}
            self.application = app
            super().__init__()

        def load_config(self) -> None:
            config = {
                key: value
                for key, value in self.options.items()
                if key in self.cfg.settings and value is not None
            }
            for key, value in config.items():
                self.cfg.set(key.lower(), value)

        def load(self) -> Any:
            return self.application

    workers = settings.FASTAPI_WORKERS
    if workers < 1:
        workers = min((os.cpu_count() or 1) * 2 + 1, 12)

    options = {
        "bind": f"{settings.FASTAPI_HOST}:{settings.FASTAPI_PORT}",
        "workers": workers,
        "worker_class": "uvicorn.workers.UvicornWorker",
        "timeout": 120,
        "keepalive": 5,
        "preload_app": True,
        "proxy_protocol": True,
        "forwarded_allow_ips": "*",
        "loglevel": "warning",
    }

    start_log()
    logger.log("ASTREAM", f"Démarrage Gunicorn avec {workers} workers")

    StandaloneApplication(app, options).run()


if __name__ == "__main__":
    if os.name == "nt" or not settings.USE_GUNICORN:
        run_with_uvicorn()
    else:
        run_with_gunicorn()
