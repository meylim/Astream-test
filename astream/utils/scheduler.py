"""
Scheduler de pré-chauffage et rafraîchissement automatique des caches.

v2: Ajout du pré-chauffage TMDB au démarrage + à minuit.

Stratégie :
  AU DÉMARRAGE → homepage + planning + TMDB enrichissement
  À MINUIT PARIS → invalide + recharge planning_by_day + homepage + TMDB
  TOUTES LES HEURES → anticipe expiration TTL homepage + planning
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from astream.utils.logger import logger
from astream.config.settings import settings

# Timezone Paris
PARIS_UTC_OFFSET_WINTER = 1  # CET
PARIS_UTC_OFFSET_SUMMER = 2  # CEST


def _get_paris_now() -> datetime:
    """Retourne l'heure actuelle à Paris (gestion simplifiée DST)."""
    utc_now = datetime.now(timezone.utc)
    year = utc_now.year
    march_last = datetime(year, 3, 31, tzinfo=timezone.utc)
    dst_start = march_last - timedelta(days=(march_last.weekday() + 1) % 7)
    dst_start = dst_start.replace(hour=1)
    oct_last = datetime(year, 10, 31, tzinfo=timezone.utc)
    dst_end = oct_last - timedelta(days=(oct_last.weekday() + 1) % 7)
    dst_end = dst_end.replace(hour=1)

    if dst_start <= utc_now < dst_end:
        offset = PARIS_UTC_OFFSET_SUMMER
    else:
        offset = PARIS_UTC_OFFSET_WINTER

    return utc_now + timedelta(hours=offset)


def _seconds_until_midnight_paris() -> float:
    """Calcule le nombre de secondes jusqu'à minuit Paris."""
    paris_now = _get_paris_now()
    tomorrow = (paris_now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    delta = tomorrow - paris_now
    return max(delta.total_seconds(), 60)


# ===========================
# Pré-chauffage TMDB
# ===========================
async def _warmup_tmdb(anime_list: list) -> None:
    """
    Pré-charge le cache TMDB pour tous les anime de la homepage.
    Utilise la clé API serveur (settings.TMDB_API_KEY).
    """
    if not settings.TMDB_API_KEY:
        logger.log("ASTREAM", "  ⏭ TMDB : pas de TMDB_API_KEY serveur, skip pré-chauffage")
        return

    if not anime_list:
        return

    try:
        from astream.utils.validators import ConfigModel
        from astream.services.catalog import catalog_service

        # Config minimale avec la clé serveur
        warmup_config = ConfigModel(
            tmdbApiKey=settings.TMDB_API_KEY,
            tmdbEnabled=True,
        )

        logger.log("ASTREAM", f"  ⏳ TMDB : enrichissement de {len(anime_list)} anime...")

        # Le sémaphore dans catalog_service limite à 5 parallèles
        enhanced = await catalog_service._enrich_catalog_with_tmdb(anime_list, warmup_config)

        enriched = sum(1 for a in enhanced if a.get("poster"))
        logger.log("ASTREAM", f"  ✓ TMDB : {enriched}/{len(anime_list)} anime enrichis en cache")

    except Exception as e:
        logger.error(f"  ✗ TMDB pré-chauffage : {e}")


# ===========================
# Pré-chauffage au démarrage
# ===========================
async def warmup_startup_caches() -> None:
    """
    Pré-charge les caches stables au démarrage.
    Appelé dans lifespan() AVANT que le serveur accepte des connexions.
    """
    from astream.scrapers.animesama.client import animesama_api
    from astream.scrapers.animesama.planning import get_planning_checker

    logger.log("ASTREAM", "Pré-chauffage des caches stables...")

    homepage_anime = []

    try:
        homepage_anime = await animesama_api.get_homepage_content()
        count = len(homepage_anime) if homepage_anime else 0
        logger.log("ASTREAM", f"  ✓ Homepage : {count} anime en cache")
    except Exception as e:
        logger.error(f"  ✗ Homepage : {e}")

    try:
        checker = await get_planning_checker()
        planning = await checker.get_current_planning_anime()
        logger.log("ASTREAM", f"  ✓ Planning : {len(planning)} anime en cours")
    except Exception as e:
        logger.error(f"  ✗ Planning : {e}")

    try:
        checker = await get_planning_checker()
        by_day = await checker.get_planning_by_day()
        total = sum(len(v) for v in by_day.values())
        logger.log("ASTREAM", f"  ✓ Planning/jour : {total} anime sur {len(by_day)} jours")
    except Exception as e:
        logger.error(f"  ✗ Planning/jour : {e}")

    # TMDB — élimine les 20s de cold start pour le 1er utilisateur
    await _warmup_tmdb(homepage_anime)

    logger.log("ASTREAM", "Pré-chauffage terminé — caches stables prêts")


# ===========================
# Rafraîchissement journalier
# ===========================
async def refresh_daily_caches() -> None:
    """Invalide et recharge les caches journaliers. Appelé à minuit Paris."""
    from astream.scrapers.animesama.planning import get_planning_checker
    from astream.scrapers.animesama.client import animesama_api
    from astream.utils.cache import CacheManager

    paris_now = _get_paris_now()
    logger.log("ASTREAM", f"Rafraîchissement journalier ({paris_now.strftime('%A %d/%m %H:%M')} Paris)")

    try:
        await CacheManager.invalidate("as:planning:by_day")
        checker = await get_planning_checker()
        by_day = await checker.get_planning_by_day()
        total = sum(len(v) for v in by_day.values())
        logger.log("ASTREAM", f"  ✓ Planning/jour rechargé : {total} anime")

        today_anime = await checker.get_today_anime()
        logger.log("ASTREAM", f"  ✓ Sorties du jour : {len(today_anime)} anime prêts")
    except Exception as e:
        logger.error(f"  ✗ Erreur planning : {e}")

    homepage_anime = []
    try:
        await CacheManager.invalidate("as:homepage")
        homepage_anime = await animesama_api.get_homepage_content()
        count = len(homepage_anime) if homepage_anime else 0
        logger.log("ASTREAM", f"  ✓ Homepage rechargée : {count} anime")
    except Exception as e:
        logger.error(f"  ✗ Homepage : {e}")

    # Ré-enrichir TMDB pour les éventuels nouveaux anime
    await _warmup_tmdb(homepage_anime)

    logger.log("ASTREAM", "Rafraîchissement journalier terminé")


# ===========================
# Scheduler principal
# ===========================
async def daily_scheduler_task() -> None:
    """Boucle infinie : attend minuit Paris puis rafraîchit."""
    while True:
        try:
            wait_seconds = _seconds_until_midnight_paris()
            paris_now = _get_paris_now()
            next_run = paris_now + timedelta(seconds=wait_seconds)

            logger.log(
                "ASTREAM",
                f"Scheduler : prochain rafraîchissement à {next_run.strftime('%H:%M')} Paris "
                f"(dans {wait_seconds / 3600:.1f}h)"
            )

            await asyncio.sleep(wait_seconds)
            await refresh_daily_caches()

        except asyncio.CancelledError:
            logger.log("ASTREAM", "Scheduler journalier arrêté")
            break
        except Exception as e:
            logger.error(f"Erreur scheduler journalier : {e}")
            await asyncio.sleep(300)


# ===========================
# Refresh périodique
# ===========================
async def periodic_refresh_task() -> None:
    """Anticipe les expirations TTL toutes les ~1h."""
    await asyncio.sleep(300)

    while True:
        try:
            interval = min(settings.DYNAMIC_LIST_TTL, settings.PLANNING_TTL, 3600)
            sleep_time = max(interval - 60, 300)

            await asyncio.sleep(sleep_time)

            logger.log("ASTREAM", "Rafraîchissement périodique des caches...")

            from astream.scrapers.animesama.client import animesama_api
            await animesama_api.get_homepage_content()

            from astream.scrapers.animesama.planning import get_planning_checker
            checker = await get_planning_checker()
            await checker.get_current_planning_anime()

            logger.log("ASTREAM", "Rafraîchissement périodique terminé")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Erreur rafraîchissement périodique : {e}")
            await asyncio.sleep(300)
