"""
Scheduler de pré-chauffage et rafraîchissement automatique des caches.

Stratégie :
  ┌─────────────────────────────────────────────────────────────────┐
  │  AU DÉMARRAGE (lifespan)                                       │
  │  → homepage (stable ~1h)                                       │
  │  → planning global (stable ~1h)                                │
  │  → nouveautés (= 24 premiers de homepage, pas de coût extra)   │
  │  → en_cours (dépend du planning global, pas du jour)           │
  │  ⛔ PAS sorties_du_jour (dépend du jour courant → scheduler)   │
  └─────────────────────────────────────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────────────┐
  │  À MINUIT PARIS (scheduler quotidien)                          │
  │  → Invalide + recharge planning_by_day                         │
  │  → Invalide + recharge sorties_du_jour                         │
  │  → Invalide + recharge en_cours (les "aujourd'hui" changent)   │
  │                                                                 │
  │  TOUTES LES HEURES                                             │
  │  → Rafraîchit homepage si TTL expiré                           │
  │  → Rafraîchit planning global si TTL expiré                    │
  └─────────────────────────────────────────────────────────────────┘

Le TMDB n'est PAS pré-chargé ici car il nécessite une config utilisateur
(clé API). Il est chargé lazy au premier appel et profite du cache SQLite.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from astream.utils.logger import logger

# Timezone Paris (UTC+1 / UTC+2 en été)
# On utilise un offset fixe simplifié — pour être précis il faudrait pytz/zoneinfo
# mais sur Alpine/Docker c'est souvent absent. On calcule manuellement.
PARIS_UTC_OFFSET_WINTER = 1  # CET
PARIS_UTC_OFFSET_SUMMER = 2  # CEST


def _get_paris_now() -> datetime:
    """Retourne l'heure actuelle à Paris (gestion simplifiée DST)."""
    utc_now = datetime.now(timezone.utc)
    # Règle DST Europe : dernier dimanche de mars → dernier dimanche d'octobre
    year = utc_now.year
    # Dernier dimanche de mars
    march_last = datetime(year, 3, 31, tzinfo=timezone.utc)
    dst_start = march_last - timedelta(days=(march_last.weekday() + 1) % 7)
    dst_start = dst_start.replace(hour=1)  # 01:00 UTC
    # Dernier dimanche d'octobre
    oct_last = datetime(year, 10, 31, tzinfo=timezone.utc)
    dst_end = oct_last - timedelta(days=(oct_last.weekday() + 1) % 7)
    dst_end = dst_end.replace(hour=1)  # 01:00 UTC

    if dst_start <= utc_now < dst_end:
        offset = PARIS_UTC_OFFSET_SUMMER
    else:
        offset = PARIS_UTC_OFFSET_WINTER

    return utc_now + timedelta(hours=offset)


def _seconds_until_midnight_paris() -> float:
    """Calcule le nombre de secondes jusqu'à minuit Paris."""
    paris_now = _get_paris_now()
    # Prochain minuit
    tomorrow = (paris_now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    delta = tomorrow - paris_now
    return max(delta.total_seconds(), 60)  # Minimum 60s de sécurité


# ===========================
# Pré-chauffage au démarrage
# ===========================
async def warmup_startup_caches() -> None:
    """
    Pré-charge les caches stables au démarrage.
    Appelé dans lifespan() AVANT que le serveur accepte des connexions.
    
    Charge :
      - Homepage (as:homepage) → base pour nouveautés + catalog principal
      - Planning global (as:planning) → base pour en_cours
      - Planning par jour (as:planning:by_day) → prêt pour sorties_du_jour
    
    Ne charge PAS :
      - Sorties du jour → sera chargé au premier appel ou par le scheduler
      - TMDB → nécessite config utilisateur, lazy loading
    """
    from astream.scrapers.animesama.client import animesama_api
    from astream.scrapers.animesama.planning import get_planning_checker

    logger.log("ASTREAM", "Pré-chauffage des caches stables...")

    try:
        # 1. Homepage — remplit as:homepage
        #    Aussi utilisé par : nouveautés (24 premiers), catalog principal, en_cours
        homepage = await animesama_api.get_homepage_content()
        count = len(homepage) if homepage else 0
        logger.log("ASTREAM", f"  ✓ Homepage : {count} anime en cache")

    except Exception as e:
        logger.error(f"  ✗ Homepage : {e}")

    try:
        # 2. Planning global — remplit as:planning
        #    Utilisé par : en_cours, is_anime_ongoing, smart_cache_ttl
        checker = await get_planning_checker()
        planning = await checker.get_current_planning_anime()
        logger.log("ASTREAM", f"  ✓ Planning : {len(planning)} anime en cours")

    except Exception as e:
        logger.error(f"  ✗ Planning : {e}")

    try:
        # 3. Planning par jour — remplit as:planning:by_day
        #    Utilisé par : sorties_du_jour, en_cours (tri "aujourd'hui" en tête)
        checker = await get_planning_checker()
        by_day = await checker.get_planning_by_day()
        total = sum(len(v) for v in by_day.values())
        logger.log("ASTREAM", f"  ✓ Planning/jour : {total} anime sur {len(by_day)} jours")

    except Exception as e:
        logger.error(f"  ✗ Planning/jour : {e}")

    logger.log("ASTREAM", "Pré-chauffage terminé — caches stables prêts")


# ===========================
# Rafraîchissement des données journalières
# ===========================
async def refresh_daily_caches() -> None:
    """
    Invalide et recharge les caches qui dépendent du jour courant.
    Appelé à minuit Paris par le scheduler.
    
    Invalide + recharge :
      - Planning par jour (as:planning:by_day) → les jours ont tourné
      - Le cache "sorties du jour" sera naturellement rechargé au prochain appel
        car il dépend de get_today_anime() qui lit planning_by_day
    """
    from astream.scrapers.animesama.planning import get_planning_checker
    from astream.utils.cache import CacheManager

    paris_now = _get_paris_now()
    logger.log("ASTREAM", f"Rafraîchissement journalier ({paris_now.strftime('%A %d/%m %H:%M')} Paris)")

    try:
        # 1. Invalider le planning par jour pour forcer un re-scrape
        #    Le planning global (as:planning) reste valide — les anime en cours
        #    ne changent pas à minuit, seul le jour de diffusion change
        await CacheManager.invalidate("as:planning:by_day")
        logger.log("ASTREAM", "  ✓ Cache planning/jour invalidé")

        # 2. Recharger immédiatement le planning par jour
        checker = await get_planning_checker()
        by_day = await checker.get_planning_by_day()
        total = sum(len(v) for v in by_day.values())
        logger.log("ASTREAM", f"  ✓ Planning/jour rechargé : {total} anime")

        # 3. Récupérer les anime du nouveau jour
        today_anime = await checker.get_today_anime()
        logger.log("ASTREAM", f"  ✓ Sorties du jour : {len(today_anime)} anime prêts")

    except Exception as e:
        logger.error(f"  ✗ Erreur rafraîchissement journalier : {e}")

    # 4. Rafraîchir aussi la homepage (les "sorties" peuvent changer)
    try:
        from astream.scrapers.animesama.client import animesama_api
        await CacheManager.invalidate("as:homepage")
        homepage = await animesama_api.get_homepage_content()
        count = len(homepage) if homepage else 0
        logger.log("ASTREAM", f"  ✓ Homepage rechargée : {count} anime")
    except Exception as e:
        logger.error(f"  ✗ Homepage : {e}")

    logger.log("ASTREAM", "Rafraîchissement journalier terminé")


# ===========================
# Tâche scheduler principale
# ===========================
async def daily_scheduler_task() -> None:
    """
    Boucle infinie qui attend minuit Paris puis rafraîchit les caches journaliers.
    Conçu pour être lancé comme asyncio.create_task() dans lifespan().
    """
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

            # Exécuter le rafraîchissement
            await refresh_daily_caches()

        except asyncio.CancelledError:
            logger.log("ASTREAM", "Scheduler journalier arrêté")
            break
        except Exception as e:
            logger.error(f"Erreur scheduler journalier : {e}")
            # En cas d'erreur, attendre 5 min avant de réessayer
            await asyncio.sleep(300)


# ===========================
# Rafraîchissement périodique (toutes les heures)
# ===========================
async def periodic_refresh_task() -> None:
    """
    Rafraîchit les caches à TTL expiré de manière proactive,
    pour que les utilisateurs ne tombent jamais sur un cache miss.
    """
    from astream.config.settings import settings

    # Attendre 5 min après le démarrage pour laisser le warmup se terminer
    await asyncio.sleep(300)

    while True:
        try:
            interval = min(settings.DYNAMIC_LIST_TTL, settings.PLANNING_TTL, 3600)
            # Rafraîchir 60s AVANT l'expiration pour anticiper
            sleep_time = max(interval - 60, 300)

            await asyncio.sleep(sleep_time)

            logger.log("ASTREAM", "Rafraîchissement périodique des caches...")

            # Homepage — si le TTL est proche de l'expiration, il sera
            # automatiquement re-fetché par get_or_fetch
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
