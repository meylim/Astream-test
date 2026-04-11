"""
Scheduler de pré-chauffage et rafraîchissement automatique des caches.

Stratégie :
  AU DÉMARRAGE  → Anime-Sama (homepage + planning) + Adkami (catalogues JSON) + TMDB enrichissement
  À 3H PARIS    → Reconstruction complète des catalogues Adkami + Anime-Sama + TMDB
  TOUTES LES ~1H → Anticipe expiration TTL homepage + planning Anime-Sama

Adkami : les catalogues JSON sont construits une fois par jour à 3h Paris.
Simulcast : rechargé EN DIRECT à chaque appel client (scraper.scan_simulcasts_cached).
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
    return utc_now + timedelta(hours=PARIS_UTC_OFFSET_SUMMER if dst_start <= utc_now < dst_end else PARIS_UTC_OFFSET_WINTER)


def _seconds_until_3am_paris() -> float:
    """Calcule le nombre de secondes jusqu'à 3h du matin Paris."""
    paris_now = _get_paris_now()
    # Prochain 3h : aujourd'hui si pas encore passé, sinon demain
    target = paris_now.replace(hour=3, minute=0, second=0, microsecond=0)
    if target <= paris_now:
        target += timedelta(days=1)
    return max((target - paris_now).total_seconds(), 60)


# ===========================
# Initialisation / Rafraîchissement des catalogues Adkami via GitHub
# ===========================
async def _warmup_adkami_catalogs(force: bool = False) -> None:
    """
    Initialise les catalogues Adkami :
    - force=False (démarrage) : télécharge les fichiers manquants depuis GitHub
    - force=True  (3h daily)  : re-télécharge TOUS les fichiers depuis GitHub

    Si ADKAMI_CATALOGS_URL n'est pas configurée, les fichiers seed locaux
    (data/catalogues/) sont utilisés directement sans aucun téléchargement.
    """
    try:
        from astream.scrapers.adkami.catalog_loader import catalog_loader
        if force:
            logger.log("ASTREAM", "  ⏳ Adkami : re-téléchargement complet depuis GitHub...")
            await catalog_loader.refresh_all()
        else:
            logger.log("ASTREAM", "  ⏳ Adkami : initialisation des catalogues...")
            await catalog_loader.initialize()
        logger.log("ASTREAM", "  ✓ Adkami : catalogues prêts")
    except Exception as e:
        logger.error(f"  ✗ Adkami catalogues : {e}")


# ===========================
# Alias de compatibilité
# ===========================
async def _warmup_jikan() -> None:
    """Alias conservé pour compatibilité — délègue à CatalogLoader."""
    await _warmup_adkami_catalogs(force=True)


# ===========================
# Pré-chauffage TMDB (Anime-Sama + Jikan)
# ===========================
async def _warmup_tmdb(anime_list: list) -> None:
    """
    Pré-charge le cache TMDB pour une liste d'anime.
    Utilisé pour la homepage Anime-Sama et les catalogues Jikan.
    """
    if not settings.TMDB_API_KEY:
        logger.log("ASTREAM", "  ⏭ TMDB : pas de TMDB_API_KEY serveur, skip")
        return

    if not anime_list:
        return

    try:
        from astream.utils.validators import ConfigModel
        from astream.services.catalog import catalog_service

        warmup_config = ConfigModel(
            tmdbApiKey=settings.TMDB_API_KEY,
            tmdbEnabled=True,
        )

        logger.log("ASTREAM", f"  ⏳ TMDB : enrichissement de {len(anime_list)} anime...")
        enhanced = await catalog_service._enrich_catalog_with_tmdb(anime_list, warmup_config)
        enriched = sum(1 for a in enhanced if a.get("poster"))
        logger.log("ASTREAM", f"  ✓ TMDB : {enriched}/{len(anime_list)} anime enrichis en cache")

    except Exception as e:
        logger.error(f"  ✗ TMDB pré-chauffage : {e}")


async def _warmup_tmdb_adkami() -> None:
    """
    Pré-charge TMDB pour tous les catalogues Adkami (JSON sur disque).
    """
    if not settings.TMDB_API_KEY:
        return

    try:
        from astream.services.adkami_catalog import adkami_catalog_service, ADKAMI_CATALOG_MAP

        logger.log("ASTREAM", "  ⏳ TMDB×Adkami : enrichissement de tous les catalogues Adkami...")

        all_anime = []
        seen_ids: set = set()

        def _collect(items):
            for item in (items or []):
                mid = item.get("mal_id") or item.get("slug")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_anime.append(item)

        # Simulcasts
        _collect(adkami_catalog_service.get_simulcast_catalog(limit=25))

        # Tous les genres
        for catalog_id, genre_slug in ADKAMI_CATALOG_MAP.items():
            try:
                _collect(adkami_catalog_service.get_genre_catalog(genre_slug, limit=25))
            except Exception as e:
                logger.error(f"    ✗ TMDB collect genre {genre_slug} : {e}")

        logger.log("ASTREAM", f"  ⏳ TMDB×Adkami : enrichissement de {len(all_anime)} anime uniques...")
        if all_anime:
            await _warmup_tmdb(all_anime)

    except Exception as e:
        logger.error(f"  ✗ TMDB×Adkami warmup : {e}")


# ===========================
# Pré-chauffage complet au démarrage
# ===========================
async def warmup_startup_caches() -> None:
    """
    Pré-charge tous les caches au démarrage AVANT que le serveur accepte des connexions.
    Ordre : Anime-Sama → Jikan → TMDB (Anime-Sama) → TMDB (Jikan)
    """
    from astream.scrapers.animesama.client import animesama_api
    from astream.scrapers.animesama.planning import get_planning_checker

    logger.log("ASTREAM", "═══════════════════════════════════════")
    logger.log("ASTREAM", "Pré-chauffage des caches au démarrage...")
    logger.log("ASTREAM", "═══════════════════════════════════════")

    # --- Anime Offline Database (cross-refs) ---
    logger.log("ASTREAM", "⓪ Anime Offline Database : chargement des cross-refs")
    try:
        from astream.utils.anime_db import load_anime_db
        success = await load_anime_db()
        if success:
            logger.log("ASTREAM", "  ✓ Anime Offline Database chargée")
        else:
            logger.log("ASTREAM", "  ⚠ Anime Offline Database indisponible (dégradé)")
    except Exception as e:
        logger.error(f"  ✗ Anime Offline Database : {e}")

    homepage_anime = []

    # --- Anime-Sama ---
    logger.log("ASTREAM", "① Anime-Sama : homepage + planning")
    try:
        homepage_anime = await animesama_api.get_homepage_content()
        logger.log("ASTREAM", f"  ✓ Homepage : {len(homepage_anime)} anime")
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

    # --- Adkami (catalogues JSON sur disque) ---
    logger.log("ASTREAM", "② Adkami : construction des catalogues JSON")
    await _warmup_adkami_catalogs(force=False)

    # --- TMDB pour Anime-Sama homepage ---
    logger.log("ASTREAM", "③ TMDB : enrichissement homepage Anime-Sama")
    await _warmup_tmdb(homepage_anime)

    # --- TMDB pour Adkami ---
    logger.log("ASTREAM", "④ TMDB : enrichissement catalogues Adkami")
    await _warmup_tmdb_adkami()

    logger.log("ASTREAM", "═══════════════════════════════════════")
    logger.log("ASTREAM", "Pré-chauffage terminé — tous les caches prêts")
    logger.log("ASTREAM", "═══════════════════════════════════════")


# ===========================
# Rafraîchissement journalier (minuit Paris)
# ===========================
async def refresh_daily_caches() -> None:
    """
    Invalide et recharge tous les caches une fois par jour à minuit Paris.
    Garantit des données fraîches sans latence pour les utilisateurs.
    """
    from astream.scrapers.animesama.planning import get_planning_checker
    from astream.scrapers.animesama.client import animesama_api
    from astream.utils.cache import CacheManager

    paris_now = _get_paris_now()
    logger.log("ASTREAM", f"═══ Rafraîchissement journalier — {paris_now.strftime('%A %d/%m %H:%M')} Paris ═══")

    # --- Anime-Sama ---
    logger.log("ASTREAM", "① Anime-Sama : invalidation + rechargement")
    homepage_anime = []
    try:
        await CacheManager.invalidate("as:planning:by_day")
        checker = await get_planning_checker()
        by_day = await checker.get_planning_by_day()
        total = sum(len(v) for v in by_day.values())
        logger.log("ASTREAM", f"  ✓ Planning/jour : {total} anime")
    except Exception as e:
        logger.error(f"  ✗ Planning : {e}")

    try:
        await CacheManager.invalidate("as:homepage")
        homepage_anime = await animesama_api.get_homepage_content()
        logger.log("ASTREAM", f"  ✓ Homepage : {len(homepage_anime)} anime")
    except Exception as e:
        logger.error(f"  ✗ Homepage : {e}")

    # --- Adkami : reconstruction complète des catalogues JSON ---
    logger.log("ASTREAM", "② Adkami : reconstruction des catalogues JSON")
    await _warmup_adkami_catalogs(force=True)

    # --- TMDB ---
    logger.log("ASTREAM", "③ TMDB : ré-enrichissement homepage")
    await _warmup_tmdb(homepage_anime)

    # --- TMDB pour Adkami ---
    logger.log("ASTREAM", "④ TMDB : ré-enrichissement catalogues Adkami")
    await _warmup_tmdb_adkami()

    # Rafraîchir l'Anime Offline Database une fois par semaine (pas daily pour économiser la bande)
    try:
        import os as _os
        db_path = _os.path.join("data", "anime-offline-db.json")
        import time as _time
        if not _os.path.exists(db_path) or (_time.time() - _os.path.getmtime(db_path)) > 604800:
            from astream.utils.anime_db import load_anime_db
            await load_anime_db(force_refresh=True)
            logger.log("ASTREAM", "  ✓ Anime Offline Database rafraîchie")
    except Exception as e:
        logger.error(f"  ✗ Anime DB refresh : {e}")

    logger.log("ASTREAM", "═══ Rafraîchissement journalier terminé ═══")


# ===========================
# Scheduler principal (boucle 3h Paris)
# ===========================
async def daily_scheduler_task() -> None:
    """Boucle infinie : attend 3h Paris puis reconstruit tous les catalogues Adkami."""
    while True:
        try:
            wait_seconds = _seconds_until_3am_paris()
            paris_now = _get_paris_now()
            next_run = paris_now + timedelta(seconds=wait_seconds)
            logger.log(
                "ASTREAM",
                f"Scheduler : prochain rafraîchissement Adkami à {next_run.strftime('%H:%M')} Paris "
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
# Refresh périodique Anime-Sama (~1h)
# ===========================
async def periodic_refresh_task() -> None:
    """Anticipe les expirations TTL Anime-Sama toutes les ~1h (données Jikan non concernées)."""
    await asyncio.sleep(300)

    while True:
        try:
            interval = min(settings.DYNAMIC_LIST_TTL, settings.PLANNING_TTL, 3600)
            sleep_time = max(interval - 60, 300)
            await asyncio.sleep(sleep_time)

            logger.log("ASTREAM", "Rafraîchissement périodique Anime-Sama...")
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
