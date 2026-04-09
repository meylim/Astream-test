"""
Scheduler de pré-chauffage et rafraîchissement automatique des caches.

Stratégie :
  AU DÉMARRAGE  → Anime-Sama (homepage + planning) + Jikan (tous catalogues) + TMDB enrichissement
  À MINUIT PARIS → Invalide + recharge tous les caches Jikan + Anime-Sama + TMDB
  TOUTES LES ~1H → Anticipe expiration TTL homepage + planning Anime-Sama

Jikan : les 5 catalogues sont chargés UNE FOIS par jour (TTL 24h) :
  - Sorties du jour (planning hebdomadaire du jour courant)
  - Simulcasts en cours
  - Films
  - Top anime (popularité)
  - Liste des genres
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


def _seconds_until_midnight_paris() -> float:
    """Calcule le nombre de secondes jusqu'à minuit Paris."""
    paris_now = _get_paris_now()
    tomorrow = (paris_now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max((tomorrow - paris_now).total_seconds(), 60)


# ===========================
# Pré-chauffage Jikan
# ===========================
async def _warmup_jikan() -> None:
    """
    Pré-charge TOUS les catalogues Jikan en cache (TTL 24h).
    Appelé au démarrage et à minuit Paris.
    Les appels suivants des routes catalog lisent depuis le cache → aucune latence.
    """
    try:
        from astream.services.jikan.client import jikan_client
        from datetime import datetime as _dt

        today_idx = _dt.now().weekday()
        day_map = {0: "monday", 1: "tuesday", 2: "wednesday",
                   3: "thursday", 4: "friday", 5: "saturday", 6: "sunday"}
        today_en = day_map.get(today_idx, "monday")

        logger.log("ASTREAM", "  ⏳ Jikan : chargement des catalogues...")

        # Toutes les requêtes en séquence (rate limit Jikan : 3 req/s)
        from astream.services.catalog import GENRE_CATALOG_MAP
        from astream.services.jikan.service import JIKAN_GENRE_ID_MAP

        tasks_info = [
            ("Planning du jour",    jikan_client.get_schedules(today_en)),
            ("Simulcasts",          jikan_client.get_airing(limit=25)),
            ("Films",               jikan_client.get_movies(limit=25)),
            ("Top popularité",      jikan_client.get_top_anime(filter_type="bypopularity", limit=25)),
            ("Top airing",          jikan_client.get_top_anime(filter_type="airing", limit=25)),
            ("Saison en cours",     jikan_client.get_season_now(limit=25)),
            ("Prochaine saison",    jikan_client.get_season_upcoming(limit=25)),
            ("Genres",              jikan_client.get_genres()),
        ]

        # Ajouter tous les genres du manifest
        for catalog_id, genre_name in GENRE_CATALOG_MAP.items():
            genre_id = JIKAN_GENRE_ID_MAP.get(genre_name)
            if genre_id:
                tasks_info.append(
                    (f"Genre {genre_name}", jikan_client.get_anime_by_genre(genre_id=genre_id, limit=25))
                )

        for label, coro in tasks_info:
            try:
                result = await coro
                count = len(result) if result else 0
                logger.log("ASTREAM", f"    ✓ Jikan {label} : {count} entrées en cache")
            except Exception as e:
                logger.error(f"    ✗ Jikan {label} : {e}")
            # Pause légère entre les appels pour respecter le rate limit
            await asyncio.sleep(0.5)

        logger.log("ASTREAM", "  ✓ Jikan : tous les catalogues en cache")

    except Exception as e:
        logger.error(f"  ✗ Jikan warmup global : {e}")


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


async def _warmup_tmdb_jikan() -> None:
    """
    Pré-charge TMDB pour tous les catalogues Jikan :
    simulcasts, films, top, saison, prochaine saison et les 16 genres.
    Appelé après _warmup_jikan() pour que les données soient déjà en cache Jikan.
    """
    if not settings.TMDB_API_KEY:
        return

    try:
        from astream.services.jikan.service import jikan_service
        from astream.services.catalog import GENRE_CATALOG_MAP

        logger.log("ASTREAM", "  ⏳ TMDB×Jikan : enrichissement de tous les catalogues Jikan...")

        all_anime = []
        seen_ids = set()

        def _collect(items):
            for item in (items or []):
                mid = item.get("mal_id")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_anime.append(item)

        # Catalogues principaux (lus depuis le cache Jikan)
        for label, coro in [
            ("simulcasts",       jikan_service.get_simulcasts(limit=25)),
            ("films",            jikan_service.get_films(limit=25)),
            ("top",              jikan_service.get_top_anime(limit=25)),
            ("saison_now",       jikan_service.get_season_now(limit=25)),
            ("saison_upcoming",  jikan_service.get_season_upcoming(limit=25)),
        ]:
            try:
                _collect(await coro)
            except Exception as e:
                logger.error(f"    ✗ TMDB collect {label} : {e}")

        # Tous les genres du manifest
        for catalog_id, genre_name in GENRE_CATALOG_MAP.items():
            try:
                _collect(await jikan_service.get_by_genre(genre_name=genre_name, limit=25))
            except Exception as e:
                logger.error(f"    ✗ TMDB collect genre {genre_name} : {e}")

        logger.log("ASTREAM", f"  ⏳ TMDB×Jikan : enrichissement de {len(all_anime)} anime uniques...")
        if all_anime:
            await _warmup_tmdb(all_anime)

    except Exception as e:
        logger.error(f"  ✗ TMDB×Jikan warmup : {e}")


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

    # --- Jikan (tous les catalogues) ---
    logger.log("ASTREAM", "② Jikan : chargement des 5 catalogues")
    await _warmup_jikan()

    # --- TMDB pour Anime-Sama homepage ---
    logger.log("ASTREAM", "③ TMDB : enrichissement homepage Anime-Sama")
    await _warmup_tmdb(homepage_anime)

    # --- TMDB pour Jikan ---
    logger.log("ASTREAM", "④ TMDB : enrichissement catalogues Jikan")
    await _warmup_tmdb_jikan()

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

    # --- Jikan : invalider tous les caches Jikan ---
    logger.log("ASTREAM", "② Jikan : invalidation + rechargement")
    try:
        from astream.services.catalog import GENRE_CATALOG_MAP
        from astream.services.jikan.service import JIKAN_GENRE_ID_MAP

        jikan_cache_keys = [
            "jikan:airing:25",
            "jikan:movies:25",
            "jikan:top:bypopularity:25",
            "jikan:top:airing:25",
            "jikan:season_now:25",
            "jikan:season_upcoming:25",
            "jikan:genres",
        ]
        # Invalider le planning du jour
        day_map = {0: "monday", 1: "tuesday", 2: "wednesday",
                   3: "thursday", 4: "friday", 5: "saturday", 6: "sunday"}
        today_key = f"jikan:schedule:{day_map.get(datetime.now().weekday(), 'monday')}"
        jikan_cache_keys.append(today_key)

        # Invalider tous les caches genre
        for genre_name in GENRE_CATALOG_MAP.values():
            genre_id = JIKAN_GENRE_ID_MAP.get(genre_name)
            if genre_id:
                jikan_cache_keys.append(f"jikan:genre:{genre_id}:25")

        for key in jikan_cache_keys:
            try:
                await CacheManager.invalidate(key)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"  ✗ Invalidation cache Jikan : {e}")

    # Recharger Jikan après invalidation
    await _warmup_jikan()

    # --- TMDB ---
    logger.log("ASTREAM", "③ TMDB : ré-enrichissement")
    await _warmup_tmdb(homepage_anime)
    await _warmup_tmdb_jikan()

    logger.log("ASTREAM", "═══ Rafraîchissement journalier terminé ═══")


# ===========================
# Scheduler principal (boucle minuit Paris)
# ===========================
async def daily_scheduler_task() -> None:
    """Boucle infinie : attend minuit Paris puis rafraîchit tous les caches."""
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
