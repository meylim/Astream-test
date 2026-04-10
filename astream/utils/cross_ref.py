"""
Base de données de références croisées — le cœur de l'addon.

Mappe tous les identifiants entre eux :
  as_slug   — slug Anime-Sama (ex: "re-zero")
  imdb_id   — ID IMDb (ex: "tt5491994")    → accès Cinemeta
  tmdb_id   — ID TMDB (ex: 70791)          → images HD, descriptions FR
  mal_id    — ID MyAnimeList (ex: 31240)   → données anime MAL/Jikan
  kitsu_id  — ID Kitsu (ex: 11123)         → fallback

Population automatique :
  1. On connaît as_slug + title depuis Anime-Sama
  2. TMDB search(title) → tmdb_id + appel /external_ids → imdb_id
  3. Jikan search(title) → mal_id
  4. Stockage en base SQLite permanent

La table `anime_xref` est persistante — elle ne se vide pas entre les redémarrages.
"""
import time
from typing import Optional, Dict, Any

from astream.utils.logger import logger
from astream.config.settings import database


# ===========================
# CRUD de base
# ===========================

async def get_xref(as_slug: str) -> Optional[Dict[str, Any]]:
    """Récupère la fiche de références croisées pour un slug Anime-Sama."""
    try:
        row = await database.fetch_one(
            "SELECT * FROM anime_xref WHERE as_slug = :slug",
            {"slug": as_slug}
        )
        if row:
            return dict(row)
        return None
    except Exception as e:
        logger.error(f"XREF get {as_slug}: {e}")
        return None


async def get_xref_by_imdb(imdb_id: str) -> Optional[Dict[str, Any]]:
    try:
        row = await database.fetch_one(
            "SELECT * FROM anime_xref WHERE imdb_id = :imdb_id",
            {"imdb_id": imdb_id}
        )
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"XREF by_imdb {imdb_id}: {e}")
        return None


async def get_xref_by_tmdb(tmdb_id: int) -> Optional[Dict[str, Any]]:
    try:
        row = await database.fetch_one(
            "SELECT * FROM anime_xref WHERE tmdb_id = :tmdb_id",
            {"tmdb_id": tmdb_id}
        )
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"XREF by_tmdb {tmdb_id}: {e}")
        return None


async def get_xref_by_mal(mal_id: int) -> Optional[Dict[str, Any]]:
    try:
        row = await database.fetch_one(
            "SELECT * FROM anime_xref WHERE mal_id = :mal_id",
            {"mal_id": mal_id}
        )
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"XREF by_mal {mal_id}: {e}")
        return None


async def save_xref(
    as_slug: str,
    *,
    imdb_id: Optional[str] = None,
    tmdb_id: Optional[int] = None,
    mal_id: Optional[int] = None,
    kitsu_id: Optional[int] = None,
    cinemeta_type: str = "series",
    title: Optional[str] = None,
) -> None:
    """
    Insère ou met à jour une fiche de référence croisée.
    Les champs None ne remplacent pas les valeurs existantes.
    """
    try:
        existing = await get_xref(as_slug)
        now = int(time.time())

        if existing:
            # Mise à jour partielle : ne remplace que les champs fournis
            updates = {"updated_at": now, "slug": as_slug}
            if imdb_id is not None:
                updates["imdb_id"] = imdb_id
            if tmdb_id is not None:
                updates["tmdb_id"] = tmdb_id
            if mal_id is not None:
                updates["mal_id"] = mal_id
            if kitsu_id is not None:
                updates["kitsu_id"] = kitsu_id
            if title is not None:
                updates["title"] = title
            if cinemeta_type:
                updates["cinemeta_type"] = cinemeta_type

            set_clauses = ", ".join(
                f"{k} = :{k}" for k in updates if k != "slug"
            )
            await database.execute(
                f"UPDATE anime_xref SET {set_clauses} WHERE as_slug = :slug",
                updates
            )
        else:
            await database.execute(
                """INSERT INTO anime_xref
                   (as_slug, imdb_id, tmdb_id, mal_id, kitsu_id, cinemeta_type, title, created_at, updated_at)
                   VALUES (:slug, :imdb_id, :tmdb_id, :mal_id, :kitsu_id, :cinemeta_type, :title, :now, :now)""",
                {
                    "slug": as_slug,
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "mal_id": mal_id,
                    "kitsu_id": kitsu_id,
                    "cinemeta_type": cinemeta_type,
                    "title": title,
                    "now": now,
                }
            )
        logger.log("XREF", f"Sauvegardé: {as_slug} → imdb={imdb_id} tmdb={tmdb_id} mal={mal_id}")
    except Exception as e:
        logger.error(f"XREF save {as_slug}: {e}")


# ===========================
# Population automatique
# ===========================

async def resolve_and_save_xref(
    as_slug: str,
    title: str,
    http_client,
    tmdb_api_key: Optional[str] = None,
    existing_tmdb_id: Optional[int] = None,
    existing_mal_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Résout tous les IDs pour un anime et les sauvegarde en base.

    Stratégie :
      1. Si tmdb_id fourni → /external_ids → imdb_id
      2. Sinon TMDB search(title) → tmdb_id → imdb_id
      3. Jikan search(title) → mal_id
      4. Sauvegarde en base

    Retourne la fiche cross_ref complète.
    """
    imdb_id: Optional[str] = None
    tmdb_id: Optional[int] = existing_tmdb_id
    mal_id: Optional[int] = existing_mal_id
    cinemeta_type: str = "series"

    # --- TMDB → imdb_id ---
    if tmdb_api_key:
        try:
            if tmdb_id:
                # On a déjà le TMDB ID, chercher l'IMDb ID directement
                for media_type in ("tv", "movie"):
                    ext_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/external_ids"
                    resp = await http_client.get(ext_url, params={"api_key": tmdb_api_key})
                    if resp.status_code == 200:
                        ext = resp.json()
                        if ext.get("imdb_id"):
                            imdb_id = ext["imdb_id"]
                            cinemeta_type = "movie" if media_type == "movie" else "series"
                            logger.log("XREF", f"{as_slug}: TMDB {tmdb_id} ({media_type}) → IMDb {imdb_id}")
                            break
            else:
                # Recherche TMDB par titre
                from astream.services.tmdb.client import normalize_title
                clean = normalize_title(title, for_search=True)

                for media_type, endpoint in (("tv", "search/tv"), ("movie", "search/movie")):
                    search_url = f"https://api.themoviedb.org/3/{endpoint}"
                    resp = await http_client.get(search_url, params={
                        "api_key": tmdb_api_key,
                        "query": clean,
                        "language": "en-US",
                        "include_adult": False,
                    })
                    if resp.status_code != 200:
                        continue
                    results = resp.json().get("results", [])
                    if not results:
                        continue

                    # Prendre le premier résultat animé (genre_id 16 = Animation)
                    for r in results[:5]:
                        genre_ids = r.get("genre_ids", [])
                        if 16 in genre_ids:  # Animation
                            tmdb_id = r["id"]
                            cinemeta_type = "movie" if media_type == "movie" else "series"
                            # Récupérer IMDb ID
                            ext_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/external_ids"
                            ext_resp = await http_client.get(ext_url, params={"api_key": tmdb_api_key})
                            if ext_resp.status_code == 200:
                                ext = ext_resp.json()
                                imdb_id = ext.get("imdb_id")
                            logger.log("XREF", f"{as_slug}: TMDB search → {tmdb_id} → IMDb {imdb_id}")
                            break
                    if tmdb_id:
                        break
        except Exception as e:
            logger.warning(f"XREF TMDB resolve {as_slug}: {e}")

    # --- anime_db → mal_id (instant, sans appel API) ---
    if not mal_id:
        try:
            from astream.utils.anime_db import search_by_title, _db_loaded
            if _db_loaded:
                db_results = search_by_title(title, max_results=3)
                if db_results:
                    mal_id = db_results[0]["_ids"].get("mal_id")
                    if mal_id:
                        logger.log("XREF", f"{as_slug}: anime_db → MAL {mal_id}")
        except Exception as e:
            logger.debug(f"XREF anime_db lookup {as_slug}: {e}")

    # --- Jikan → mal_id (fallback si anime_db pas chargée ou pas de résultat) ---
    if not mal_id:
        try:
            from astream.services.jikan.client import jikan_client
            results = await jikan_client.search_anime(query=title, limit=3)
            if results:
                best = results[0]
                mal_id = best.get("mal_id")
                logger.log("XREF", f"{as_slug}: Jikan search → MAL {mal_id}")
        except Exception as e:
            logger.warning(f"XREF Jikan resolve {as_slug}: {e}")

    # --- Sauvegarde ---
    await save_xref(
        as_slug,
        imdb_id=imdb_id,
        tmdb_id=tmdb_id,
        mal_id=mal_id,
        cinemeta_type=cinemeta_type,
        title=title,
    )

    return {
        "as_slug": as_slug,
        "imdb_id": imdb_id,
        "tmdb_id": tmdb_id,
        "mal_id": mal_id,
        "cinemeta_type": cinemeta_type,
        "title": title,
    }


async def get_or_resolve_xref(
    as_slug: str,
    title: str,
    http_client,
    tmdb_api_key: Optional[str] = None,
    existing_tmdb_id: Optional[int] = None,
    existing_mal_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Récupère le xref depuis la DB ou le résout et le sauvegarde.
    Point d'entrée principal.
    """
    existing = await get_xref(as_slug)
    if existing and (existing.get("imdb_id") or existing.get("tmdb_id") or existing.get("mal_id")):
        return existing

    logger.log("XREF", f"Résolution des IDs pour: {as_slug} ('{title}')")
    return await resolve_and_save_xref(
        as_slug, title, http_client,
        tmdb_api_key=tmdb_api_key,
        existing_tmdb_id=existing_tmdb_id,
        existing_mal_id=existing_mal_id,
    )
