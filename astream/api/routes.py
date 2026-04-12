from typing import Dict, List, Any, Optional
from fastapi import APIRouter, Request, Path, Query
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from astream.config.settings import settings, web_config, get_base_manifest
from astream.utils.filters import get_all_excluded_domains
from astream.utils.validators import validate_config, ConfigModel
from astream.utils.logger import logger
from astream.services.stream import stream_service
from astream.services.catalog import catalog_service, GENRE_CATALOG_MAP
from fastapi import HTTPException
from astream.services.metadata import metadata_service


templates = Jinja2Templates("astream/public")
main = APIRouter()


# ===========================
# Helper : extraire skip depuis query params ou path extra
# ===========================
def _parse_skip(request: Request, skip_path: Optional[str] = None) -> int:
    """Extrait skip depuis le path Stremio (skip=N) ou query params."""
    if skip_path is not None:
        try:
            return int(skip_path)
        except (ValueError, TypeError):
            pass
    raw = request.query_params.get("skip")
    if raw:
        try:
            return int(raw)
        except (ValueError, TypeError):
            pass
    return 0


# ===========================
# Web
# ===========================
@main.get("/", summary="Accueil")
async def root() -> RedirectResponse:
    return RedirectResponse("/configure")

@main.get("/configure", summary="Configuration")
async def configure(request: Request) -> Any:
    config_data = dict(web_config)
    config_data["ADDON_NAME"] = settings.ADDON_NAME
    return templates.TemplateResponse(
        request, "index.html",
        {"CUSTOM_HEADER_HTML": settings.CUSTOM_HEADER_HTML or "",
         "EXCLUDED_DOMAINS": get_all_excluded_domains(),
         "webConfig": config_data},
    )

@main.get("/{b64config}/configure", summary="Reconfiguration")
async def configure_addon(request: Request, b64config: str = Path(...)) -> Any:
    config_data = dict(web_config)
    config_data["ADDON_NAME"] = settings.ADDON_NAME
    return templates.TemplateResponse(
        request, "index.html",
        {"CUSTOM_HEADER_HTML": settings.CUSTOM_HEADER_HTML or "",
         "EXCLUDED_DOMAINS": get_all_excluded_domains(),
         "webConfig": config_data},
    )


# ===========================
# Manifest
# ===========================
@main.get("/{b64config}/manifest.json")
async def manifest(request: Request, b64config: str = Path(...)) -> Dict[str, Any]:
    base_manifest = get_base_manifest()
    config = validate_config(b64config)
    language_extension = config.get("language", "Tout")
    if language_extension != "Tout":
        base_manifest["name"] = f"{settings.ADDON_NAME} | {language_extension}"
    else:
        base_manifest["name"] = settings.ADDON_NAME
    try:
        unique_genres = await catalog_service.extract_unique_genres()
        base_manifest["catalogs"][0]["extra"][1]["options"] = unique_genres
    except Exception as e:
        logger.error(f"MANIFEST - Echec genres: {e}")
    return base_manifest

@main.get("/manifest.json")
async def manifest_default(request: Request) -> Dict[str, Any]:
    base_manifest = get_base_manifest()
    base_manifest["name"] = "| AStream"
    base_manifest["description"] = (
        f"CONFIGURATION OBSOLETE, VEUILLEZ RECONFIGURER SUR {request.url.scheme}://{request.url.netloc}"
    )
    try:
        unique_genres = await catalog_service.extract_unique_genres()
        base_manifest["catalogs"][0]["extra"][1]["options"] = unique_genres
    except Exception as e:
        logger.error(f"MANIFEST - Echec genres: {e}")
    return base_manifest


# ===========================
# Catalogue principal (recherche + genre + skip)
# ===========================
@main.get("/{b64config}/catalog/anime/animesama_catalog.json")
@main.get("/{b64config}/catalog/anime/animesama_catalog/search={search}.json")
@main.get("/{b64config}/catalog/anime/animesama_catalog/genre={genre}.json")
@main.get("/{b64config}/catalog/anime/animesama_catalog/skip={skip_val}.json")
@main.get("/{b64config}/catalog/anime/animesama_catalog/genre={genre}&skip={skip_val}.json")
@main.get("/{b64config}/catalog/anime/animesama_catalog/search={search}&genre={genre}.json")
@main.get("/{b64config}/catalog/anime/animesama_catalog/search={search}&skip={skip_val}.json")
@main.get("/{b64config}/catalog/series/animesama_catalog.json")
@main.get("/{b64config}/catalog/series/animesama_catalog/search={search}.json")
@main.get("/{b64config}/catalog/series/animesama_catalog/genre={genre}.json")
@main.get("/{b64config}/catalog/series/animesama_catalog/skip={skip_val}.json")
@main.get("/{b64config}/catalog/series/animesama_catalog/genre={genre}&skip={skip_val}.json")
@main.get("/{b64config}/catalog/series/animesama_catalog/search={search}&skip={skip_val}.json")
async def animesama_catalog(
    request: Request,
    b64config: Optional[str] = None,
    search: Optional[str] = None,
    genre: Optional[str] = None,
    skip_val: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    try:
        if not search and "search" in request.query_params:
            search = request.query_params.get("search")
        if not genre and "genre" in request.query_params:
            genre = request.query_params.get("genre")
        skip = _parse_skip(request, skip_val)

        config_dict = validate_config(b64config)
        config = ConfigModel(**config_dict)

        metas = await catalog_service.get_complete_catalog(
            request=request, b64config=b64config,
            search=search, genre=genre, config=config, skip=skip,
        )
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalogue: {e}")
        return {"metas": []}


@main.get("/catalog/anime/animesama_catalog.json")
@main.get("/catalog/anime/animesama_catalog/search={search}.json")
@main.get("/catalog/anime/animesama_catalog/genre={genre}.json")
@main.get("/catalog/anime/animesama_catalog/skip={skip_val}.json")
@main.get("/catalog/anime/animesama_catalog/genre={genre}&skip={skip_val}.json")
@main.get("/catalog/series/animesama_catalog.json")
@main.get("/catalog/series/animesama_catalog/search={search}.json")
@main.get("/catalog/series/animesama_catalog/genre={genre}.json")
@main.get("/catalog/series/animesama_catalog/skip={skip_val}.json")
@main.get("/catalog/series/animesama_catalog/genre={genre}&skip={skip_val}.json")
async def catalog_default(
    request: Request,
    search: Optional[str] = None,
    genre: Optional[str] = None,
    skip_val: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    try:
        if not search:
            search = request.query_params.get("search")
        if not genre:
            genre = request.query_params.get("genre")
        skip = _parse_skip(request, skip_val)
        config = ConfigModel()
        metas = await catalog_service.get_complete_catalog(
            request=request, b64config=None,
            search=search, genre=genre, config=config, skip=skip,
        )
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalogue: {e}")
        return {"metas": []}


# ===========================
# Meta et Stream (inchangés)
# ===========================
@main.get("/{b64config}/meta/anime/{id}.json")
async def animesama_meta(request: Request, id: str = Path(...), b64config: str = Path(...)):
    config_dict = validate_config(b64config)
    config = ConfigModel(**config_dict)
    meta = await metadata_service.get_complete_anime_meta(
        anime_id=id, config=config, request=request, b64config=b64config,
    )
    return {"meta": meta}

@main.get("/meta/anime/{id}.json")
async def meta_default(request: Request, id: str = Path(...)):
    config = ConfigModel()
    meta = await metadata_service.get_complete_anime_meta(
        anime_id=id, config=config, request=request, b64config=None,
    )
    return {"meta": meta}

@main.get("/{b64config}/stream/anime/{episode_id}.json")
@main.get("/{b64config}/stream/series/{episode_id}.json")
@main.get("/{b64config}/stream/movie/{episode_id}.json")
async def get_anime_stream(
    request: Request, episode_id: str = Path(...), b64config: str = Path(...),
) -> Dict[str, List[Dict[str, Any]]]:
    logger.log("STREAM", f"Demande de flux pour: {episode_id}")
    config = validate_config(b64config)
    episode_id_formatted = episode_id.replace(".json", "")
    language_filter = config.get("language", "Tout")
    language_order = config.get("languageOrder", "VOSTFR,VF")
    try:
        streams = await stream_service.get_episode_streams(
            episode_id=episode_id_formatted, language_filter=language_filter,
            language_order=language_order, config=config,
        )
        logger.log("STREAM", f"{len(streams)} flux pour {episode_id}")
        return {"streams": streams}
    except Exception as e:
        logger.error(f"Erreur flux: {e}")
        return {"streams": []}

@main.get("/stream/anime/{episode_id}.json")
@main.get("/stream/series/{episode_id}.json")
@main.get("/stream/movie/{episode_id}.json")
async def stream_default(request: Request, episode_id: str = Path(...)):
    logger.log("STREAM", f"Demande de flux pour: {episode_id}")
    episode_id_formatted = episode_id.replace(".json", "")
    config = ConfigModel()
    try:
        streams = await stream_service.get_episode_streams(
            episode_id=episode_id_formatted, language_filter=config.language,
            language_order=config.languageOrder, config=config.model_dump(),
        )
        logger.log("STREAM", f"{len(streams)} flux pour {episode_id}")
        return {"streams": streams}
    except Exception as e:
        logger.error(f"Erreur flux: {e}")
        return {"streams": []}


# ===========================
# Simulcasts Adkami + skip
# ===========================
@main.get("/{b64config}/catalog/anime/adkami_simulcasts.json")
@main.get("/{b64config}/catalog/anime/adkami_simulcasts/skip={skip_val}.json")
async def catalog_adkami_simulcasts(
    request: Request, b64config: str = Path(...), skip_val: Optional[str] = None,
):
    try:
        config = ConfigModel(**validate_config(b64config))
        skip = _parse_skip(request, skip_val)
        return {"metas": catalog_service.get_simulcasts_catalog(request, b64config, config, skip=skip)}
    except Exception as e:
        logger.error(f"Erreur simulcasts: {e}")
        return {"metas": []}

@main.get("/catalog/anime/adkami_simulcasts.json")
@main.get("/catalog/anime/adkami_simulcasts/skip={skip_val}.json")
async def catalog_adkami_simulcasts_default(request: Request, skip_val: Optional[str] = None):
    try:
        skip = _parse_skip(request, skip_val)
        return {"metas": catalog_service.get_simulcasts_catalog(request, None, ConfigModel(), skip=skip)}
    except Exception as e:
        return {"metas": []}


# ===========================
# Catalogues par genre Adkami — catch-all adkami_genre_* + skip
# ===========================
@main.get("/{b64config}/catalog/anime/{catalog_id}.json")
@main.get("/{b64config}/catalog/anime/{catalog_id}/skip={skip_val}.json")
async def catalog_adkami_genre(
    request: Request,
    catalog_id: str = Path(...),
    b64config: str = Path(...),
    skip_val: Optional[str] = None,
):
    if not catalog_id.startswith("adkami_genre_"):
        raise HTTPException(status_code=404, detail=f"Catalogue inconnu: {catalog_id}")
    try:
        config = ConfigModel(**validate_config(b64config))
        skip = _parse_skip(request, skip_val)
        metas = catalog_service.get_genre_catalog(request, b64config, config, catalog_id, skip=skip)
        return {"metas": metas}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur genre {catalog_id}: {e}")
        return {"metas": []}

@main.get("/catalog/anime/{catalog_id}.json")
@main.get("/catalog/anime/{catalog_id}/skip={skip_val}.json")
async def catalog_adkami_genre_default(
    request: Request, catalog_id: str = Path(...), skip_val: Optional[str] = None,
):
    if not catalog_id.startswith("adkami_genre_"):
        raise HTTPException(status_code=404, detail=f"Catalogue inconnu: {catalog_id}")
    try:
        skip = _parse_skip(request, skip_val)
        metas = catalog_service.get_genre_catalog(request, None, ConfigModel(), catalog_id, skip=skip)
        return {"metas": metas}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur genre {catalog_id}: {e}")
        return {"metas": []}


# ===========================
# Miroirs /catalog/series/ et /catalog/movie/
# ===========================
@main.get("/{b64config}/catalog/series/{catalog_id}.json")
@main.get("/{b64config}/catalog/movie/{catalog_id}.json")
@main.get("/{b64config}/catalog/series/{catalog_id}/skip={skip_val}.json")
@main.get("/{b64config}/catalog/movie/{catalog_id}/skip={skip_val}.json")
async def catalog_by_type(
    request: Request,
    catalog_id: str = Path(...),
    b64config: str = Path(...),
    skip_val: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    try:
        config = ConfigModel(**validate_config(b64config))
        skip = _parse_skip(request, skip_val)
        metas = _dispatch_catalog(request, b64config, config, catalog_id, skip)
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalog {catalog_id}: {e}")
        return {"metas": []}

@main.get("/catalog/series/{catalog_id}.json")
@main.get("/catalog/movie/{catalog_id}.json")
@main.get("/catalog/series/{catalog_id}/skip={skip_val}.json")
@main.get("/catalog/movie/{catalog_id}/skip={skip_val}.json")
async def catalog_by_type_default(
    request: Request,
    catalog_id: str = Path(...),
    skip_val: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    try:
        config = ConfigModel()
        skip = _parse_skip(request, skip_val)
        metas = _dispatch_catalog(request, None, config, catalog_id, skip)
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalog default {catalog_id}: {e}")
        return {"metas": []}


def _dispatch_catalog(request, b64config, config, catalog_id: str, skip: int = 0):
    """Dispatch instantané vers le bon catalogue."""
    if catalog_id == "animesama_catalog":
        # Cas spécial : search est async
        return []
    if catalog_id == "adkami_simulcasts":
        return catalog_service.get_simulcasts_catalog(request, b64config, config, skip=skip)
    if catalog_id.startswith("adkami_genre_"):
        return catalog_service.get_genre_catalog(request, b64config, config, catalog_id, skip=skip)
    return []


# ===========================
# Test TMDB + Health
# ===========================
@main.get("/api/test-tmdb")
async def test_tmdb_key(key: str) -> Dict[str, Any]:
    if not key or len(key.strip()) < 10:
        return {"valid": False, "error": "Clé trop courte"}
    try:
        from astream.utils.http_client import http_client as _hc
        response = await _hc.get("https://api.themoviedb.org/3/configuration", params={"api_key": key.strip()})
        if response.status_code == 200:
            return {"valid": True, "error": None}
        elif response.status_code == 401:
            return {"valid": False, "error": "Clé API invalide"}
        else:
            return {"valid": False, "error": f"Erreur HTTP {response.status_code}"}
    except Exception as e:
        return {"valid": False, "error": str(e)}

@main.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}
