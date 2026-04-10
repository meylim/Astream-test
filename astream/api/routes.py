from typing import Dict, List, Any, Optional
from fastapi import APIRouter, Request, Path
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


# ===========================
# Routeur et Templates
# ===========================
templates = Jinja2Templates("astream/public")
main = APIRouter()


# ===========================
# Points de terminaison Web
# ===========================
@main.get("/", summary="Accueil", description="Redirige vers la page de configuration")
async def root() -> RedirectResponse:
    return RedirectResponse("/configure")


@main.get("/configure", summary="Configuration", description="Interface web pour configurer l'addon")
async def configure(request: Request) -> Any:
    config_data = dict(web_config)
    config_data["ADDON_NAME"] = settings.ADDON_NAME
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "CUSTOM_HEADER_HTML": settings.CUSTOM_HEADER_HTML or "",
            "EXCLUDED_DOMAINS": get_all_excluded_domains(),
            "webConfig": config_data,
        },
    )


@main.get("/{b64config}/configure", summary="Reconfiguration", description="Modifier une configuration existante")
async def configure_addon(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
) -> Any:
    config_data = dict(web_config)
    config_data["ADDON_NAME"] = settings.ADDON_NAME
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "CUSTOM_HEADER_HTML": settings.CUSTOM_HEADER_HTML or "",
            "EXCLUDED_DOMAINS": get_all_excluded_domains(),
            "webConfig": config_data,
        },
    )


# ===========================
# Points de terminaison Stremio
# ===========================
@main.get("/{b64config}/manifest.json", summary="Manifeste Stremio", description="Retourne les métadonnées de l'addon pour l'installation avec genres dynamiques")
async def manifest(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
) -> Dict[str, Any]:
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
        logger.log("API", f"MANIFEST - Ajout de {len(unique_genres)} options de genre depuis le catalogue")
    except Exception as e:
        logger.error(f"MANIFEST - Echec de l'extraction des genres: {e}")

    return base_manifest


@main.get("/{b64config}/catalog/anime/animesama_catalog.json", summary="Catalogue d'anime", description="Retourne le catalogue d'anime avec recherche, filtrage par genre et langue, enrichissement TMDB")
@main.get("/{b64config}/catalog/anime/animesama_catalog/search={search}.json", summary="Recherche d'anime", description="Recherche d'anime par titre avec configuration")
@main.get("/{b64config}/catalog/anime/animesama_catalog/genre={genre}.json", summary="Filtrage par genre", description="Filtre le catalogue par genre avec configuration")
@main.get("/{b64config}/catalog/anime/animesama_catalog/search={search}&genre={genre}.json", summary="Recherche et filtrage", description="Recherche d'anime par titre et genre avec configuration")
# Miroirs /catalog/series/ pour Stremio
@main.get("/{b64config}/catalog/series/animesama_catalog.json")
@main.get("/{b64config}/catalog/series/animesama_catalog/search={search}.json")
@main.get("/{b64config}/catalog/series/animesama_catalog/genre={genre}.json")
@main.get("/{b64config}/catalog/series/animesama_catalog/search={search}&genre={genre}.json")
async def animesama_catalog(
    request: Request,
    b64config: Optional[str] = None,
    search: Optional[str] = None,
    genre: Optional[str] = None
) -> Dict[str, List[Dict[str, Any]]]:
    try:
        if not search and "search" in request.query_params:
            search = request.query_params.get("search")
        if not genre and "genre" in request.query_params:
            genre = request.query_params.get("genre")

        config_dict = validate_config(b64config)
        config = ConfigModel(**config_dict)

        metas = await catalog_service.get_complete_catalog(
            request=request,
            b64config=b64config,
            search=search,
            genre=genre,
            config=config
        )

        return {"metas": metas}

    except Exception as e:
        logger.error(f"Erreur dans le catalogue: {e}")
        return {"metas": []}


@main.get("/{b64config}/meta/anime/{id}.json", summary="Métadonnées d'anime", description="Retourne les métadonnées complètes de l'anime avec liste d'épisodes et enrichissement TMDB")
async def animesama_meta(
    request: Request,
    id: str = Path(..., description="Identifiant d'anime (format: as:slug)"),
    b64config: str = Path(..., description="Configuration encodée en base64")
) -> Dict[str, Any]:
    config_dict = validate_config(b64config)
    config = ConfigModel(**config_dict)

    meta = await metadata_service.get_complete_anime_meta(
        anime_id=id,
        config=config,
        request=request,
        b64config=b64config
    )

    return {"meta": meta}


@main.get("/{b64config}/stream/anime/{episode_id}.json", summary="Obtenir les flux", description="Retourne les flux vidéo disponibles pour l'épisode demandé avec fusion dataset + scraping et filtrage de langue")
@main.get("/{b64config}/stream/series/{episode_id}.json", summary="Obtenir les flux (series)", description="Flux pour IDs IMDb/Kitsu - type series")
@main.get("/{b64config}/stream/movie/{episode_id}.json", summary="Obtenir les flux (movie)", description="Flux pour IDs IMDb/Kitsu - type movie")
async def get_anime_stream(
    request: Request,
    episode_id: str = Path(..., description="Identifiant d'épisode (format: as:slug:s1e1)"),
    b64config: str = Path(..., description="Configuration encodée en base64")
) -> Dict[str, List[Dict[str, Any]]]:
    logger.log("STREAM", f"Demande de flux pour: {episode_id}")

    config = validate_config(b64config)
    episode_id_formatted = episode_id.replace(".json", "")
    language_filter = config.get("language", "Tout")
    language_order = config.get("languageOrder", "VOSTFR,VF")

    try:
        streams = await stream_service.get_episode_streams(
            episode_id=episode_id_formatted,
            language_filter=language_filter,
            language_order=language_order,
            config=config
        )

        logger.log("STREAM", f"{len(streams)} flux trouvés pour {episode_id}")
        return {"streams": streams}

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des flux: {e}")
        return {"streams": []}


# ===========================
# Catalogues spéciaux
# ===========================
@main.get("/{b64config}/catalog/anime/animesama_en_cours.json", summary="Catalogue En cours")
async def catalog_en_cours(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
) -> Dict[str, List[Dict[str, Any]]]:
    try:
        config_dict = validate_config(b64config)
        config = ConfigModel(**config_dict)
        metas = await catalog_service.get_en_cours_catalog(request, b64config, config)
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalogue en cours: {e}")
        return {"metas": []}


@main.get("/{b64config}/catalog/anime/animesama_nouveautes.json", summary="Catalogue Nouveautés")
async def catalog_nouveautes(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
) -> Dict[str, List[Dict[str, Any]]]:
    try:
        config_dict = validate_config(b64config)
        config = ConfigModel(**config_dict)
        metas = await catalog_service.get_nouveautes_catalog(request, b64config, config)
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalogue nouveautés: {e}")
        return {"metas": []}


# ===========================
# Test clé TMDB
# ===========================
@main.get("/api/test-tmdb", summary="Test clé API TMDB")
async def test_tmdb_key(key: str) -> Dict[str, Any]:
    """Valide une clé API TMDB en effectuant un appel test vers l'API."""
    if not key or len(key.strip()) < 10:
        return {"valid": False, "error": "Clé trop courte"}
    try:
        test_url = "https://api.themoviedb.org/3/configuration"
        from astream.utils.http_client import http_client as _hc
        response = await _hc.get(test_url, params={"api_key": key.strip()})
        if response.status_code == 200:
            return {"valid": True, "error": None}
        elif response.status_code == 401:
            return {"valid": False, "error": "Clé API invalide"}
        else:
            return {"valid": False, "error": f"Erreur HTTP {response.status_code}"}
    except Exception as e:
        return {"valid": False, "error": str(e)}


@main.get("/manifest.json", summary="Manifeste Stremio", description="Retourne les métadonnées de l'addon pour l'installation avec genres dynamiques")
async def manifest_default(request: Request) -> Dict[str, Any]:
    base_manifest = get_base_manifest()

    base_manifest["name"] = "| AStream"
    base_manifest["description"] = (
        f"CONFIGURATION OBSELETE, VEUILLEZ RECONFIGURER SUR {request.url.scheme}://{request.url.netloc}"
    )

    try:
        unique_genres = await catalog_service.extract_unique_genres()
        base_manifest["catalogs"][0]["extra"][1]["options"] = unique_genres
        logger.log("API", f"MANIFEST - Ajout de {len(unique_genres)} options de genre depuis le catalogue")
    except Exception as e:
        logger.error(f"MANIFEST - Echec de l'extraction des genres: {e}")

    return base_manifest


@main.get("/catalog/anime/animesama_catalog.json", summary="Catalogue d'anime", description="Retourne le catalogue d'anime avec recherche, filtrage par genre et langue, enrichissement TMDB")
@main.get("/catalog/anime/animesama_catalog/search={search}.json", summary="Recherche d'anime", description="Recherche d'anime par titre")
@main.get("/catalog/anime/animesama_catalog/genre={genre}.json", summary="Filtrage par genre", description="Filtre le catalogue par genre")
@main.get("/catalog/anime/animesama_catalog/search={search}&genre={genre}.json", summary="Recherche et filtrage", description="Recherche d'anime par titre et genre")
# Miroirs /catalog/series/ (Stremio utilise le type déclaré dans le manifest)
@main.get("/catalog/series/animesama_catalog.json")
@main.get("/catalog/series/animesama_catalog/search={search}.json")
@main.get("/catalog/series/animesama_catalog/genre={genre}.json")
@main.get("/catalog/series/animesama_catalog/search={search}&genre={genre}.json")
async def catalog_default(request: Request) -> Dict[str, List[Dict[str, Any]]]:
    try:
        search = request.query_params.get("search")
        genre = request.query_params.get("genre")

        config = ConfigModel()

        metas = await catalog_service.get_complete_catalog(
            request=request,
            b64config=None,
            search=search,
            genre=genre,
            config=config
        )

        return {"metas": metas}

    except Exception as e:
        logger.error(f"Erreur dans le catalogue: {e}")
        return {"metas": []}


@main.get("/meta/anime/{id}.json", summary="Métadonnées d'anime", description="Retourne les métadonnées complètes de l'anime avec liste d'épisodes et enrichissement TMDB")
async def meta_default(
    request: Request,
    id: str = Path(..., description="Identifiant d'anime (format: as:slug)")
) -> Dict[str, Any]:
    config = ConfigModel()

    meta = await metadata_service.get_complete_anime_meta(
        anime_id=id,
        config=config,
        request=request,
        b64config=None
    )

    return {"meta": meta}


@main.get("/stream/anime/{episode_id}.json", summary="Obtenir les flux", description="Retourne les flux vidéo disponibles pour l'épisode demandé avec fusion dataset + scraping et filtrage de langue")
@main.get("/stream/series/{episode_id}.json", summary="Obtenir les flux (series)", description="Flux pour IDs IMDb/Kitsu - type series")
@main.get("/stream/movie/{episode_id}.json", summary="Obtenir les flux (movie)", description="Flux pour IDs IMDb/Kitsu - type movie")
async def stream_default(
    request: Request,
    episode_id: str = Path(..., description="Identifiant d'épisode (format: as:slug:s1e1)")
) -> Dict[str, List[Dict[str, Any]]]:
    logger.log("STREAM", f"Demande de flux pour: {episode_id}")

    episode_id_formatted = episode_id.replace(".json", "")
    config = ConfigModel()

    try:
        streams = await stream_service.get_episode_streams(
            episode_id=episode_id_formatted,
            language_filter=config.language,
            language_order=config.languageOrder,
            config=config.model_dump()
        )

        logger.log("STREAM", f"{len(streams)} flux trouvés pour {episode_id}")
        return {"streams": streams}

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des flux: {e}")
        return {"streams": []}


@main.get("/catalog/anime/animesama_en_cours.json", summary="Catalogue En cours (défaut)")
async def catalog_en_cours_default(request: Request) -> Dict[str, List[Dict[str, Any]]]:
    try:
        config = ConfigModel()
        metas = await catalog_service.get_en_cours_catalog(request, None, config)
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalogue en cours: {e}")
        return {"metas": []}


@main.get("/catalog/anime/animesama_nouveautes.json", summary="Catalogue Nouveautés (défaut)")
async def catalog_nouveautes_default(request: Request) -> Dict[str, List[Dict[str, Any]]]:
    try:
        config = ConfigModel()
        metas = await catalog_service.get_nouveautes_catalog(request, None, config)
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalogue nouveautés: {e}")
        return {"metas": []}


@main.get("/{b64config}/catalog/anime/animesama_sorties_du_jour.json", summary="Catalogue Sorties du jour")
async def catalog_sorties_du_jour(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
) -> Dict[str, List[Dict[str, Any]]]:
    try:
        config_dict = validate_config(b64config)
        config = ConfigModel(**config_dict)
        metas = await catalog_service.get_sorties_du_jour_catalog(request, b64config, config)
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalogue sorties du jour: {e}")
        return {"metas": []}


@main.get("/catalog/anime/animesama_sorties_du_jour.json", summary="Catalogue Sorties du jour (défaut)")
async def catalog_sorties_du_jour_default(request: Request) -> Dict[str, List[Dict[str, Any]]]:
    try:
        config = ConfigModel()
        metas = await catalog_service.get_sorties_du_jour_catalog(request, None, config)
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalogue sorties du jour: {e}")
        return {"metas": []}


# ===========================
# Catalogues Jikan — Découverte & Home
# ===========================

def _make_catalog_route(get_fn_name: str):
    """Helper pour générer des paires de routes (avec/sans b64config)."""
    pass


@main.get("/{b64config}/catalog/anime/jikan_simulcasts.json", summary="Simulcasts en cours")
async def catalog_jikan_simulcasts(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
):
    try:
        config = ConfigModel(**validate_config(b64config))
        return {"metas": await catalog_service.get_simulcasts_catalog(request, b64config, config)}
    except Exception as e:
        logger.error(f"Erreur simulcasts: {e}")
        return {"metas": []}

@main.get("/catalog/anime/jikan_simulcasts.json", summary="Simulcasts (défaut)")
async def catalog_jikan_simulcasts_default(request: Request):
    try:
        return {"metas": await catalog_service.get_simulcasts_catalog(request, None, ConfigModel())}
    except Exception as e:
        return {"metas": []}


@main.get("/{b64config}/catalog/anime/jikan_films.json", summary="Films d'anime")
async def catalog_jikan_films(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
):
    try:
        config = ConfigModel(**validate_config(b64config))
        return {"metas": await catalog_service.get_films_catalog(request, b64config, config)}
    except Exception as e:
        logger.error(f"Erreur films: {e}")
        return {"metas": []}

@main.get("/catalog/anime/jikan_films.json", summary="Films (défaut)")
async def catalog_jikan_films_default(request: Request):
    try:
        return {"metas": await catalog_service.get_films_catalog(request, None, ConfigModel())}
    except Exception as e:
        return {"metas": []}


@main.get("/{b64config}/catalog/anime/jikan_top.json", summary="Top Anime")
async def catalog_jikan_top(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
):
    try:
        config = ConfigModel(**validate_config(b64config))
        return {"metas": await catalog_service.get_top_anime_catalog(request, b64config, config)}
    except Exception as e:
        logger.error(f"Erreur top: {e}")
        return {"metas": []}

@main.get("/catalog/anime/jikan_top.json", summary="Top (défaut)")
async def catalog_jikan_top_default(request: Request):
    try:
        return {"metas": await catalog_service.get_top_anime_catalog(request, None, ConfigModel())}
    except Exception as e:
        return {"metas": []}


@main.get("/{b64config}/catalog/anime/jikan_sorties_du_jour.json", summary="Sorties du jour")
async def catalog_jikan_sorties(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
):
    try:
        config = ConfigModel(**validate_config(b64config))
        return {"metas": await catalog_service.get_sorties_du_jour_catalog(request, b64config, config)}
    except Exception as e:
        logger.error(f"Erreur sorties du jour: {e}")
        return {"metas": []}

@main.get("/catalog/anime/jikan_sorties_du_jour.json", summary="Sorties du jour (défaut)")
async def catalog_jikan_sorties_default(request: Request):
    try:
        return {"metas": await catalog_service.get_sorties_du_jour_catalog(request, None, ConfigModel())}
    except Exception as e:
        return {"metas": []}


@main.get("/{b64config}/catalog/anime/jikan_saison.json", summary="Saison en cours")
async def catalog_jikan_saison(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
):
    try:
        config = ConfigModel(**validate_config(b64config))
        return {"metas": await catalog_service.get_season_now_catalog(request, b64config, config)}
    except Exception as e:
        logger.error(f"Erreur saison: {e}")
        return {"metas": []}

@main.get("/catalog/anime/jikan_saison.json", summary="Saison en cours (défaut)")
async def catalog_jikan_saison_default(request: Request):
    try:
        return {"metas": await catalog_service.get_season_now_catalog(request, None, ConfigModel())}
    except Exception as e:
        return {"metas": []}


@main.get("/{b64config}/catalog/anime/jikan_prochaine_saison.json", summary="Prochaine saison")
async def catalog_jikan_prochaine_saison(
    request: Request,
    b64config: str = Path(..., description="Configuration encodée en base64")
):
    try:
        config = ConfigModel(**validate_config(b64config))
        return {"metas": await catalog_service.get_season_upcoming_catalog(request, b64config, config)}
    except Exception as e:
        logger.error(f"Erreur prochaine saison: {e}")
        return {"metas": []}

@main.get("/catalog/anime/jikan_prochaine_saison.json", summary="Prochaine saison (défaut)")
async def catalog_jikan_prochaine_saison_default(request: Request):
    try:
        return {"metas": await catalog_service.get_season_upcoming_catalog(request, None, ConfigModel())}
    except Exception as e:
        return {"metas": []}


# ===========================
# Catalogues par genre — route générique (catch-all jikan_genre_*)
# Un seul handler couvre les 16 genres du manifest
# ===========================
@main.get("/{b64config}/catalog/anime/{catalog_id}.json", summary="Catalogue genre Jikan")
async def catalog_jikan_genre(
    request: Request,
    catalog_id: str = Path(..., description="ID du catalogue genre (ex: jikan_genre_action)"),
    b64config: str = Path(..., description="Configuration encodée en base64"),
):
    """
    Route générique pour tous les catalogues jikan_genre_*.
    Doit être déclarée APRÈS les routes spécifiques pour ne pas les masquer.
    """
    if not catalog_id.startswith("jikan_genre_"):
        raise HTTPException(status_code=404, detail=f"Catalogue inconnu: {catalog_id}")
    try:
        config = ConfigModel(**validate_config(b64config))
        metas = await catalog_service.get_genre_catalog(request, b64config, config, catalog_id)
        return {"metas": metas}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur genre catalog {catalog_id}: {e}")
        return {"metas": []}


@main.get("/catalog/anime/{catalog_id}.json", summary="Catalogue genre Jikan (défaut)")
async def catalog_jikan_genre_default(
    request: Request,
    catalog_id: str = Path(..., description="ID du catalogue genre"),
):
    if not catalog_id.startswith("jikan_genre_"):
        raise HTTPException(status_code=404, detail=f"Catalogue inconnu: {catalog_id}")
    try:
        metas = await catalog_service.get_genre_catalog(request, None, ConfigModel(), catalog_id)
        return {"metas": metas}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur genre catalog {catalog_id}: {e}")
        return {"metas": []}



# ===========================
# Miroirs /catalog/series/ et /catalog/movie/
# Stremio appelle ces URLs selon le type déclaré dans le manifest.
# On redirige vers les mêmes handlers que /catalog/anime/.
# ===========================
@main.get("/{b64config}/catalog/series/{catalog_id}.json")
@main.get("/{b64config}/catalog/movie/{catalog_id}.json")
async def catalog_by_type(
    request: Request,
    catalog_id: str = Path(...),
    b64config: str = Path(...),
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Route générique pour /catalog/series/ et /catalog/movie/.
    Redirige vers le bon service selon le catalog_id.
    """
    try:
        config = ConfigModel(**validate_config(b64config))
        metas = await _dispatch_catalog(request, b64config, config, catalog_id)
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalog {catalog_id}: {e}")
        return {"metas": []}


@main.get("/catalog/series/{catalog_id}.json")
@main.get("/catalog/movie/{catalog_id}.json")
async def catalog_by_type_default(
    request: Request,
    catalog_id: str = Path(...),
) -> Dict[str, List[Dict[str, Any]]]:
    try:
        config = ConfigModel()
        metas = await _dispatch_catalog(request, None, config, catalog_id)
        return {"metas": metas}
    except Exception as e:
        logger.error(f"Erreur catalog default {catalog_id}: {e}")
        return {"metas": []}


async def _dispatch_catalog(request, b64config, config, catalog_id: str):
    """Dispatch vers le bon handler de catalogue selon l'ID."""
    dispatch = {
        "animesama_catalog":        catalog_service.get_complete_catalog,
        "jikan_sorties_du_jour":    lambda r, b, c: catalog_service.get_sorties_du_jour_catalog(r, b, c),
        "jikan_simulcasts":         lambda r, b, c: catalog_service.get_simulcasts_catalog(r, b, c),
        "jikan_saison":             lambda r, b, c: catalog_service.get_season_now_catalog(r, b, c),
        "jikan_top":                lambda r, b, c: catalog_service.get_top_anime_catalog(r, b, c),
        "jikan_films":              lambda r, b, c: catalog_service.get_films_catalog(r, b, c),
        "jikan_prochaine_saison":   lambda r, b, c: catalog_service.get_season_upcoming_catalog(r, b, c),
        "animesama_en_cours":       lambda r, b, c: catalog_service.get_en_cours_catalog(r, b, c),
        "animesama_nouveautes":     lambda r, b, c: catalog_service.get_nouveautes_catalog(r, b, c),
        "animesama_sorties_du_jour":lambda r, b, c: catalog_service.get_sorties_du_jour_catalog(r, b, c),
    }
    if catalog_id in dispatch:
        fn = dispatch[catalog_id]
        if catalog_id == "animesama_catalog":
            return await fn(request=request, b64config=b64config, config=config)
        return await fn(request, b64config, config)
    if catalog_id.startswith("jikan_genre_"):
        return await catalog_service.get_genre_catalog(request, b64config, config, catalog_id)
    return []


@main.get("/health", summary="État de santé", description="Retourne l'état de santé actuel du service")
async def health() -> Dict[str, str]:
    return {"status": "ok"}
