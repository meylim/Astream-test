from typing import Optional, Dict, Any
from databases import Database
from pydantic_settings import BaseSettings, SettingsConfigDict
import sys

# ===========================
# Domaines exclus
# ===========================
DEFAULT_EXCLUDED_DOMAINS = [
    "s22.anime-sama.fr",
    "vk.com",
    "vkvideo.ru",
    "moly.to",
    "vidmoly.net"
]

# ===========================
# Saisons spéciales
# ===========================
SEASON_TYPE_SPECIAL = 0        # Saison 0: Épisodes spéciaux/extras
SEASON_TYPE_FILM = 990          # Saison 990: Films (valeur haute pour tri en fin de liste)
SEASON_TYPE_OVA = 991           # Saison 991: OVA/OAV (valeur haute pour tri en fin de liste)
SPECIAL_SEASON_THRESHOLD = 900  # Seuil: saisons >= 900 sont considérées comme "spéciales" (films/OVA)

# ===========================
# Langues supportées
# ===========================
SUPPORTED_LANGUAGES = ["Tout", "VOSTFR", "VF"]
VALID_LANGUAGE_CODES = ["VOSTFR", "VF", "VF1", "VF2"]
LANGUAGES_TO_CHECK = ["vostfr", "vf", "vf1", "vf2"]

# ===========================
# TMDB
# ===========================
TMDB_ANIMATION_GENRE_ID = 16


# ===========================
# Classe AppSettings
# ===========================
class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    ANIMESAMA_URL: Optional[str] = None
    ANIMESAMA_AUTO_FETCH: Optional[bool] = False
    ADDON_ID: Optional[str] = "community.astream"
    ADDON_NAME: Optional[str] = "AStream"
    FASTAPI_HOST: Optional[str] = "0.0.0.0"
    FASTAPI_PORT: Optional[int] = 8000
    FASTAPI_WORKERS: Optional[int] = 1
    USE_GUNICORN: Optional[bool] = True
    DATABASE_TYPE: Optional[str] = "sqlite"
    DATABASE_URL: Optional[str] = "username:password@hostname:port"
    DATABASE_PATH: Optional[str] = "data/astream.db"
    DATASET_ENABLED: Optional[bool] = True
    DATASET_URL: Optional[str] = None
    DATASET_UPDATE_INTERVAL: Optional[int] = 3600
    EPISODE_TTL: Optional[int] = 3600
    DYNAMIC_LIST_TTL: Optional[int] = 3600
    PLANNING_TTL: Optional[int] = 3600
    ONGOING_ANIME_TTL: Optional[int] = 3600
    FINISHED_ANIME_TTL: Optional[int] = 604800
    SCRAPE_LOCK_TTL: Optional[int] = 300
    SCRAPE_WAIT_TIMEOUT: Optional[int] = 30
    HTTP_TIMEOUT: Optional[int] = 15
    PROXY_URL: Optional[str] = None
    EXCLUDED_DOMAINS: Optional[str] = ""
    CUSTOM_HEADER_HTML: Optional[str] = None
    LOG_LEVEL: Optional[str] = "DEBUG"
    TMDB_API_KEY: Optional[str] = None
    TMDB_TTL: Optional[int] = 604800
    JIKAN_TTL: Optional[int] = 3600


# ===========================
# Instance Singleton
# ===========================
settings = AppSettings()

# ===========================
# Validation de la configuration
# ===========================
if not settings.ANIMESAMA_URL:
    sys.stderr.write("ERREUR: ANIMESAMA_URL non configurée. Voir README: https://github.com/Dyhlio/astream#configuration\n")
    sys.exit(1)

if settings.ANIMESAMA_AUTO_FETCH:
    from astream.utils.domain_fetcher import fetch_animesama_domain_sync
    fetched = fetch_animesama_domain_sync(settings.ANIMESAMA_URL)
    if fetched:
        settings.ANIMESAMA_URL = fetched
        sys.stderr.write(f"INFO: Domaine récupéré automatiquement: {fetched}\n")
    else:
        sys.stderr.write("ERREUR: Impossible de récupérer le domaine depuis l'URL.\n")
        sys.exit(1)

if settings.ANIMESAMA_URL.endswith('/'):
    settings.ANIMESAMA_URL = settings.ANIMESAMA_URL.rstrip('/')

if not settings.ANIMESAMA_URL.startswith(('http://', 'https://')):
    settings.ANIMESAMA_URL = f"https://{settings.ANIMESAMA_URL}"

web_config = {
    "languages": {
        "Tout": "Tout afficher",
        "VOSTFR": "VOSTFR uniquement",
        "VF": "VF uniquement"
    },
    "tmdb": {
        "enabled": bool(settings.TMDB_API_KEY),
        "episode_mapping": False
    }
}


# ===========================
# Manifest Stremio de base
# ===========================
def get_base_manifest() -> Dict[str, Any]:
    """
    Retourne le manifest Stremio de base de l'addon.
    Ce manifest est ensuite personnalisé dans routes.py selon la config utilisateur.
    """
    return {
        "id": settings.ADDON_ID,
        "name": settings.ADDON_NAME,
        "description": f"{settings.ADDON_NAME} – Addon non officiel pour accéder au contenu d'Anime-Sama",
        "version": "3.1.0",
                "catalogs": [
            {
                "type": "series",
                "id": "animesama_catalog",
                "name": "🔍 Recherche Anime",
                "extra": [
                    {"name": "search", "isRequired": False},
                    {"name": "genre", "isRequired": False, "options": []},
                    {"name": "skip", "isRequired": False}
                ]
            },
            {
                "type": "series",
                "id": "adkami_simulcasts",
                "name": "📡 Simulcasts en cours",
                "extra": [{"name": "skip", "isRequired": False}]
            },
            {"type": "series", "id": "adkami_genre_action", "name": "⚔️ Action", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_amitie", "name": "🤝 Amitié", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_aventure", "name": "🗺️ Aventure", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_combat", "name": "🥊 Combat", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_comedie", "name": "😂 Comédie", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_contes_et_recits", "name": "📖 Contes et Récits", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_cyber_et_mecha", "name": "🤖 Cyber et Mecha", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_dark_fantasy", "name": "🌑 Dark Fantasy", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_drame", "name": "🎭 Drame", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_ecchi", "name": "🔥 Ecchi", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_educatif", "name": "📚 Educatif", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_enigme_et_policier", "name": "🔍 Enigme et Policier", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_epique_et_heroique", "name": "🦸 Épique et Héroïque", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_espace_et_sci_fiction", "name": "🚀 Espace et Sci-Fiction", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_familial_et_jeunesse", "name": "👨‍👩‍👧 Familial et Jeunesse", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_fantastique_et_mythe", "name": "🧙 Fantastique et Mythe", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_fantasy", "name": "✨ Fantasy", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_gastronomie", "name": "🍜 Gastronomie", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_gender_bender", "name": "🔄 Gender Bender", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_gyaru", "name": "💅 Gyaru", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_harem", "name": "💕 Harem", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_historique", "name": "⛩️ Historique", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_horreur", "name": "😱 Horreur", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_idols", "name": "🎤 Idols", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_inceste", "name": "⚠️ Inceste", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_isekai", "name": "🌀 Isekai", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_magical_girl", "name": "🪄 Magical Girl", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_magie", "name": "🎩 Magie", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_mature", "name": "🔞 Mature", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_moe", "name": "🥰 Moe", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_monster_girl", "name": "👾 Monster Girl", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_musical", "name": "🎵 Musical", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_mystere", "name": "❓ Mystère", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_psychologique", "name": "🧠 Psychologique", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_romance", "name": "💗 Romance", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_school_life", "name": "🏫 School Life", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_sport", "name": "⚽ Sport", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_surnaturel", "name": "👻 Surnaturel", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_survival_game", "name": "🎯 Survival Game", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_thriller", "name": "😰 Thriller", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_tokusatsu", "name": "🦹 Tokusatsu", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_tranche_de_vie", "name": "☀️ Tranche de vie", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_triangle_amoureux", "name": "💔 Triangle Amoureux", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_yaoi", "name": "👬 Yaoi", "extra": [{"name": "skip", "isRequired": False}]},
            {"type": "series", "id": "adkami_genre_yuri", "name": "👭 Yuri", "extra": [{"name": "skip", "isRequired": False}]},
        ],
        "resources": [
            "catalog",
            {
                "name": "meta",
                "types": ["anime"],
                "idPrefixes": ["as", "jikan"]
            },
            {
                "name": "stream",
                "types": ["series", "movie", "anime"],
                "idPrefixes": ["tt", "kitsu", "as", "jikan", "tmdb"]
            }
        ],
        "types": ["movie", "series", "anime"],
        "logo": "https://raw.githubusercontent.com/Dyhlio/astream/refs/heads/main/astream/public/astream-logo.jpg",
        "background": "https://raw.githubusercontent.com/Dyhlio/astream/refs/heads/main/astream/public/astream-background.png",
        "behaviorHints": {"configurable": True, "configurationRequired": False},
    }


# ===========================
# Configuration de la base de données
# ===========================
database_url = settings.DATABASE_PATH if settings.DATABASE_TYPE == "sqlite" else settings.DATABASE_URL
database = Database(f"{'sqlite' if settings.DATABASE_TYPE == 'sqlite' else 'postgresql'}://{'/' if settings.DATABASE_TYPE == 'sqlite' else ''}{database_url}")
    
