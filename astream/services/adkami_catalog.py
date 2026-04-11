"""
AdkamiCatalogService — Pont entre les JSON Adkami et le pipeline AStream.

Rôle :
  - Charge les catalogues JSON construits par le scraper Adkami
  - Convertit les entrées au format interne AStream (compatible pipeline TMDB + Kitsu)
  - Expose get_genre_catalog() et get_simulcast_catalog()
  - Le mapping genre_slug → catalog_id est centralisé ici

Format interne AStream attendu par _pipeline() (catalog.py) :
  {
    "_meta_id":          str,   # "jikan:{mal_id}" ou fallback URL
    "slug":              str,   # slug virtuel
    "_is_jikan":         bool,  # True → pas de résolution Anime-Sama
    "mal_id":            int|None,
    "title":             str,
    "genres":            list[str],
    "poster":            str,
    "mal_score":         float|None,
    ...
  }
"""

from typing import Dict, List, Any, Optional

from astream.utils.logger import logger
from astream.scrapers.adkami.scraper import (
    ADKAMI_GENRES,
    load_genre_catalog,
    load_simulcast_catalog,
)

# ===========================
# Mapping : nom genre Adkami → nom d'affichage FR
# ===========================
ADKAMI_GENRE_DISPLAY: Dict[str, str] = {
    "Action":               "Action",
    "Amitie":               "Amitié",
    "Aventure":             "Aventure",
    "Combat":               "Combat",
    "Comedie":              "Comédie",
    "Contes_et_Recits":     "Contes & Récits",
    "Cyber_et_Mecha":       "Cyber & Mecha",
    "Dark_Fantasy":         "Dark Fantasy",
    "Drame":                "Drame",
    "Ecchi":                "Ecchi",
    "Educatif":             "Éducatif",
    "Enigme_et_Policier":   "Énigme & Policier",
    "Epique_et_Heroique":   "Épique & Héroïque",
    "Espace_et_Sci_Fiction":"Espace & Sci-Fi",
    "Familial_et_Jeunesse": "Familial & Jeunesse",
    "Fantastique_et_Mythe": "Fantastique & Mythe",
    "Fantasy":              "Fantasy",
    "Gastronomie":          "Gastronomie",
    "Gender_Bender":        "Gender Bender",
    "Gyaru":                "Gyaru",
    "Harem":                "Harem",
    "Historique":           "Historique",
    "Horreur":              "Horreur",
    "Idols":                "Idols",
    "Inceste":              "Inceste",
    "Isekai":               "Isekai",
    "Magical_Girl":         "Magical Girl",
    "Magie":                "Magie",
    "Mature":               "Mature",
    "Moe":                  "Moé",
    "Monster_Girl":         "Monster Girl",
    "Musical":              "Musical",
    "Mystere":              "Mystère",
    "Psychologique":        "Psychologique",
    "Romance":              "Romance",
    "School_Life":          "School Life",
    "Sport":                "Sport",
    "Surnaturel":           "Surnaturel",
    "Survival_Game":        "Survival Game",
    "Thriller":             "Thriller",
    "Tokusatsu":            "Tokusatsu",
    "Tranche_de_vie":       "Tranche de vie",
    "Triangle_Amoureux":    "Triangle Amoureux",
    "Yaoi":                 "Yaoi",
    "Yuri":                 "Yuri",
}

# ===========================
# Mapping catalog_id → nom genre Adkami
# catalog_id = "adkami_{slug_lower}"
# ===========================
ADKAMI_CATALOG_MAP: Dict[str, str] = {
    f"adkami_{slug.lower()}": slug
    for slug in ADKAMI_GENRES.keys()
}

# Liste des genres à exposer dans le manifest (ordre d'affichage)
ADKAMI_MANIFEST_GENRES: List[str] = [
    ADKAMI_GENRE_DISPLAY[slug]
    for slug in [
        "Action", "Aventure", "Comedie", "Drame", "Fantasy",
        "Romance", "Surnaturel", "Horreur", "Psychologique",
        "Isekai", "Historique", "School_Life", "Ecchi", "Harem",
        "Moe", "Sport", "Mystere", "Thriller", "Tranche_de_vie",
        "Magical_Girl", "Cyber_et_Mecha", "Dark_Fantasy", "Combat",
        "Musical", "Magie", "Yuri", "Yaoi", "Survival_Game",
        "Gastronomie", "Gender_Bender", "Espace_et_Sci_Fiction",
        "Fantastique_et_Mythe", "Epique_et_Heroique", "Amitie",
    ]
]


# ===========================
# Conversion Adkami → AStream interne
# ===========================

def _adkami_entry_to_astream(entry: Dict[str, Any], genre_slug: str) -> Optional[Dict[str, Any]]:
    """
    Convertit une entrée Adkami (issue du JSON) en dictionnaire interne AStream
    compatible avec le pipeline catalog.py (_pipeline → TMDB → build_metas).
    """
    titre = (entry.get("titre_affiche") or entry.get("titre_recherche") or "").strip()
    if not titre:
        return None

    mal_id: Optional[int] = entry.get("mal_id")
    image_url: str = entry.get("image_url") or ""
    score: Optional[float] = entry.get("score_mal")
    display_genre = ADKAMI_GENRE_DISPLAY.get(genre_slug, genre_slug)

    # Identifiant Stremio
    if mal_id:
        meta_id = f"jikan:{mal_id}"
        slug = f"jikan-{mal_id}"
    else:
        # Fallback basé sur le titre
        safe_title = titre.lower().replace(" ", "-").replace("/", "-")
        meta_id = f"adkami:{safe_title}"
        slug = f"adkami-{safe_title}"

    return {
        "_meta_id":        meta_id,
        "slug":            slug,
        "_is_jikan":       True,
        "mal_id":          mal_id,
        "title":           titre,
        "genres":          [display_genre],
        "poster":          image_url,
        "image":           image_url,
        "mal_score":       score,
        "description":     "",
        "synopsis":        "",
        "year":            "",
        "runtime":         "",
        "status":          "",
        "type":            "TV",
    }


def _adkami_list_to_astream(entries: List[Dict[str, Any]], genre_slug: str) -> List[Dict[str, Any]]:
    """Convertit une liste d'entrées Adkami en format interne AStream."""
    result = []
    seen_mal_ids: set = set()

    for entry in entries:
        item = _adkami_entry_to_astream(entry, genre_slug)
        if not item:
            continue

        # Déduplication par mal_id
        mid = item.get("mal_id")
        if mid:
            if mid in seen_mal_ids:
                continue
            seen_mal_ids.add(mid)

        result.append(item)

    return result


# ===========================
# Service principal
# ===========================

class AdkamiCatalogService:
    """
    Service qui lit les catalogues Adkami (JSON) et les fournit
    au format interne AStream pour les routes /catalog/*.
    """

    def get_genre_by_display_name(self, display_name: str) -> Optional[str]:
        """Retourne le slug genre Adkami à partir du nom d'affichage."""
        for slug, name in ADKAMI_GENRE_DISPLAY.items():
            if name == display_name:
                return slug
        return None

    def get_genre_catalog(self, genre_slug: str, limit: int = 25) -> List[Dict[str, Any]]:
        """Charge et convertit le catalogue d'un genre Adkami."""
        entries = load_genre_catalog(genre_slug)
        if not entries:
            logger.warning(f"ADKAMI: Catalogue vide pour genre '{genre_slug}'")
            return []

        items = _adkami_list_to_astream(entries[:limit * 2], genre_slug)
        logger.log("ADKAMI", f"get_genre_catalog({genre_slug}): {len(items)} items chargés")
        return items[:limit]

    def get_simulcast_catalog(self, limit: int = 25) -> List[Dict[str, Any]]:
        """Charge et convertit le catalogue simulcast Adkami."""
        entries = load_simulcast_catalog()
        if not entries:
            logger.warning("ADKAMI: Catalogue simulcast vide")
            return []

        items = _adkami_list_to_astream(entries[:limit * 2], "Simulcast")
        logger.log("ADKAMI", f"get_simulcast_catalog(): {len(items)} items chargés")
        return items[:limit]

    def get_manifest_genres(self) -> List[str]:
        """Retourne les genres à exposer dans le manifest Stremio."""
        return list(ADKAMI_MANIFEST_GENRES)

    def get_catalog_id_for_genre(self, genre_slug: str) -> str:
        """Retourne le catalog_id Stremio pour un slug genre Adkami."""
        return f"adkami_{genre_slug.lower()}"

    def search_in_catalogs(self, query: str, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Recherche locale dans tous les catalogues JSON déjà construits.
        Utilisé comme fallback si Cinemeta ne répond pas.
        """
        q = query.lower().strip()
        results: List[Dict[str, Any]] = []
        seen: set = set()

        for slug in ADKAMI_GENRES.keys():
            entries = load_genre_catalog(slug)
            for entry in entries:
                titre = (entry.get("titre_recherche") or "").lower()
                if q in titre:
                    item = _adkami_entry_to_astream(entry, slug)
                    if item:
                        mid = item.get("mal_id") or item.get("slug")
                        if mid not in seen:
                            seen.add(mid)
                            results.append(item)
                            if len(results) >= limit:
                                return results

        return results


# ===========================
# Instance Singleton
# ===========================
adkami_catalog_service = AdkamiCatalogService()
