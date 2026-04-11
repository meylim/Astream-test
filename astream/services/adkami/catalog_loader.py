"""
Adkami Catalog Loader — Charge les catalogues JSON + résolution Cinemeta.

Flux :
  1. Charge les titres Adkami depuis les fichiers final_*.json / simulcast.json
  2. Résout chaque titre vers un tt* IMDb via Cinemeta + validation Kitsu
  3. Cache la résolution dans resolution_cache.json pour ne pas refaire les appels
  4. Sert des metas Stremio avec ID tt* (Cinemeta délègue les fiches)
"""
import json
import os
import re
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from astream.utils.logger import logger

# Chemin vers le dossier catalogues (relatif au module)
CATALOGUES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "catalogues")
RESOLUTION_CACHE_FILE = os.path.join(CATALOGUES_DIR, "resolution_cache.json")

# ===========================
# 45 catégories Adkami
# ===========================
ADKAMI_CATEGORIES: Dict[str, Dict[str, Any]] = {
    "Action":               {"id": 1,  "emoji": "⚔️"},
    "Amitie":               {"id": 3,  "emoji": "🤝"},
    "Aventure":             {"id": 2,  "emoji": "🗺️"},
    "Combat":               {"id": 4,  "emoji": "🥊"},
    "Comedie":              {"id": 5,  "emoji": "😂"},
    "Contes_et_Recits":     {"id": 6,  "emoji": "📖"},
    "Cyber_et_Mecha":       {"id": 7,  "emoji": "🤖"},
    "Dark_Fantasy":         {"id": 8,  "emoji": "🌑"},
    "Drame":                {"id": 9,  "emoji": "🎭"},
    "Ecchi":                {"id": 10, "emoji": "🔥"},
    "Educatif":             {"id": 11, "emoji": "📚"},
    "Enigme_et_Policier":   {"id": 12, "emoji": "🔍"},
    "Epique_et_Heroique":   {"id": 13, "emoji": "🦸"},
    "Espace_et_Sci_Fiction":{"id": 14, "emoji": "🚀"},
    "Familial_et_Jeunesse": {"id": 15, "emoji": "👨‍👩‍👧"},
    "Fantastique_et_Mythe": {"id": 16, "emoji": "🧙"},
    "Fantasy":              {"id": 30, "emoji": "✨"},
    "Gastronomie":          {"id": 39, "emoji": "🍜"},
    "Gender_Bender":        {"id": 61, "emoji": "🔄"},
    "Gyaru":                {"id": 70, "emoji": "💅"},
    "Harem":                {"id": 32, "emoji": "💕"},
    "Historique":           {"id": 18, "emoji": "⛩️"},
    "Horreur":              {"id": 19, "emoji": "😱"},
    "Idols":                {"id": 38, "emoji": "🎤"},
    "Inceste":              {"id": 36, "emoji": "⚠️"},
    "Isekai":               {"id": 42, "emoji": "🌀"},
    "Magical_Girl":         {"id": 20, "emoji": "🪄"},
    "Magie":                {"id": 43, "emoji": "🎩"},
    "Mature":               {"id": 26, "emoji": "🔞"},
    "Moe":                  {"id": 25, "emoji": "🥰"},
    "Monster_Girl":         {"id": 71, "emoji": "👾"},
    "Musical":              {"id": 21, "emoji": "🎵"},
    "Mystere":              {"id": 31, "emoji": "❓"},
    "Psychologique":        {"id": 22, "emoji": "🧠"},
    "Romance":              {"id": 34, "emoji": "💗"},
    "School_Life":          {"id": 29, "emoji": "🏫"},
    "Sport":                {"id": 23, "emoji": "⚽"},
    "Surnaturel":           {"id": 33, "emoji": "👻"},
    "Survival_Game":        {"id": 40, "emoji": "🎯"},
    "Thriller":             {"id": 35, "emoji": "😰"},
    "Tokusatsu":            {"id": 41, "emoji": "🦹"},
    "Tranche_de_vie":       {"id": 24, "emoji": "☀️"},
    "Triangle_Amoureux":    {"id": 37, "emoji": "💔"},
    "Yaoi":                 {"id": 27, "emoji": "👬"},
    "Yuri":                 {"id": 28, "emoji": "👭"},
}


def _normalize_key(title: str) -> str:
    """Clé de cache normalisée."""
    if not title:
        return ""
    return re.sub(r'[^a-z0-9]', '', title.lower())


class AdkamiCatalogLoader:
    """
    Charge les catalogues Adkami et sert des metas résolues vers Cinemeta (tt*).
    """

    def __init__(self):
        # Cache en mémoire : {genre_name: [raw_adkami_items]}
        self._raw_cache: Dict[str, List[Dict]] = {}
        self._simulcast_raw: Optional[List[Dict]] = None

        # Cache de résolution : {normalized_title: {tt_id, name, poster, type, ...}}
        self._resolution_cache: Dict[str, Dict] = {}
        self._load_resolution_cache()

        # Sémaphore pour limiter les appels parallèles de résolution
        self._resolve_semaphore: Optional[asyncio.Semaphore] = None

        # Flag : résolution en cours
        self._resolving = False

    # ===========================
    # Chargement / Sauvegarde du cache de résolution
    # ===========================
    def _load_resolution_cache(self):
        if os.path.exists(RESOLUTION_CACHE_FILE):
            try:
                with open(RESOLUTION_CACHE_FILE, 'r', encoding='utf-8') as f:
                    self._resolution_cache = json.load(f)
                logger.log("ADKAMI", f"Cache résolution chargé: {len(self._resolution_cache)} entrées")
            except Exception as e:
                logger.error(f"ADKAMI: Erreur lecture cache résolution: {e}")
                self._resolution_cache = {}
        else:
            self._resolution_cache = {}

    def _save_resolution_cache(self):
        try:
            os.makedirs(os.path.dirname(RESOLUTION_CACHE_FILE), exist_ok=True)
            with open(RESOLUTION_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self._resolution_cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"ADKAMI: Erreur sauvegarde cache: {e}")

    # ===========================
    # Chargement des fichiers JSON Adkami
    # ===========================
    def _load_json(self, filename: str) -> List[Dict]:
        filepath = os.path.join(CATALOGUES_DIR, filename)
        if not os.path.exists(filepath):
            logger.warning(f"ADKAMI: Fichier introuvable: {filepath}")
            return []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"ADKAMI: Erreur lecture {filename}: {e}")
            return []

    def _get_raw_genre(self, genre_name: str) -> List[Dict]:
        """Charge les items bruts d'une catégorie (avec cache mémoire)."""
        if genre_name in self._raw_cache:
            return self._raw_cache[genre_name]
        filename = f"final_{genre_name.lower()}.json"
        items = self._load_json(filename)
        self._raw_cache[genre_name] = items
        return items

    def _get_raw_simulcasts(self) -> List[Dict]:
        if self._simulcast_raw is not None:
            return self._simulcast_raw
        self._simulcast_raw = self._load_json("simulcast.json")
        return self._simulcast_raw

    # ===========================
    # Résolution titre → tt* via Cinemeta + Kitsu
    # ===========================
    async def _resolve_title(self, title: str) -> Optional[Dict]:
        """
        Résout un titre Adkami en meta Cinemeta (tt*).
        Utilise le CinemetaClient existant (qui fait Cinemeta + Kitsu validation).
        Retourne la première correspondance validée ou None.
        """
        from astream.services.cinemeta.client import cinemeta_client

        key = _normalize_key(title)
        if key in self._resolution_cache:
            cached = self._resolution_cache[key]
            if cached.get("_not_found"):
                return None
            return cached

        try:
            results = await cinemeta_client.search(title, limit=1)
            if results:
                best = results[0]
                resolved = {
                    "tt_id": best.get("id", ""),
                    "name": best.get("name", title),
                    "type": best.get("type", "series"),
                    "poster": best.get("poster", ""),
                    "background": best.get("background", ""),
                    "description": best.get("description", ""),
                    "releaseInfo": best.get("releaseInfo", ""),
                    "imdbRating": best.get("imdbRating", ""),
                    "runtime": best.get("runtime", ""),
                    "genres": best.get("genres", []),
                }
                self._resolution_cache[key] = resolved
                return resolved
            else:
                # Marquer comme introuvable pour ne pas re-chercher
                self._resolution_cache[key] = {"_not_found": True}
                return None
        except Exception as e:
            logger.warning(f"ADKAMI: Résolution échouée pour '{title}': {e}")
            return None

    async def resolve_items(self, raw_items: List[Dict], limit: int = 50) -> List[Dict]:
        """
        Résout une liste d'items Adkami vers des metas Cinemeta (tt*).
        Retourne uniquement les items résolus.
        """
        if self._resolve_semaphore is None:
            self._resolve_semaphore = asyncio.Semaphore(3)

        # Dédupliquer par titre normalisé
        seen_keys = set()
        unique_items = []
        for item in raw_items:
            key = _normalize_key(item.get("titre_affiche", ""))
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            unique_items.append(item)

        # Séparer les déjà résolus des à résoudre
        resolved_metas = []
        to_resolve = []

        for item in unique_items:
            title = item.get("titre_affiche", "").strip()
            if not title:
                continue
            key = _normalize_key(title)
            cached = self._resolution_cache.get(key)
            if cached and not cached.get("_not_found"):
                resolved_metas.append(self._build_stremio_meta(cached, item))
            elif cached and cached.get("_not_found"):
                continue  # Déjà tenté, introuvable
            else:
                to_resolve.append(item)

            if len(resolved_metas) >= limit and not to_resolve:
                break

        # Résoudre les manquants (en parallèle, limité)
        if to_resolve:
            needed = limit - len(resolved_metas)
            batch = to_resolve[:max(needed * 2, 20)]  # Résoudre un peu plus que nécessaire

            async def _resolve_one(item):
                async with self._resolve_semaphore:
                    title = item.get("titre_affiche", "").strip()
                    resolved = await self._resolve_title(title)
                    if resolved:
                        return self._build_stremio_meta(resolved, item)
                    return None

            results = await asyncio.gather(
                *[_resolve_one(it) for it in batch],
                return_exceptions=True
            )

            for r in results:
                if isinstance(r, Exception) or r is None:
                    continue
                resolved_metas.append(r)

            # Sauvegarder le cache après résolution
            self._save_resolution_cache()

        # Dédupliquer par tt_id
        seen_tt = set()
        final = []
        for meta in resolved_metas:
            tt_id = meta.get("id", "")
            if tt_id in seen_tt:
                continue
            seen_tt.add(tt_id)
            final.append(meta)
            if len(final) >= limit:
                break

        return final

    def _build_stremio_meta(self, resolved: Dict, adkami_item: Dict) -> Dict:
        """Construit une meta Stremio à partir des données résolues + Adkami."""
        tt_id = resolved.get("tt_id", "")
        media_type = resolved.get("type", "series")

        meta = {
            "id": tt_id,
            "type": media_type,
            "name": resolved.get("name", adkami_item.get("titre_affiche", "")),
            "posterShape": "poster",
        }

        poster = resolved.get("poster") or adkami_item.get("image_url", "")
        if poster:
            meta["poster"] = poster

        background = resolved.get("background", "")
        if background:
            meta["background"] = background

        description = resolved.get("description", "")
        if description:
            meta["description"] = description

        release_info = resolved.get("releaseInfo", "")
        if release_info:
            meta["releaseInfo"] = release_info

        imdb_rating = resolved.get("imdbRating", "")
        if not imdb_rating and adkami_item.get("score_mal"):
            imdb_rating = str(adkami_item["score_mal"])
        if imdb_rating:
            meta["imdbRating"] = imdb_rating

        runtime = resolved.get("runtime", "")
        if runtime:
            meta["runtime"] = runtime

        genres = resolved.get("genres", [])
        if genres:
            meta["genres"] = genres

        return meta

    # ===========================
    # API publique
    # ===========================
    async def get_genre_catalog(self, genre_name: str, limit: int = 50) -> List[Dict]:
        """Retourne les metas résolues pour une catégorie Adkami."""
        if genre_name not in ADKAMI_CATEGORIES:
            logger.warning(f"ADKAMI: Catégorie inconnue: {genre_name}")
            return []
        raw_items = self._get_raw_genre(genre_name)
        return await self.resolve_items(raw_items, limit=limit)

    async def get_simulcasts(self, limit: int = 50) -> List[Dict]:
        """Retourne les simulcasts résolus."""
        raw_items = self._get_raw_simulcasts()
        return await self.resolve_items(raw_items, limit=limit)

    def get_all_genres(self) -> List[str]:
        """Retourne la liste des catégories pour le manifest."""
        return list(ADKAMI_CATEGORIES.keys())

    # ===========================
    # Résolution en arrière-plan (appelée au startup)
    # ===========================
    async def background_resolve_all(self):
        """
        Résout tous les titres Adkami en arrière-plan.
        Appelée une fois au démarrage pour pré-remplir le cache.
        """
        if self._resolving:
            return
        self._resolving = True

        logger.log("ADKAMI", "Début de la résolution en arrière-plan...")
        all_titles = set()

        # Collecter tous les titres uniques
        for genre_name in ADKAMI_CATEGORIES:
            for item in self._get_raw_genre(genre_name):
                title = item.get("titre_affiche", "").strip()
                if title:
                    all_titles.add(title)

        for item in self._get_raw_simulcasts():
            title = item.get("titre_affiche", "").strip()
            if title:
                all_titles.add(title)

        # Filtrer ceux déjà résolus
        unresolved = []
        for title in all_titles:
            key = _normalize_key(title)
            if key not in self._resolution_cache:
                unresolved.append(title)

        logger.log("ADKAMI", f"{len(all_titles)} titres uniques, {len(unresolved)} à résoudre")

        if not unresolved:
            self._resolving = False
            return

        if self._resolve_semaphore is None:
            self._resolve_semaphore = asyncio.Semaphore(3)

        # Résoudre par vagues
        resolved_count = 0
        for i in range(0, len(unresolved), 10):
            batch = unresolved[i:i+10]

            async def _do(title):
                async with self._resolve_semaphore:
                    return await self._resolve_title(title)

            await asyncio.gather(*[_do(t) for t in batch], return_exceptions=True)
            resolved_count += len(batch)

            # Sauvegarde régulière
            if resolved_count % 50 == 0:
                self._save_resolution_cache()
                logger.log("ADKAMI", f"Résolution: {resolved_count}/{len(unresolved)} traités")

            # Rate limiting
            await asyncio.sleep(1.5)

        self._save_resolution_cache()
        logger.log("ADKAMI", f"Résolution terminée: {resolved_count} titres traités")
        self._resolving = False

    def clear_cache(self):
        self._raw_cache.clear()
        self._simulcast_raw = None


# ===========================
# Instance Singleton Globale
# ===========================
adkami_loader = AdkamiCatalogLoader()
