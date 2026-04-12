"""
Adkami Catalog Loader — Pré-charge TOUT, résout vers Cinemeta, valide via Kitsu.

Architecture :
  1. Au __init__ : charge TOUS les JSON Adkami en mémoire
  2. background_resolve_all() (appelé au startup) :
     - Pour chaque titre Adkami → search Cinemeta (search_raw, sans Kitsu)
     - Pour chaque résultat Cinemeta → vérifie via Kitsu texte que c'est un anime
     - Premier résultat qui passe titre-match + Kitsu → on garde le tt*
     - Construit _ready_catalogs[genre] = [meta, meta, ...] prêt à servir
  3. get_genre_catalog(genre, skip, limit) : slice instantané depuis la mémoire
  4. Cache de résolution persisté dans resolution_cache.json
"""
import json
import os
import re
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from astream.utils.logger import logger
from astream.services.kitsu.validator import kitsu_validator

CATALOGUES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "catalogues")
RESOLUTION_CACHE_FILE = os.path.join(CATALOGUES_DIR, "resolution_cache.json")

# Limite de résultats par page Stremio
PAGE_SIZE = 50

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


def _normalize(text: str) -> str:
    """Clé normalisée pour comparaison et cache."""
    if not text:
        return ""
    return re.sub(r'[^a-z0-9]', '', text.lower())


def _words(text: str) -> set:
    return set(re.sub(r'[^a-z0-9]', ' ', text.lower()).split())


# ===========================
# Vérification Kitsu rapide (par texte uniquement)
# ===========================
async def _kitsu_is_anime(adkami_query: str, cinemeta_name: str) -> bool:
    """
    Vérifie qu'un titre correspond à un anime sur Kitsu.
    adkami_query : le titre original recherché.
    cinemeta_name : le résultat renvoyé par Cinemeta.
    """
    from astream.utils.http_client import http_client, safe_json_decode

    # On prépare le terme de recherche comme dans anim2.py
    search_term = kitsu_validator.prepare_search_term(adkami_query, cinemeta_name)
    
    # Si le validateur renvoie None, c'est qu'un parasite a été détecté
    if not search_term:
        return False

    safe_q = search_term.replace(" ", "%20")
    url = f"https://kitsu.io/api/edge/anime?filter[text]={safe_q}&page[limit]=5"

    try:
        resp = await http_client.get(url, headers={"Accept": "application/vnd.api+json"})
        if resp.status_code != 200:
            return False
            
        data = safe_json_decode(resp, f"Kitsu check {cinemeta_name}", default=None)
        if not data:
            return False

        for anime in data.get("data", []):
            attr = anime.get("attributes", {})
            subtype = str(attr.get("subtype", "")).lower()
            
            # Rejet des formats music/special
            if subtype in ("music", "special"):
                continue

            # Comparaison avec les différents titres Kitsu
            k_titles = [
                attr.get("canonicalTitle"),
                (attr.get("titles") or {}).get("en"),
                (attr.get("titles") or {}).get("en_jp"),
                (attr.get("slug") or "").replace("-", " "),
            ]

            for kt in k_titles:
                # MATCH AVANCÉ ICI
                if kt and kitsu_validator.check_advanced_match(search_term, kt):
                    return True

        return False
    except Exception:
        # En cas d'erreur réseau, on accepte par défaut
        return True


# ===========================
# Classe principale
# ===========================
class AdkamiCatalogLoader:

    def __init__(self):
        # JSON bruts chargés en mémoire : {genre: [raw_items]}
        self._raw_cache: Dict[str, List[Dict]] = {}
        self._simulcast_raw: List[Dict] = []

        # Cache de résolution persisté : {norm_title: {tt_id, name, ...} | {_not_found: True}}
        self._resolution_cache: Dict[str, Dict] = {}

        # Catalogues prêts à servir : {genre: [stremio_meta, ...]}
        self._ready_catalogs: Dict[str, List[Dict]] = {}
        self._ready_simulcasts: List[Dict] = []

        # État
        self._init_done = False
        self._resolving = False

        # Charger tout au démarrage
        self._load_all_json()
        self._load_resolution_cache()

    # ===========================
    # Chargement initial — TOUT en mémoire
    # ===========================
    def _load_all_json(self):
        """Charge tous les JSON Adkami d'un coup."""
        total = 0
        for genre_name in ADKAMI_CATEGORIES:
            filepath = os.path.join(CATALOGUES_DIR, f"final_{genre_name.lower()}.json")
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        items = json.load(f)
                    self._raw_cache[genre_name] = items
                    total += len(items)
                except Exception as e:
                    logger.error(f"ADKAMI: Erreur lecture final_{genre_name.lower()}.json: {e}")
                    self._raw_cache[genre_name] = []
            else:
                self._raw_cache[genre_name] = []

        # Simulcasts
        sim_path = os.path.join(CATALOGUES_DIR, "simulcast.json")
        if os.path.exists(sim_path):
            try:
                with open(sim_path, 'r', encoding='utf-8') as f:
                    self._simulcast_raw = json.load(f)
            except Exception as e:
                logger.error(f"ADKAMI: Erreur lecture simulcast.json: {e}")
                self._simulcast_raw = []

        logger.log("ADKAMI", f"Chargement terminé: {len(self._raw_cache)} genres, "
                   f"{total} items total, {len(self._simulcast_raw)} simulcasts")

    def _load_resolution_cache(self):
        if os.path.exists(RESOLUTION_CACHE_FILE):
            try:
                with open(RESOLUTION_CACHE_FILE, 'r', encoding='utf-8') as f:
                    self._resolution_cache = json.load(f)
                logger.log("ADKAMI", f"Cache résolution: {len(self._resolution_cache)} entrées")
            except Exception as e:
                logger.error(f"ADKAMI: Erreur lecture cache: {e}")

    def _save_resolution_cache(self):
        try:
            os.makedirs(os.path.dirname(RESOLUTION_CACHE_FILE), exist_ok=True)
            with open(RESOLUTION_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self._resolution_cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"ADKAMI: Erreur sauvegarde cache: {e}")

    # ===========================
    # Résolution titre → tt* (Cinemeta + Kitsu check)
    # ===========================
    async def _resolve_title(self, title: str) -> Optional[Dict]:
        """
        Résout un titre Adkami :
          1. Cherche sur Cinemeta (search_raw, SANS Kitsu intégré)
          2. Pour chaque résultat Cinemeta, vérifie titre-match
          3. Vérifie via Kitsu texte que c'est bien un anime
          4. Premier qui passe les 2 checks → on garde
        """
        from astream.services.cinemeta.client import cinemeta_client

        key = _normalize(title)
        if key in self._resolution_cache:
            cached = self._resolution_cache[key]
            if cached.get("_not_found"):
                return None
            return cached

        try:
            results = await cinemeta_client.search_raw(title, limit=10)
            if not results:
                self._resolution_cache[key] = {"_not_found": True}
                return None

            # Filtrer par pertinence de titre d'abord
            candidates = self._rank_candidates(title, results)

            for candidate in candidates:
                cinemeta_name = candidate.get("name", "")
                cinemeta_id = candidate.get("id", "")

                # On passe le `title` (la recherche initiale) ET `cinemeta_name`
                is_anime = await _kitsu_is_anime(title, cinemeta_name)
                
                if is_anime:
                    resolved = {
                        "tt_id": cinemeta_id,
                        "name": cinemeta_name,
                        "type": candidate.get("type", "series"),
                        "poster": candidate.get("poster", ""),
                        "background": candidate.get("background", ""),
                        "description": candidate.get("description", ""),
                        "releaseInfo": candidate.get("releaseInfo", ""),
                        "imdbRating": candidate.get("imdbRating", ""),
                        "runtime": candidate.get("runtime", ""),
                        "genres": candidate.get("genres", []),
                    }
                    self._resolution_cache[key] = resolved
                    logger.debug(f"ADKAMI ✅ '{title}' → {cinemeta_id} ({cinemeta_name})")
                    return resolved
                else:
                    logger.debug(f"ADKAMI ❌ '{title}' candidat '{cinemeta_name}' rejeté Kitsu")

            # Aucun candidat validé
            self._resolution_cache[key] = {"_not_found": True}
            logger.debug(f"ADKAMI: Aucun match anime pour '{title}'")
            return None

        except Exception as e:
            logger.warning(f"ADKAMI: Résolution échouée pour '{title}': {e}")
            return None

    def _rank_candidates(self, query: str, results: List[Dict]) -> List[Dict]:
        """
        Trie les résultats Cinemeta par pertinence.
        Retourne seulement ceux avec un score de match acceptable.
        """
        query_norm = _normalize(query)
        q_words = _words(query)
        scored = []

        for r in results:
            name = r.get("name", "")
            name_norm = _normalize(name)
            r_words = _words(name)

            # Score de match
            if name_norm == query_norm:
                score = 100  # Match exact
            elif query_norm in name_norm or name_norm in query_norm:
                score = 80   # L'un contient l'autre
            else:
                common = len(q_words & r_words)
                pct = common / max(len(q_words), 1)
                if pct >= 0.5:
                    score = int(pct * 60)
                else:
                    continue  # Pas assez proche, on skip

            scored.append((score, r))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [r for _, r in scored]

    # ===========================
    # Construction meta Stremio
    # ===========================
    @staticmethod
    def _build_meta(resolved: Dict, adkami_item: Dict) -> Dict:
        """Construit une meta Stremio depuis les données résolues + Adkami."""
        meta = {
            "id": resolved.get("tt_id", ""),
            "type": resolved.get("type", "series"),
            "name": resolved.get("name", adkami_item.get("titre_affiche", "")),
            "posterShape": "poster",
        }
        poster = resolved.get("poster") or adkami_item.get("image_url", "")
        if poster:
            meta["poster"] = poster
        for field in ("background", "description", "releaseInfo", "runtime"):
            val = resolved.get(field)
            if val:
                meta[field] = val

        imdb_rating = resolved.get("imdbRating", "")
        if not imdb_rating and adkami_item.get("score_mal"):
            imdb_rating = str(adkami_item["score_mal"])
        if imdb_rating:
            meta["imdbRating"] = imdb_rating

        genres = resolved.get("genres", [])
        if genres:
            meta["genres"] = genres
        return meta

    # ===========================
    # Construction catalogues prêts à servir
    # ===========================
    def _build_ready_catalog(self, genre_name: str) -> List[Dict]:
        """Construit le catalogue prêt à servir pour un genre, depuis le cache de résolution."""
        raw_items = self._raw_cache.get(genre_name, [])
        metas = []
        seen_titles = set()
        seen_tt = set()

        for item in raw_items:
            title = item.get("titre_affiche", "").strip()
            if not title:
                continue
            norm = _normalize(title)
            if norm in seen_titles:
                continue
            seen_titles.add(norm)

            cached = self._resolution_cache.get(norm)
            if not cached or cached.get("_not_found"):
                continue

            tt_id = cached.get("tt_id", "")
            if not tt_id or tt_id in seen_tt:
                continue
            seen_tt.add(tt_id)

            metas.append(self._build_meta(cached, item))

        return metas

    def _rebuild_all_ready_catalogs(self):
        """Reconstruit tous les catalogues prêts à servir depuis le cache."""
        for genre_name in ADKAMI_CATEGORIES:
            self._ready_catalogs[genre_name] = self._build_ready_catalog(genre_name)

        # Simulcasts
        metas = []
        seen_tt = set()
        seen_norm = set()
        for item in self._simulcast_raw:
            title = item.get("titre_affiche", "").strip()
            if not title:
                continue
            norm = _normalize(title)
            if norm in seen_norm:
                continue
            seen_norm.add(norm)

            cached = self._resolution_cache.get(norm)
            if not cached or cached.get("_not_found"):
                continue
            tt_id = cached.get("tt_id", "")
            if not tt_id or tt_id in seen_tt:
                continue
            seen_tt.add(tt_id)
            metas.append(self._build_meta(cached, item))

        self._ready_simulcasts = metas

        total_ready = sum(len(v) for v in self._ready_catalogs.values())
        logger.log("ADKAMI", f"Catalogues prêts: {total_ready} metas genre + "
                   f"{len(self._ready_simulcasts)} simulcasts")

    # ===========================
    # API publique — service instantané
    # ===========================
    def get_genre_catalog(self, genre_name: str, skip: int = 0, limit: int = PAGE_SIZE) -> List[Dict]:
        """Retourne une page du catalogue genre. Instantané depuis la mémoire."""
        if genre_name not in ADKAMI_CATEGORIES:
            return []
        catalog = self._ready_catalogs.get(genre_name, [])
        return catalog[skip:skip + limit]

    def get_simulcasts(self, skip: int = 0, limit: int = PAGE_SIZE) -> List[Dict]:
        """Retourne une page des simulcasts. Instantané."""
        return self._ready_simulcasts[skip:skip + limit]

    def get_all_genres(self) -> List[str]:
        return list(ADKAMI_CATEGORIES.keys())

    # ===========================
    # Résolution complète au démarrage
    # ===========================
    async def background_resolve_all(self):
        """
        Résout TOUS les titres Adkami au démarrage :
          1. Collecte tous les titres uniques
          2. Filtre ceux déjà en cache
          3. Résout par vagues (3 en parallèle, rate-limité)
          4. Construit les catalogues prêts à servir
        """
        if self._resolving:
            return
        self._resolving = True

        try:
            # --- Étape 1 : Collecter tous les titres uniques ---
            all_titles = set()
            for genre_name, items in self._raw_cache.items():
                for item in items:
                    t = item.get("titre_affiche", "").strip()
                    if t:
                        all_titles.add(t)
            for item in self._simulcast_raw:
                t = item.get("titre_affiche", "").strip()
                if t:
                    all_titles.add(t)

            # --- Étape 2 : Filtrer ceux déjà résolus ---
            unresolved = [t for t in all_titles if _normalize(t) not in self._resolution_cache]

            logger.log("ADKAMI", f"🚀 INIT: {len(all_titles)} titres uniques, "
                       f"{len(all_titles) - len(unresolved)} déjà en cache, "
                       f"{len(unresolved)} à résoudre")

            # Construire les catalogues depuis le cache existant immédiatement
            self._rebuild_all_ready_catalogs()
            self._init_done = True
            logger.log("ADKAMI", "📦 Catalogues initiaux construits (depuis cache existant)")

            if not unresolved:
                logger.log("ADKAMI", "✅ Tout est déjà résolu, prêt à servir !")
                self._resolving = False
                return

            # --- Étape 3 : Résoudre par vagues ---
            sem = asyncio.Semaphore(3)
            resolved_ok = 0
            resolved_fail = 0

            for i in range(0, len(unresolved), 10):
                batch = unresolved[i:i + 10]

                async def _do(title):
                    async with sem:
                        return await self._resolve_title(title)

                results = await asyncio.gather(*[_do(t) for t in batch], return_exceptions=True)

                for r in results:
                    if isinstance(r, Exception):
                        resolved_fail += 1
                    elif r is not None:
                        resolved_ok += 1
                    else:
                        resolved_fail += 1

                # Sauvegarde et rebuild réguliers
                progress = i + len(batch)
                if progress % 50 == 0 or progress >= len(unresolved):
                    self._save_resolution_cache()
                    self._rebuild_all_ready_catalogs()
                    logger.log("ADKAMI", f"⏳ Résolution: {progress}/{len(unresolved)} "
                               f"({resolved_ok} ✅ / {resolved_fail} ❌)")

                # Rate limiting Cinemeta + Kitsu
                await asyncio.sleep(1.0)

            # --- Étape 4 : Sauvegarde finale ---
            self._save_resolution_cache()
            self._rebuild_all_ready_catalogs()
            logger.log("ADKAMI", f"🎉 Résolution terminée: {resolved_ok} ✅ / {resolved_fail} ❌ "
                       f"sur {len(unresolved)} titres")

        except Exception as e:
            logger.error(f"ADKAMI: Erreur background_resolve_all: {e}")
        finally:
            self._resolving = False


# ===========================
# Instance Singleton Globale
# ===========================
adkami_loader = AdkamiCatalogLoader()
