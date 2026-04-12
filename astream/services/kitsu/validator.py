import re
from typing import List, Optional

class KitsuValidator:
    # --- RÈGLE 3 : PARASITES ---
    PARASITES = ["reaction", "abridged", "fan made", "review", "blind wave", "vostfr", "vf", "live action", "trailer"]
    
    # --- RÈGLE 4 : NETTOYAGE DU TERME DE RECHERCHE ---
    CLEANING_REGEX = re.compile(r'(?i)\s*(the movie|movie|film|part|cour|season|series|version|memories|special).*')

    @staticmethod
    def normalize_text(text: str) -> str:
        """Nettoyage pour comparaison textuelle (minuscules, pas de ponctuation)."""
        if not text: return ""
        return re.sub(r'[^a-z0-9]', '', text.lower())

    @staticmethod
    def get_best_segment(full_title: str, query: str) -> str:
        """--- RÈGLE 2 : SEGMENTATION --- 
        Sépare par ':' et choisit la partie la plus proche de la recherche."""
        if ":" not in full_title:
            return full_title
        
        norm_query = KitsuValidator.normalize_text(query)
        if norm_query in KitsuValidator.normalize_text(full_title):
            return full_title
            
        query_words = set(query.lower().split())
        segments = full_title.split(":")
        best_part = max(segments, key=lambda s: len(set(s.lower().split()) & query_words))
        return best_part.strip()

    @classmethod
    def prepare_search_term(cls, original_query: str, cinemeta_title: str) -> Optional[str]:
        """
        Applique tous les filtres préliminaires de anim2.py.
        Retourne le terme nettoyé, ou None si le résultat doit être rejeté d'office.
        """
        query_clean = original_query.lower().strip()
        
        # --- RÈGLE 1 : LONGUEUR --- (Réintégrée depuis anim2.py)
        if len(cinemeta_title.replace(" ", "")) < len(query_clean.replace(" ", "")):
            return None # "Refus Auto : Titre trop court"
            
        # --- RÈGLE 3 : PARASITES ---
        if any(p in cinemeta_title.lower() for p in cls.PARASITES):
            return None # "Rejeté : Contenu parasite détecté"
            
        # --- RÈGLE 2 : SEGMENTATION ---
        target_segment = cls.get_best_segment(cinemeta_title, query_clean)
        
        # --- RÈGLE 4 : NETTOYAGE REGEX ---
        clean_search = cls.CLEANING_REGEX.sub('', target_segment).strip()
        return clean_search

    @staticmethod
    def check_order_and_position(c_words: List[str], k_words: List[str], min_matches: int) -> bool:
        """Vérifie l'ordre strict et l'écart absolu des index (max 50% de la taille de Kitsu)."""
        matched_c_words = [w for w in c_words if w in k_words]
        if len(matched_c_words) < min_matches:
            return False
            
        last_idx_k = -1
        last_idx_c = -1
        actual_matches = []
        max_allowed_gap = len(k_words) * 0.5
        
        for w in matched_c_words:
            try:
                idx_k = k_words.index(w, last_idx_k + 1)
                idx_c = c_words.index(w, last_idx_c + 1)
                actual_matches.append((idx_c, idx_k))
                last_idx_k = idx_k
                last_idx_c = idx_c
            except ValueError:
                continue
                
        if len(actual_matches) < min_matches:
            return False
            
        for idx_c, idx_k in actual_matches:
            if abs(idx_k - idx_c) >= max_allowed_gap:
                return False
        return True

    @classmethod
    def check_advanced_match(cls, search_term: str, kitsu_title: str) -> bool:
        """--- RÈGLE 6 : LOGIQUE DE MATCH AVANCÉE ---
        Nouvelle règle de comparaison stricte basée sur les mots, l'ordre et l'écart."""
        if not search_term or not kitsu_title:
            return False
            
        c_words = re.sub(r'[^a-z0-9]', ' ', search_term.lower()).split()
        k_words = re.sub(r'[^a-z0-9]', ' ', kitsu_title.lower()).split()
        
        if not c_words or not k_words:
            return False
            
        len_c, len_k = len(c_words), len(k_words)
        
        if len_c == 1:
            return c_words == k_words
        if len_c < 3 or len_k < 3:
            return cls.check_order_and_position(c_words, k_words, min_matches=len_c)
        return cls.check_order_and_position(c_words, k_words, min_matches=3)

kitsu_validator = KitsuValidator()
