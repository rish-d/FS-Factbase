import os
import pickle
import numpy as np
import duckdb
from sentence_transformers import SentenceTransformer, util
from loguru import logger
import db_config

class LocalSemanticMatcher:
    def __init__(self, model_name='all-MiniLM-L6-v2'):
        logger.info(f"Loading embedding model: {model_name}...")
        self.model = SentenceTransformer(model_name)
        self.db_path = db_config.get_db_path()
        self.vector_cache_path = os.path.join(os.path.dirname(__file__), "ifrs_vectors.pkl")
        self.concept_vectors = {} # role -> {concept_id: vector}
        self.concept_labels = {}  # role -> {concept_id: label}

    def refresh_cache(self):
        """Fetches all concepts from DB and computes embeddings, grouped by role."""
        logger.info("Refreshing local semantic cache from IFRS dictionary...")
        conn = duckdb.connect(self.db_path)
        
        # We fetch concept IDs, labels, and their statement roles
        query = """
            SELECT metric_id, standardized_metric_name, statement_role 
            FROM Core_Metrics
        """
        rows = conn.execute(query).fetchall()
        conn.close()

        # Group concepts by role to support partitioned search
        role_groups = {}
        for metric_id, label, role in rows:
            role = role or "universal"
            if role not in role_groups:
                role_groups[role] = []
            role_groups[role].append((metric_id, label))

        new_cache = {"vectors": {}, "labels": {}}
        
        for role, concepts in role_groups.items():
            logger.info(f"Computing embeddings for role: {role} ({len(concepts)} concepts)...")
            ids = [c[0] for c in concepts]
            labels = [c[1] for c in concepts]
            
            embeddings = self.model.encode(labels, convert_to_tensor=True)
            
            new_cache["vectors"][role] = embeddings
            new_cache["labels"][role] = list(zip(ids, labels))

        with open(self.vector_cache_path, 'wb') as f:
            pickle.dump(new_cache, f)
        
        self.concept_vectors = new_cache["vectors"]
        self.concept_labels = new_cache["labels"]
        logger.success("Semantic cache refreshed and saved.")

    def load_cache(self):
        """Loads vectors from disk if available, otherwise refreshes."""
        if os.path.exists(self.vector_cache_path):
            try:
                with open(self.vector_cache_path, 'rb') as f:
                    cache = pickle.load(f)
                self.concept_vectors = cache["vectors"]
                self.concept_labels = cache["labels"]
                logger.info("Loaded semantic cache from disk.")
            except Exception as e:
                logger.error(f"Failed to load cache: {e}. Refreshing...")
                self.refresh_cache()
        else:
            self.refresh_cache()

    def map_term(self, raw_term, statement_type=None, threshold=0.85):
        """
        Finds the best semantic match for a term within a specific statement context.
        statement_type: e.g., 'Balance Sheet', 'Income Statement'
        """
        if not self.concept_vectors:
            self.load_cache()

        # Map statement_type to IFRS roles
        role_map = {
            "balance sheet": "210000",
            "statement of financial position": "210000",
            "income statement": "310000",
            "profit or loss": "310000",
            "cash flow": "610000",
            "equity": "510000",
            "oci": "410000"
        }
        
        target_role = "universal"
        if statement_type:
            for key, role in role_map.items():
                if key in statement_type.lower():
                    target_role = role
                    break

        # If we have vectors for this role, search them. Otherwise fallback to universal.
        search_roles = [target_role]
        if target_role != "universal":
            search_roles.append("universal")

        best_score = 0
        best_match = None

        query_vector = self.model.encode(raw_term, convert_to_tensor=True)

        for role in search_roles:
            if role not in self.concept_vectors:
                continue
                
            vectors = self.concept_vectors[role]
            labels_info = self.concept_labels[role] # List of (id, label)
            
            # Compute cosine similarity
            cos_scores = util.cos_sim(query_vector, vectors)[0]
            
            top_results = np.argpartition(-cos_scores, range(min(5, len(cos_scores))))[:5]
            
            for idx in top_results:
                score = cos_scores[idx].item()
                if score > best_score:
                    best_score = score
                    best_match = {
                        "metric_id": labels_info[idx][0],
                        "label": labels_info[idx][1],
                        "score": score
                    }

        if best_match and best_score >= threshold:
            logger.info(f"Semantic Match: '{raw_term}' -> {best_match['metric_id']} ({best_score:.2f})")
            return best_match
        
        return None

if __name__ == "__main__":
    matcher = LocalSemanticMatcher()
    matcher.refresh_cache()
    # Test case
    match = matcher.map_term("Financing and advances", statement_type="Balance Sheet")
    print(f"Result: {match}")
