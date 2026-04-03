import duckdb
from difflib import SequenceMatcher
from typing import List, Dict, Any
import db_config

def get_similarity(a: str, b: str) -> float:
    """Returns a similarity ratio between 0.0 and 1.0"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

class ClusterAnalyzer:
    def __init__(self, db_path=None):
        if db_path is None:
            self.db_path = db_config.get_db_path()
        else:
            self.db_path = db_path
        
    def get_clusters(self, threshold=0.65) -> List[Dict[str, Any]]:
        """
        Fetches all unmapped terms and groups them by string similarity.
        Does not mutate the database, only prepares a JSON-friendly payload for review.
        """
        conn = duckdb.connect(self.db_path, read_only=True)
        # Fetch unique terms with their frequency to prioritize high-impact clusters
        rows = conn.execute("SELECT raw_term, COUNT(*) as frequency FROM Unmapped_Staging GROUP BY raw_term").fetchall()
        
        # We also need some sample institution/values for context
        # Just grabbing the first occurrence for context
        context_rows = conn.execute("SELECT raw_term, raw_value, institution_id FROM Unmapped_Staging").fetchall()
        conn.close()
        
        context_map = {}
        for r_term, r_val, r_inst in context_rows:
            if r_term not in context_map:
                context_map[r_term] = {"sample_value": r_val, "institution": r_inst}
        
        clusters = []
        visited = set()
        
        for i, (term1, count1) in enumerate(rows):
            if term1 in visited:
                continue
            
            current_cluster = {
                "leader_term": term1,
                "total_frequency": count1,
                "terms": [{"term": term1, "frequency": count1, "sample": context_map.get(term1)}]
            }
            visited.add(term1)
            
            for j in range(i+1, len(rows)):
                term2, count2 = rows[j]
                if term2 in visited:
                    continue
                    
                similarity = get_similarity(term1, term2)
                # Group if high overlap or if one string is almost entirely within another
                is_substring = (term1.lower() in term2.lower()) or (term2.lower() in term1.lower())
                
                if similarity >= threshold or (is_substring and len(term1) > 4 and len(term2) > 4):
                    current_cluster["terms"].append({
                        "term": term2, 
                        "frequency": count2, 
                        "sample": context_map.get(term2)
                    })
                    current_cluster["total_frequency"] += count2
                    visited.add(term2)
                    
            clusters.append(current_cluster)
            
        # Sort by impact
        clusters.sort(key=lambda x: x["total_frequency"], reverse=True)
        return clusters

if __name__ == "__main__":
    analyzer = ClusterAnalyzer()
    import json
    print(json.dumps(analyzer.get_clusters(), indent=2))
