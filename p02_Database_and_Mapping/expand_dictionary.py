import duckdb
import os
import csv
from pathlib import Path
from loguru import logger
import db_config
from taxonomy_parser import TaxonomyParser

class DictionaryExpander:
    def __init__(self, db_path=None):
        self.db_path = db_path or db_config.get_db_path()
        # Path to IFRS taxonomy files - assuming it's in the same base dir as taxonomy_parser
        base_dir = Path(__file__).parent
        self.parser = TaxonomyParser(str(base_dir / "IFRSAT-2025"))
        self.parser.load_labels()
        self.suggested_file = base_dir / "suggested_aliases.csv"

    def expand(self):
        """
        Scans Unmapped_Staging for unique terms and tries to match them against the IFRS taxonomy.
        Confidence > 90%: Auto-map
        Confidence 70-90%: Suggest in CSV
        """
        logger.info("Starting dictionary expansion...")
        conn = duckdb.connect(self.db_path)
        
        try:
            # 1. Get unique unmapped terms that haven't been resolved yet
            # We filter out items already in Metric_Aliases to avoid duplicates
            unmapped_query = """
                SELECT DISTINCT s.raw_term 
                FROM Unmapped_Staging s
                LEFT JOIN Metric_Aliases a ON s.raw_term = a.raw_term
                WHERE a.raw_term IS NULL 
                AND s.requires_human_review = FALSE
            """
            unmapped_terms = conn.execute(unmapped_query).fetchall()
            
            if not unmapped_terms:
                logger.info("No unique unmapped terms requiring expansion.")
                return

            logger.info(f"Processing {len(unmapped_terms)} unique unmapped terms...")
            
            auto_mapped_count = 0
            suggestion_count = 0
            suggestions = []
            
            for (term,) in unmapped_terms:
                result = self.parser.find_best_match(term)
                if not result:
                    continue
                    
                concept_id, label, score = result
                
                if score > 90.0:
                    # Case 1: Confidence > 90% -> Auto-insert
                    try:
                        self._apply_auto_mapping(conn, term, concept_id, label)
                        auto_mapped_count += 1
                    except Exception as e:
                        logger.error(f"Failed to auto-map '{term}': {e}")
                elif 70.0 <= score <= 90.0:
                    # Case 2: Confidence 70-90% -> Add to suggestions
                    suggestions.append({
                        "raw_term": term,
                        "suggested_id": concept_id,
                        "label": label,
                        "score": round(score, 2)
                    })
                    suggestion_count += 1
            
            # Save suggestions to CSV
            if suggestions:
                self._save_suggestions(suggestions)
                
            logger.info(f"Expansion cycle finished. Auto-mapped: {auto_mapped_count}, Suggested: {suggestion_count}")
            
        finally:
            conn.close()

    def _apply_auto_mapping(self, conn, raw_term, concept_id, label):
        """Atomically updates Core_Metrics (if new) and Metric_Aliases."""
        # 1. Check/Insert into Core_Metrics
        # Note: We use a simple source_metadata tag for tracking
        exists = conn.execute("SELECT 1 FROM Core_Metrics WHERE metric_id = ?", [concept_id]).fetchone()
        if not exists:
            conn.execute("""
                INSERT INTO Core_Metrics (metric_id, standardized_metric_name, data_type, source_metadata)
                VALUES (?, ?, 'MONETARY', 'source: "auto-expanded"')
            """, [concept_id, label])
            logger.info(f"NEW CORE METRIC: {concept_id} ({label})")
            
        # 2. Add to Metric_Aliases
        conn.execute("""
            INSERT INTO Metric_Aliases (metric_id, raw_term)
            VALUES (?, ?)
        """, [concept_id, raw_term])
        logger.info(f"NEW ALIAS: '{raw_term}' -> {concept_id}")

    def _save_suggestions(self, suggestions):
        """Append suggestions to the CSV file for manual review."""
        file_exists = self.suggested_file.exists()
        
        with open(self.suggested_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["raw_term", "suggested_id", "label", "score"])
            if not file_exists:
                writer.writeheader()
            writer.writerows(suggestions)
        
        logger.info(f"Appended {len(suggestions)} suggestions to {self.suggested_file}")

if __name__ == "__main__":
    expander = DictionaryExpander()
    expander.expand()
