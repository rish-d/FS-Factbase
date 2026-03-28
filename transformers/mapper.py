import os
import json
import duckdb
from loguru import logger

class StandardizedMapper:
    def __init__(self, db_path="fs_factbase.duckdb"):
        self.db_path = db_path

    def load_aliases(self, conn):
        """Loads all aliases into a lookup dictionary for fast access."""
        query = "SELECT raw_term, institution_id, metric_id FROM Metric_Aliases"
        rows = conn.execute(query).fetchall()
        # Key: (raw_term.lower(), institution_id)
        return {(r[0].lower(), r[1]): r[2] for r in rows}

    def process_file(self, json_path):
        logger.info(f"Processing extraction result: {json_path}")
        
        with open(json_path, 'r') as f:
            data = json.load(f)

        institution_id = data.get("institution_id")
        reporting_period = data.get("reporting_period")
        source_doc = data.get("source_document")
        metrics = data.get("extracted_metrics", [])

        if not metrics:
            logger.warning(f"No metrics found in {json_path}")
            return

        conn = duckdb.connect(self.db_path)
        alias_map = self.load_aliases(conn)
        
        mapped_count = 0
        unmapped_count = 0

        for m in metrics:
            raw_term = m.get("raw_term")
            raw_value = m.get("raw_value")
            page_num = m.get("source_page_number", 0)

            if raw_term is None or raw_value is None:
                continue

            # Lookup
            metric_id = alias_map.get((raw_term.lower(), institution_id))

            if metric_id:
                # Insert into Fact_Financials
                conn.execute("""
                    INSERT INTO Fact_Financials (metric_id, institution_id, reporting_period, value, source_document, source_page_number)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (metric_id, institution_id, reporting_period, raw_value, source_doc, page_num))
                mapped_count += 1
            else:
                # Insert into Unmapped_Staging
                conn.execute("""
                    INSERT INTO Unmapped_Staging (raw_term, raw_value, institution_id, reporting_period, source_document, source_page_number)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (raw_term, raw_value, institution_id, reporting_period, source_doc, page_num))
                unmapped_count += 1

        conn.close()
        logger.success(f"Finished {json_path}: Mapped {mapped_count}, Unmapped {unmapped_count}")

def run_mapping():
    interim_dir = "data/interim/extracted_metrics"
    if not os.path.exists(interim_dir):
        logger.error(f"Directory not found: {interim_dir}")
        return

    mapper = StandardizedMapper()
    
    for filename in os.listdir(interim_dir):
        if filename.endswith(".json") and "FAILED" not in filename:
            full_path = os.path.join(interim_dir, filename)
            mapper.process_file(full_path)

if __name__ == "__main__":
    run_mapping()
