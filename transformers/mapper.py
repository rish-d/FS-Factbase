import os
import json
import duckdb
from loguru import logger
from analytics.trend_analyzer import TrendAnalyzer

class StandardizedMapper:
    def __init__(self, db_path="fs_factbase.duckdb"):
        self.db_path = db_path
        self.trend_analyzer = TrendAnalyzer(db_path)

    def load_aliases(self, conn):
        """Loads all aliases into a lookup dictionary for fast access."""
        query = "SELECT raw_term, institution_id, metric_id FROM Metric_Aliases"
        rows = conn.execute(query).fetchall()
        # Key: (raw_term.lower(), institution_id)
        return {(r[0].lower(), r[1]): r[2] for r in rows}

    def process_file(self, json_path):
        logger.info(f"Processing extraction result: {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        institution_id = data.get("institution_id")
        source_doc = data.get("source_document")
        
        # Support both 'statements' and 'financial_statements' in the raw JSON
        statements = data.get("statements", data.get("financial_statements", []))
        
        # If statements is a dict, convert to list
        if isinstance(statements, dict):
            statements = list(statements.values())

        if not statements:
            logger.warning(f"No statements found in {json_path}")
            return

        conn = duckdb.connect(self.db_path)
        alias_map = self.load_aliases(conn)
        
        mapped_count = 0
        unmapped_count = 0

        for stmt in statements:
            line_items = stmt.get("line_items", stmt.get("items", stmt.get("data", [])))
            for item in line_items:
                raw_term = item.get("item", item.get("item_name", item.get("line_item", item.get("name"))))
                values = item.get("values", item.get("data_points", item.get("data", [])))
                
                # Handle flat structure in mapper too
                if not values and "value" in item:
                     values = [item]
                
                if not raw_term:
                    continue

                for i, val_obj in enumerate(values):
                    # Robust handling of both dicts and plain values
                    if isinstance(val_obj, dict):
                        year = val_obj.get("year", val_obj.get("reporting_year"))
                        val = val_obj.get("value", val_obj.get("amount"))
                        month_end = val_obj.get("month_end", 12)
                        scaling_factor = val_obj.get("scaling_factor", 1)
                        
                        # Handle is_cumulative with Balance Sheet default
                        is_cumulative = val_obj.get("is_cumulative")
                        if is_cumulative is None:
                            is_cumulative = "balance sheet" in stmt.get("statement_type", "").lower()
                    else:
                        # If it's just a number, infer the year if possible
                        val = val_obj
                        # Try to guess the year: assume first is current, second is prev
                        try:
                            primary_year = int(data.get("reporting_period", 0))
                            if primary_year > 0:
                                year = primary_year - i
                            else:
                                year = None
                        except:
                            year = None
                        month_end = 12
                        is_cumulative = "balance sheet" in stmt.get("statement_type", "").lower()
                        scaling_factor = 1

                    if year is None or val is None:
                        continue

                    # Lookup
                    metric_id = alias_map.get((raw_term.lower(), institution_id))

                    if metric_id:
                        # Analyze trend for confidence
                        score, reason = self.trend_analyzer.analyze_value(institution_id, metric_id, str(year), val)
                        
                        # Insert into Fact_Financials
                        conn.execute("""
                            INSERT INTO Fact_Financials (metric_id, institution_id, reporting_period, value, source_document, source_page_number, confidence_score, confidence_reason, month_end, is_cumulative, scaling_factor)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (metric_id, institution_id, str(year), val, source_doc, 0, score, reason, month_end, is_cumulative, scaling_factor))
                        mapped_count += 1
                    else:
                        # Insert into Unmapped_Staging
                        conn.execute("""
                            INSERT INTO Unmapped_Staging (raw_term, raw_value, institution_id, reporting_period, source_document, source_page_number, confidence_score, confidence_reason, month_end, is_cumulative, scaling_factor)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (raw_term, val, institution_id, str(year), source_doc, 0, 0.5, "Unmapped Term", month_end, is_cumulative, scaling_factor))
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
