import os
import json
import sys
import duckdb
from loguru import logger

import db_config

# Ensure project root is in sys.path for package imports
if db_config.ROOT_DIR not in sys.path:
    sys.path.append(db_config.ROOT_DIR)

try:
    from p03_Analytics_Dashboard.trend_analyzer import TrendAnalyzer
except ImportError:
    logger.warning("TrendAnalyzer not found. Using MockTrendAnalyzer.")
    class TrendAnalyzer:
        def __init__(self, db_path): pass
        def analyze_value(self, *args): return 1.0, "No analyzer available"

class StandardizedMapper:
    def __init__(self, db_path=None):
        if db_path is None:
            self.db_path = db_config.get_db_path()
        else:
            self.db_path = db_path
        self.trend_analyzer = TrendAnalyzer(self.db_path)

    def load_aliases(self, conn):
        """Loads all aliases into a lookup dictionary for fast access."""
        query = "SELECT raw_term, institution_id, metric_id FROM Metric_Aliases"
        rows = conn.execute(query).fetchall()
        # Key: (raw_term.lower(), institution_id)
        return {(r[0].lower(), r[1]): r[2] for r in rows}

    def load_institutions(self, conn):
        """Loads all institutions to support flexible matching."""
        rows = conn.execute("SELECT institution_id, name FROM Institutions").fetchall()
        # Map: lowcase_id -> id AND lowcase_name -> id
        mapping = {}
        for row in rows:
            inst_id, name = row[0].lower(), row[1].lower()
            mapping[inst_id] = row[0]
            # Strip common suffixes for better fuzzy matching
            base_name = name.replace(" berhad", "").replace(" holdings", "").replace(" group", "").strip()
            mapping[name] = row[0]
            mapping[name.replace(" ", "_")] = row[0]
            mapping[base_name] = row[0]
            mapping[base_name.replace(" ", "_")] = row[0]
        return mapping

    def process_file(self, json_path):
        logger.info(f"Processing extraction result: {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        raw_inst_id = str(data.get("institution_id") or "").lower()
        source_doc = data.get("source_document")
        
        conn = duckdb.connect(self.db_path)
        
        # Robust Institution ID Resolution
        inst_map = self.load_institutions(conn)
        institution_id = inst_map.get(raw_inst_id)
        
        if not institution_id:
            # Fallback: try to derive from filename if json field is missing or generic
            # e.g. "PUBLIC BANK BERHAD_2021_extracted.json"
            basename = os.path.basename(json_path).lower().split("_20")[0]
            base_name_stripped = basename.replace(" berhad", "").replace(" holdings", "").replace(" group", "").strip()
            institution_id = inst_map.get(basename) or inst_map.get(basename.replace(" ", "_")) or inst_map.get(base_name_stripped) or inst_map.get(base_name_stripped.replace(" ", "_"))
            
        if not institution_id:
            logger.error(f"Could not resolve institution for '{raw_inst_id}' in {json_path}")
            conn.close()
            return

        # Support both 'statements' and 'financial_statements' in the raw JSON
        statements = data.get("statements", data.get("financial_statements", []))
        
        # If statements is a dict, convert to list
        if isinstance(statements, dict):
            statements = list(statements.values())

        if not statements:
            logger.warning(f"No statements found in {json_path}")
            conn.close()
            return

        alias_map = self.load_aliases(conn)
        
        facts_to_insert = []
        staging_to_insert = []

        for stmt in statements:
            statement_type = stmt.get("statement_type", "").lower()
            line_items = stmt.get("line_items", stmt.get("items", stmt.get("data", [])))
            for item in line_items:
                raw_term = item.get("item", item.get("item_name", item.get("line_item", item.get("name"))))
                values = item.get("values", item.get("data_points", item.get("data", [])))
                
                if not values and "value" in item:
                     values = [item]
                
                if not raw_term:
                    continue

                for i, val_obj in enumerate(values):
                    if isinstance(val_obj, dict):
                        year = val_obj.get("year", val_obj.get("reporting_year"))
                        val = val_obj.get("value", val_obj.get("amount"))
                        month_end = val_obj.get("month_end", 12)
                        scaling_factor = val_obj.get("scaling_factor", 1)
                        # Default is_published to True for report-extracted values
                        is_published = val_obj.get("is_published", True)
                        currency = val_obj.get("currency", val_obj.get("currency_code", "MYR"))
                        
                        is_cumulative = val_obj.get("is_cumulative")
                        if is_cumulative is None:
                            is_cumulative = "balance sheet" in statement_type
                    else:
                        val = val_obj
                        try:
                            primary_year = int(data.get("reporting_period", 0))
                            year = primary_year - i if primary_year > 0 else None
                        except:
                            year = None
                        month_end = 12
                        is_cumulative = "balance sheet" in statement_type
                        scaling_factor = 1
                        is_published = True
                        currency = "MYR"

                    if year is None or val is None:
                        continue

                    # Lookup: Try specific institution first, then global (None)
                    metric_id = alias_map.get((raw_term.lower(), institution_id))
                    if not metric_id:
                        metric_id = alias_map.get((raw_term.lower(), None))

                    if metric_id:
                        # SCALE ENFORCEMENT: Normalize raw value by scaling factor
                        try:
                            normalized_val = float(val) * float(scaling_factor)
                        except:
                            normalized_val = val

                        score, reason = self.trend_analyzer.analyze_value(institution_id, metric_id, str(year), normalized_val)
                        facts_to_insert.append((
                            metric_id, institution_id, str(year), normalized_val, currency, 
                            is_published, None, # formula_id
                            source_doc, 0, score, reason, month_end, is_cumulative, scaling_factor
                        ))
                    else:
                        # Also normalize staging for consistency in UI review
                        try:
                            normalized_val = float(val) * float(scaling_factor)
                        except:
                            normalized_val = val
                            
                        staging_to_insert.append((
                            raw_term, normalized_val, institution_id, str(year), source_doc, 0, 0.5, "Unmapped Term", month_end, is_cumulative, scaling_factor
                        ))

        # Bulk Insert Facts
        if facts_to_insert:
            conn.executemany("""
                INSERT OR IGNORE INTO Fact_Financials 
                (metric_id, institution_id, reporting_period, value, currency_code, is_published, formula_id, source_document, source_page_number, confidence_score, confidence_reason, month_end, is_cumulative, scaling_factor)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, facts_to_insert)
            
        # Bulk Insert Staging
        if staging_to_insert:
            conn.executemany("""
                INSERT INTO Unmapped_Staging 
                (raw_term, raw_value, institution_id, reporting_period, source_document, source_page_number, confidence_score, confidence_reason, month_end, is_cumulative, scaling_factor)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, staging_to_insert)

        conn.close()
        logger.success(f"Finished {json_path}: Mapped {len(facts_to_insert)}, Unmapped {len(staging_to_insert)}")

def run_mapping():
    # Use db_config.ROOT_DIR to find the data folder reliably
    interim_dir = os.path.join(db_config.ROOT_DIR, "data", "interim", "extracted_metrics")
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
