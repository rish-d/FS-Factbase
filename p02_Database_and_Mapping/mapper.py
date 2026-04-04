import os
import json
import sys
import duckdb
from loguru import logger

# Ensure project root is in sys.path for package imports
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

try:
    from p02_Database_and_Mapping import db_config
except ImportError:
    import db_config

class StandardizedMapper:
    def __init__(self, db_path=None):
        if db_path is None:
            self.db_path = db_config.get_db_path()
        else:
            self.db_path = db_path

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

    def audit_traceability(self, fact_or_staging, is_mapped=True):
        """
        Audits a fact or staging record for traceability metadata.
        Returns (refined_score, reason)
        """
        # index 8 is page_num for facts, index 5 for staging
        page_num = fact_or_staging[8] if is_mapped else fact_or_staging[5]
        # index 7 is doc for facts, index 4 for staging
        source_doc = fact_or_staging[7] if is_mapped else fact_or_staging[4]
        
        score_penalty = 0.0
        reasons = []
        
        if not page_num or int(page_num) <= 0:
            score_penalty += 0.3
            reasons.append("Missing source page number")
        
        if not source_doc or source_doc == "Unknown":
            score_penalty += 0.2
            reasons.append("Unknown source document")
            
        initial_score = fact_or_staging[9] if is_mapped else fact_or_staging[6]
        final_score = max(0.0, initial_score - score_penalty)
        
        audit_reason = " | ".join(reasons) if reasons else "Traceability Verified"
        if not is_mapped and not reasons:
            audit_reason = "Unmapped Term (Traceable)"
            
        return final_score, audit_reason

    def process_file(self, json_path):
        logger.info(f"Processing extraction result: {json_path}")
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Failed to read {json_path}: {e}")
            return

        # Canonical ID resolution
        raw_inst_id = str(data.get("institution_id") or "").lower()
        source_doc = data.get("source_document", "Unknown")
        
        conn = duckdb.connect(self.db_path)
        
        # Robust Institution ID Resolution
        inst_map = self.load_institutions(conn)
        institution_id = inst_map.get(raw_inst_id)
        
        if not institution_id:
            # Fallback: try to derive from filename
            basename = os.path.basename(json_path).lower().split("_20")[0]
            base_name_stripped = basename.replace(" berhad", "").replace(" holdings", "").replace(" group", "").strip()
            institution_id = inst_map.get(basename) or inst_map.get(basename.replace(" ", "_")) or inst_map.get(base_name_stripped) or inst_map.get(base_name_stripped.replace(" ", "_"))
            
        if not institution_id:
            logger.error(f"CRITICAL: Could not resolve institution for '{raw_inst_id}' in {json_path}")
            conn.close()
            return

        # Standardize 'statements' vs 'financial_statements'
        statements = data.get("statements", data.get("financial_statements", []))
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
            # Standardize 'items' vs 'line_items'
            line_items = stmt.get("items", stmt.get("line_items", stmt.get("data", [])))
            
            for item in line_items:
                # Standardize 'item' vs 'item_name'
                raw_term = item.get("item", item.get("item_name", item.get("line_item", "")))
                # Standardize 'values' vs 'data_points'
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
                        is_published = val_obj.get("is_published", True)
                        currency = val_obj.get("currency", val_obj.get("currency_code", "MYR"))
                        # Traceability: Standardize page resolution
                        page_num = val_obj.get("source_page_number", val_obj.get("page_number", stmt.get("source_page_number", data.get("source_page_number", 0))))
                        
                        is_cumulative = val_obj.get("is_cumulative")
                        if is_cumulative is None:
                            is_cumulative = "balance sheet" in statement_type or "position" in statement_type
                    else:
                        # Fallback for legacy/loose JSON structures
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
                        page_num = data.get("source_page_number", 0)

                    if year is None or val is None:
                        continue

                    # NORMALIZE (Scale Factor Enforcement)
                    try:
                        normalized_val = float(val) * float(scaling_factor)
                    except:
                        normalized_val = val

                    # Map raw term to standardized metric
                    metric_id = alias_map.get((raw_term.lower(), institution_id))
                    if not metric_id:
                        metric_id = alias_map.get((raw_term.lower(), None))

                    if metric_id:
                        # [metric_id, inst_id, year, val, curr, published, formula, doc, page, score, reason, month, cumulative, scale]
                        fact = [
                            metric_id, institution_id, str(year), normalized_val, currency, 
                            is_published, None, # formula_id
                            source_doc, page_num, 1.0, "Initial Mapping", month_end, is_cumulative, scaling_factor
                        ]
                        
                        # Apply Traceability Audit
                        final_score, audit_reason = self.audit_traceability(fact, is_mapped=True)
                        fact[9] = final_score
                        fact[10] = audit_reason
                        facts_to_insert.append(tuple(fact))
                    else:
                        # Routing to Unmapped_Staging (Zero-Hallucination)
                        # [term, val, inst_id, year, doc, page, score, reason, month, cumulative, scale]
                        staging = [
                            raw_term, normalized_val, institution_id, str(year), 
                            source_doc, page_num, 0.5, "Unmapped Term", month_end, is_cumulative, scaling_factor,
                            statement_type
                        ]
                        
                        # Apply Traceability Audit (Staging uses lower base score)
                        final_score, audit_reason = self.audit_traceability(staging, is_mapped=False)
                        staging[6] = final_score
                        staging[7] = audit_reason
                        staging_to_insert.append(tuple(staging))

        # Bulk Insert using DuckDB executemany
        if facts_to_insert:
            conn.executemany("""
                INSERT OR IGNORE INTO Fact_Financials 
                (metric_id, institution_id, reporting_period, value, currency_code, is_published, formula_id, source_document, source_page_number, confidence_score, confidence_reason, month_end, is_cumulative, scaling_factor)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, facts_to_insert)
            
        if staging_to_insert:
            conn.executemany("""
                INSERT INTO Unmapped_Staging 
                (raw_term, raw_value, institution_id, reporting_period, source_document, source_page_number, confidence_score, confidence_reason, month_end, is_cumulative, scaling_factor, statement_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, staging_to_insert)
            
        conn.close()
        logger.success(f"Finished {json_path}: Mapped {len(facts_to_insert)}, Unmapped {len(staging_to_insert)}")

    def process_unmapped_staging(self):
        """
        Re-evaluates items in Unmapped_Staging against the current Metric_Aliases.
        Successfully mapped items move to Fact_Financials.
        Failing items have their retry_count incremented.
        """
        logger.info("Scanning Unmapped_Staging for newly resolvable matches...")
        conn = duckdb.connect(self.db_path)
        
        try:
            # 1. Load latest aliases
            alias_map = self.load_aliases(conn)
            
            # 2. Fetch unmapped records that haven't hit the retry limit
            # Note: We fetch the full row to move it if mapped
            staged_records = conn.execute("""
                SELECT staging_id, raw_term, raw_value, institution_id, reporting_period, 
                       source_document, source_page_number, confidence_score, confidence_reason,
                       month_end, is_cumulative, scaling_factor, retry_count
                FROM Unmapped_Staging
                WHERE requires_human_review = FALSE
                AND retry_count < 3
            """).fetchall()
            
            if not staged_records:
                logger.info("No records in staging suitable for re-processing.")
                return

            mapped_count = 0
            failed_count = 0
            
            for rec in staged_records:
                staging_id, raw_term, val, inst_id, period, doc, page, score, reason, month, cumulative, scale, retry = rec
                
                # Try to map again
                metric_id = alias_map.get((raw_term.lower(), inst_id))
                if not metric_id:
                    metric_id = alias_map.get((raw_term.lower(), None))
                
                if metric_id:
                    # Success! Move to Fact_Financials
                    # We reset confidence to 0.9 as it's now auto-resolved but was previously unmapped
                    fact = [
                        metric_id, inst_id, period, val, "MYR", 
                        True, None, doc, page, 0.9, "Auto-Resolved via Re-queue", 
                        month, cumulative, scale
                    ]
                    
                    conn.execute("""
                        INSERT OR IGNORE INTO Fact_Financials 
                        (metric_id, institution_id, reporting_period, value, currency_code, 
                         is_published, formula_id, source_document, source_page_number, 
                         confidence_score, confidence_reason, month_end, is_cumulative, scaling_factor)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, fact)
                    
                    # Delete from staging
                    conn.execute("DELETE FROM Unmapped_Staging WHERE staging_id = ?", [staging_id])
                    mapped_count += 1
                else:
                    # Still unmapped, increment retry
                    new_retry = retry + 1
                    requires_review = (new_retry >= 3)
                    conn.execute("""
                        UPDATE Unmapped_Staging
                        SET retry_count = ?, 
                            last_attempt_date = CURRENT_TIMESTAMP,
                            requires_human_review = ?
                        WHERE staging_id = ?
                    """, [new_retry, requires_review, staging_id])
                    failed_count += 1
                    
            logger.success(f"Re-queue complete: {mapped_count} resolved, {failed_count} still unmapped.")
            
        finally:
            conn.close()

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
