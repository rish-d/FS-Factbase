import os
import duckdb
from loguru import logger
import db_config
from rapidfuzz import process, fuzz
from local_semantic_matcher import LocalSemanticMatcher
from llm_reconciler import LLMReconciler

class ReconciliationEngine:
    def __init__(self, db_path=None):
        self.db_path = db_path or db_config.get_db_path()
        self.semantic_matcher = LocalSemanticMatcher()
        
        try:
            self.llm_reconciler = LLMReconciler()
        except Exception as e:
            logger.warning(f"LLMReconciler could not be initialized: {e}. Tier 3 will be bypassed.")
            self.llm_reconciler = None
        
    def reconcile_unmapped(self):
        """
        Orchestrates the three-tier Waterfall reconciliation.
        """
        logger.info("Starting Waterfall Reconciliation Cycle...")
        conn = duckdb.connect(self.db_path)
        
        try:
            # 1. Fetch unmapped terms with context
            unmapped = conn.execute("""
                SELECT staging_id, raw_term, statement_type, institution_id, reporting_period, 
                       source_document, source_page_number, raw_value, month_end, is_cumulative, scaling_factor
                FROM Unmapped_Staging
                WHERE requires_human_review = FALSE
                AND retry_count < 3
            """).df()
            
            if unmapped.empty:
                logger.info("No unmapped terms found in staging.")
                return

            # Load IFRS candidates for Tier 1
            ifrs_candidates = conn.execute("SELECT metric_id, standardized_metric_name FROM Core_Metrics").fetchall()
            ifrs_map = {row[1].lower(): row[0] for row in ifrs_candidates}
            ifrs_labels = list(ifrs_map.keys())

            resolved_count = 0
            
            for _, row in unmapped.iterrows():
                term = row['raw_term']
                stmt_type = row['statement_type']
                
                # --- TIER 1: DETERMINISTIC & FUZZY (Local, $0) ---
                match_id = self._try_tier_1(term, ifrs_map, ifrs_labels)
                if match_id:
                    self._promote_to_fact(conn, row, match_id, "Tier 1: Lexical Fuzzy Match")
                    resolved_count += 1
                    continue
                
                # --- TIER 2: LOCAL SEMANTIC (Local, $0) ---
                match = self.semantic_matcher.map_term(term, stmt_type)
                if match:
                    self._promote_to_fact(conn, row, match['metric_id'], f"Tier 2: Semantic Match (Score: {match['score']:.2f})")
                    resolved_count += 1
                    continue
                
                # --- TIER 3: LLM EXCEPTION (API, $) ---
                if self.llm_reconciler:
                    llm_results = self.llm_reconciler.reconcile_batch([{"raw_term": term, "statement_type": stmt_type}])
                    if llm_results and llm_results[0]['ifrs_concept_id'] != "UNMAPPED":
                        res = llm_results[0]
                        if res['confidence'] > 0.7:
                            self._promote_to_fact(conn, row, res['ifrs_concept_id'], f"Tier 3: LLM Reconciler (Conf: {res['confidence']:.2f})")
                            resolved_count += 1
                            continue

                # If all fail, increment retry
                conn.execute("""
                    UPDATE Unmapped_Staging 
                    SET retry_count = retry_count + 1,
                        last_attempt_date = CURRENT_TIMESTAMP,
                        requires_human_review = (retry_count + 1 >= 3)
                    WHERE staging_id = ?
                """, [row['staging_id']])

            logger.success(f"Waterfall complete. Resolved {resolved_count} items.")
            
        finally:
            conn.close()

    def _try_tier_1(self, term, ifrs_map, ifrs_labels):
        """Cheap lexical normalization and fuzzy matching (>90%)."""
        term_clean = term.lower().strip()
        # Direct hit
        if term_clean in ifrs_map:
            return ifrs_map[term_clean]
        
        # Fuzzy hit
        best_match = process.extractOne(term_clean, ifrs_labels, scorer=fuzz.token_sort_ratio)
        if best_match and best_match[1] > 90:
            return ifrs_map[best_match[0]]
            
        return None

    def _promote_to_fact(self, conn, staging_row, metric_id, reason):
        """Moves a resolved staging item to Fact_Financials."""
        # Check for existing metric and alias
        # 1. Ensure alias exists so future hits are Tier 0 (Alias Map)
        conn.execute("""
            INSERT OR IGNORE INTO Metric_Aliases (metric_id, raw_term) 
            VALUES (?, ?)
        """, [metric_id, staging_row['raw_term']])
        
        # 2. Insert into Fact_Financials
        fact = [
            metric_id, staging_row['institution_id'], staging_row['reporting_period'], 
            staging_row['raw_value'], "MYR", True, None, 
            staging_row['source_document'], staging_row['source_page_number'],
            0.9, reason, staging_row['month_end'], staging_row['is_cumulative'], staging_row['scaling_factor']
        ]
        
        conn.execute("""
            INSERT OR IGNORE INTO Fact_Financials 
            (metric_id, institution_id, reporting_period, value, currency_code, is_published, formula_id, source_document, source_page_number, confidence_score, confidence_reason, month_end, is_cumulative, scaling_factor)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, fact)
        
        # 3. Remove from staging
        conn.execute("DELETE FROM Unmapped_Staging WHERE staging_id = ?", [staging_row['staging_id']])

if __name__ == "__main__":
    engine = ReconciliationEngine()
    engine.reconcile_unmapped()
