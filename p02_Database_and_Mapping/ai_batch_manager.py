import os
import json
import duckdb
import datetime
from loguru import logger
from typing import List, Dict
import db_config
from llm_reconciler import LLMReconciler
from batch_resolver import BatchResolver

class AIBatchManager:
    def __init__(self, db_path=None, confidence_threshold=0.90, min_occurrences=1):
        self.db_path = db_path or db_config.get_db_path()
        self.confidence_threshold = confidence_threshold
        self.min_occurrences = min_occurrences
        self.reconciler = LLMReconciler()
        self.resolver = BatchResolver(self.db_path)

    def run_optimistic_batch(self, limit=20):
        """
        Runs an optimistic batch resolution:
        1. Fetch top unmapped terms.
        2. Reconcile with LLM.
        3. Auto-resolve if confidence >= threshold.
        """
        logger.info(f"🚀 Starting Optimistic AI Batch Resolution (Threshold: {self.confidence_threshold})")
        
        # 1. Fetch unmapped terms with frequency counts
        unmapped_data = self._fetch_top_unmapped(limit)
        if not unmapped_data:
            logger.info("No unmapped terms found in staging. Exiting.")
            return

        # 2. Reconcile with LLM
        # unmapped_data is a list of {"raw_term": "...", "statement_type": "...", "count": X}
        reconciliation_results = self.reconciler.reconcile_batch(unmapped_data)
        
        # 3. Filter high confidence results
        to_resolve = []
        for res in reconciliation_results:
            raw_term = res.get("raw_term")
            metric_id = res.get("ifrs_concept_id")
            confidence = res.get("confidence", 0)
            
            if metric_id != "UNMAPPED" and confidence >= self.confidence_threshold:
                to_resolve.append({
                    "raw_term": raw_term,
                    "metric_id": metric_id,
                    "confidence": confidence
                })
            else:
                logger.warning(f"Skipping term '{raw_term}': Low confidence ({confidence}) or UNMAPPED.")

        if not to_resolve:
            logger.info("No high-confidence mappings found in this batch.")
            return

        # 4. Execute atomic resolutions in a batch
        # We group by metric_id to use batch_resolver effectively
        batch_id = f"ai_batch_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"📦 Grouping {len(to_resolve)} terms into batch: {batch_id}")

        mappings_by_metric = {}
        for item in to_resolve:
            m_id = item["metric_id"]
            if m_id not in mappings_by_metric:
                mappings_by_metric[m_id] = []
            mappings_by_metric[m_id].append(item["raw_term"])

        success_count = 0
        for m_id, terms in mappings_by_metric.items():
            success = self.resolver.resolve_cluster_to_metric(
                target_metric_id=m_id,
                aliases=terms,
                batch_id=batch_id,
                is_ai_generated=True
            )
            if success:
                success_count += len(terms)

        logger.success(f"✅ Optimistic Batch {batch_id} complete. Resolved {success_count} terms.")
        return success_count

    def _fetch_top_unmapped(self, limit):
        conn = duckdb.connect(self.db_path)
        query = f"""
            SELECT raw_term, statement_type, count(*) as term_count
            FROM Unmapped_Staging
            GROUP BY raw_term, statement_type
            HAVING count(*) >= ?
            ORDER BY term_count DESC
            LIMIT ?
        """
        rows = conn.execute(query, (self.min_occurrences, limit)).fetchall()
        conn.close()
        
        return [{"raw_term": r[0], "statement_type": r[1], "count": r[2]} for r in rows]

if __name__ == "__main__":
    # Example execution
    # Set lower threshold for testing if needed
    manager = AIBatchManager(confidence_threshold=0.85)
    manager.run_optimistic_batch(limit=10)
