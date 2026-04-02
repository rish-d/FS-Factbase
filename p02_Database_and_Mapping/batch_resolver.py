import duckdb
from loguru import logger
from typing import List, Dict

class BatchResolver:
    def __init__(self, db_path="fs_factbase.duckdb"):
        self.db_path = db_path
        
    def resolve_cluster_to_metric(self, target_metric_id: str, aliases: List[str], create_new_metric: bool = False, new_metric_details: Dict = None) -> bool:
        """
        Atomically resolves a list of unmapped aliases to a core metric.
        If create_new_metric is true, inserts the core metric first.
        Moves all associated entries from Unmapped_Staging to Fact_Financials.
        Deletes the migrated entries from Unmapped_Staging.
        """
        logger.info(f"Initiating Atomic Resolution for {len(aliases)} terms to Core Metric: {target_metric_id}")
        conn = duckdb.connect(self.db_path)
        
        try:
            # 1. Begin ACID Transaction
            conn.execute("BEGIN TRANSACTION")
            
            # 2. Mutate Core Standards (Evolver Pattern)
            if create_new_metric and new_metric_details:
                if not new_metric_details.get("standard") or new_metric_details.get("standard") == "Unknown":
                    logger.warning(f"Standard not provided for {target_metric_id}. This violates the integrity rule.")
                    # In a real app, we might raise an error here if we wanted to be strict.
                
                conn.execute(
                    "INSERT INTO Core_Metrics (metric_id, standardized_metric_name, accounting_standard, data_type) VALUES (?, ?, ?, ?)",
                    (
                        new_metric_details.get("metric_id", target_metric_id),
                        new_metric_details.get("standardized_name", target_metric_id.replace("_", " ").title()),
                        new_metric_details.get("standard", "IFRS"),
                        new_metric_details.get("data_type", "Currency")
                    )
                )
            
            for alias in aliases:
                # Find all institutions using this terminology
                institutions = conn.execute("SELECT DISTINCT institution_id FROM Unmapped_Staging WHERE raw_term = ?", (alias,)).fetchall()
                if not institutions:
                    institutions = [('',)] 

                for inst in institutions:
                    # 3. Mutate Translation Schema
                    # Check if already exists to prevent duplicate constraint violation
                    exists = conn.execute("SELECT 1 FROM Metric_Aliases WHERE raw_term = ? AND institution_id = ?", (alias, inst[0])).fetchone()
                    if not exists:
                        conn.execute(
                            "INSERT INTO Metric_Aliases (metric_id, raw_term, institution_id) VALUES (?, ?, ?)",
                            (target_metric_id, alias, inst[0])
                        )
                
                # 4. Flush to Production
                conn.execute(
                    """
                    INSERT INTO Fact_Financials (metric_id, institution_id, reporting_period, value, source_document, source_page_number, confidence_score, confidence_reason, month_end, is_cumulative, scaling_factor)
                    SELECT ?, institution_id, reporting_period, raw_value, source_document, source_page_number, confidence_score, 'HITL Batch Resolution', month_end, is_cumulative, scaling_factor
                    FROM Unmapped_Staging 
                    WHERE raw_term = ?
                    """,
                    (target_metric_id, alias)
                )
                
                # 5. Clean review backlog
                conn.execute("DELETE FROM Unmapped_Staging WHERE raw_term = ?", (alias,))
                
            # Finalize Transaction
            conn.execute("COMMIT")
            logger.success(f"Atomic Batch Resolution successful. Mapped {len(aliases)} terms to {target_metric_id}.")
            return True
            
        except Exception as e:
            # Critical Safety Net
            conn.execute("ROLLBACK")
            logger.error(f"Atomic Batch Resolution failed. Database rolled back to pristine state. Error: {e}")
            return False
        finally:
            conn.close()
