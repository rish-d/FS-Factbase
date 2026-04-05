import duckdb
from loguru import logger
import db_config
import sys

class BatchRollbackManager:
    def __init__(self, db_path=None):
        self.db_path = db_path or db_config.get_db_path()

    def rollback_batch(self, batch_id: str):
        """
        Reverses a specific AI resolution batch.
        1. Restores records from AI_Resolution_Log to Unmapped_Staging.
        2. Deletes facts from Fact_Financials.
        3. Deletes aliases from Metric_Aliases.
        4. Deletes entries from AI_Resolution_Log.
        """
        if not batch_id:
            logger.error("No batch_id provided for rollback.")
            return False

        logger.info(f"🔄 Initiating rollback for batch: {batch_id}")
        conn = duckdb.connect(self.db_path)

        try:
            conn.execute("BEGIN TRANSACTION")

            # 1. Restore to Unmapped_Staging
            # We skip staging_id to let the sequence handle it, but we map all other relevant fields
            conn.execute(
                """
                INSERT INTO Unmapped_Staging (
                    raw_term, raw_value, institution_id, reporting_period, source_document, 
                    source_page_number, confidence_score, confidence_reason, month_end, 
                    is_cumulative, scaling_factor, statement_type, entity_scope
                )
                SELECT 
                    raw_term, raw_value, institution_id, reporting_period, source_document, 
                    source_page_number, confidence_score, 'Rolled back from ' || batch_id, 
                    month_end, is_cumulative, scaling_factor, statement_type, entity_scope
                FROM AI_Resolution_Log
                WHERE batch_id = ?
                """,
                (batch_id,)
            )
            restored_count = conn.execute("SELECT count(*) FROM AI_Resolution_Log WHERE batch_id = ?", (batch_id,)).fetchone()[0]

            # 2. Delete from Fact_Financials
            conn.execute("DELETE FROM Fact_Financials WHERE batch_id = ?", (batch_id,))
            facts_deleted = conn.execute("SELECT Changes()").fetchone()[0]

            # 3. Delete from Metric_Aliases
            conn.execute("DELETE FROM Metric_Aliases WHERE batch_id = ?", (batch_id,))
            aliases_deleted = conn.execute("SELECT Changes()").fetchone()[0]

            # 4. Cleanup AI_Resolution_Log
            conn.execute("DELETE FROM AI_Resolution_Log WHERE batch_id = ?", (batch_id,))

            conn.execute("COMMIT")
            logger.success(f"✅ Rollback successful for {batch_id}.")
            logger.info(f"Summary: Restored {restored_count} terms to Staging, Deleted {facts_deleted} facts and {aliases_deleted} aliases.")
            return True

        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"❌ Rollback failed for {batch_id}: {e}")
            return False
        finally:
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python rollback_batch.py <batch_id>")
    else:
        mgr = BatchRollbackManager()
        mgr.rollback_batch(sys.argv[1])
