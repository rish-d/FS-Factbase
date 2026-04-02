import duckdb
from loguru import logger

def purge_test_data(db_path: str = "fs_factbase.duckdb"):
    """Purges all record tables to allow for a clean re-run with high-fidelity data."""
    logger.warning(f"Purging all test data from {db_path}...")
    try:
        conn = duckdb.connect(db_path)
        # We keep Core_Metrics and Metric_Aliases (the seed data)
        # But we clear the facts and the review queue
        conn.execute("DELETE FROM Fact_Financials")
        conn.execute("DELETE FROM Unmapped_Staging")
        
        facts_count = conn.execute("SELECT count(*) FROM Fact_Financials").fetchone()[0]
        unmapped_count = conn.execute("SELECT count(*) FROM Unmapped_Staging").fetchone()[0]
        
        logger.success(f"Purge complete. Fact_Financials: {facts_count}, Unmapped_Staging: {unmapped_count}")
        conn.close()
    except Exception as e:
        logger.error(f"Purge failed: {e}")

if __name__ == "__main__":
    purge_test_data()
