import duckdb
import db_config
from loguru import logger

def check_db():
    db_path = db_config.get_db_path()
    conn = duckdb.connect(db_path)
    
    logger.info("--- Database Verification Report ---")
    
    # Total Facts
    facts_count = conn.execute("SELECT count(*) FROM Fact_Financials").fetchone()[0]
    logger.info(f"Total Standardized Facts: {facts_count}")
    
    # Total Unmapped
    unmapped_count = conn.execute("SELECT count(*) FROM Unmapped_Staging").fetchone()[0]
    logger.info(f"Total Unmapped Terms: {unmapped_count}")
    
    # Sample Facts
    if facts_count > 0:
        logger.info("Sample Mapped Facts (Traceability Check):")
        rows = conn.execute("""
            SELECT metric_id, institution_id, reporting_period, value, source_document, source_page_number, confidence_score 
            FROM Fact_Financials 
            LIMIT 5
        """).fetchall()
        for row in rows:
            logger.info(f"  {row}")
            
    # Sample Unmapped
    if unmapped_count > 0:
        logger.info("Sample Unmapped Terms:")
        rows = conn.execute("SELECT raw_term, raw_value, institution_id, reporting_period FROM Unmapped_Staging LIMIT 5").fetchall()
        for row in rows:
            logger.info(f"  {row}")
            
    conn.close()

if __name__ == "__main__":
    check_db()
