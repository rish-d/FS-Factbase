import duckdb
from loguru import logger
import db_config
import os

def migrate_database():
    db_path = db_config.get_db_path()
    if not os.path.exists(db_path):
        logger.error(f"Database not found at {db_path}. Run init_db.py first.")
        return

    conn = duckdb.connect(db_path)
    logger.info(f"Connected to {db_path} for migration.")

    try:
        # Start transaction
        conn.execute("BEGIN TRANSACTION")

        # 1. Update Metric_Aliases
        logger.info("Updating Metric_Aliases...")
        conn.execute("ALTER TABLE Metric_Aliases ADD COLUMN batch_id VARCHAR;")
        conn.execute("ALTER TABLE Metric_Aliases ADD COLUMN is_ai_generated BOOLEAN DEFAULT FALSE;")

        # 2. Update Fact_Financials
        logger.info("Updating Fact_Financials...")
        conn.execute("ALTER TABLE Fact_Financials ADD COLUMN batch_id VARCHAR;")

        # 3. Create AI_Resolution_Log
        logger.info("Creating AI_Resolution_Log table...")
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_ai_log_id;
            CREATE TABLE IF NOT EXISTS AI_Resolution_Log (
                log_id INTEGER PRIMARY KEY DEFAULT nextval('seq_ai_log_id'),
                batch_id VARCHAR NOT NULL,
                raw_term VARCHAR NOT NULL,
                metric_id VARCHAR NOT NULL,
                institution_id VARCHAR NOT NULL,
                reporting_period VARCHAR NOT NULL,
                raw_value DOUBLE,
                source_document VARCHAR,
                source_page_number INTEGER,
                confidence_score DOUBLE,
                confidence_reason VARCHAR,
                month_end INTEGER,
                is_cumulative BOOLEAN,
                scaling_factor INTEGER,
                statement_type VARCHAR,
                entity_scope VARCHAR,
                resolved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.execute("COMMIT")
        logger.success("Migration completed successfully.")
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Migration failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database()
