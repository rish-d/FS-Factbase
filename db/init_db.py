import duckdb
from loguru import logger

def init_database(db_path="fs_factbase.duckdb"):
    logger.info(f"Initializing DuckDB database at {db_path}...")
    
    # Connect (this will create the file if it doesn't exist)
    conn = duckdb.connect(db_path)
    
    # Create Core_Metrics Table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Core_Metrics (
            metric_id VARCHAR PRIMARY KEY,
            standardized_metric_name VARCHAR NOT NULL,
            accounting_standard VARCHAR,
            data_type VARCHAR NOT NULL
        );
    """)
    logger.info("Table created: Core_Metrics")
    
    # Create Metric_Aliases Table
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_alias_id;
        CREATE TABLE IF NOT EXISTS Metric_Aliases (
            alias_id INTEGER PRIMARY KEY DEFAULT nextval('seq_alias_id'),
            metric_id VARCHAR NOT NULL,
            raw_term VARCHAR NOT NULL,
            institution_id VARCHAR,
            FOREIGN KEY (metric_id) REFERENCES Core_Metrics(metric_id)
        );
    """)
    logger.info("Table created: Metric_Aliases")
    
    # Create Fact_Financials Table
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_fact_id;
        CREATE TABLE IF NOT EXISTS Fact_Financials (
            fact_id INTEGER PRIMARY KEY DEFAULT nextval('seq_fact_id'),
            metric_id VARCHAR NOT NULL,
            institution_id VARCHAR NOT NULL,
            reporting_period VARCHAR NOT NULL,
            value DOUBLE NOT NULL,
            source_document VARCHAR NOT NULL,
            source_page_number INTEGER NOT NULL,
            confidence_score DOUBLE DEFAULT 1.0,
            confidence_reason VARCHAR,
            FOREIGN KEY (metric_id) REFERENCES Core_Metrics(metric_id)
        );
    """)
    logger.info("Table created: Fact_Financials")
    
    # Create Unmapped_Staging Table
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_staging_id;
        CREATE TABLE IF NOT EXISTS Unmapped_Staging (
            staging_id INTEGER PRIMARY KEY DEFAULT nextval('seq_staging_id'),
            raw_term VARCHAR NOT NULL,
            raw_value DOUBLE NOT NULL,
            institution_id VARCHAR NOT NULL,
            reporting_period VARCHAR NOT NULL,
            source_document VARCHAR NOT NULL,
            source_page_number INTEGER NOT NULL,
            confidence_score DOUBLE DEFAULT 0.5,
            confidence_reason VARCHAR
        );
    """)
    logger.info("Table created: Unmapped_Staging")

    # Create Extraction_Corrections Table
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_correction_id;
        CREATE TABLE IF NOT EXISTS Extraction_Corrections (
            correction_id INTEGER PRIMARY KEY DEFAULT nextval('seq_correction_id'),
            institution_id VARCHAR NOT NULL,
            raw_term VARCHAR NOT NULL,
            original_value DOUBLE,
            corrected_value DOUBLE NOT NULL,
            source_document VARCHAR,
            page_number INTEGER,
            reason VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    logger.info("Table created: Extraction_Corrections")

    # Create Diagnostic_Lessons Table
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_lesson_id;
        CREATE TABLE IF NOT EXISTS Diagnostic_Lessons (
            lesson_id INTEGER PRIMARY KEY DEFAULT nextval('seq_lesson_id'),
            institution_id VARCHAR NOT NULL,
            error_pattern VARCHAR NOT NULL,
            advice VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        );
    """)
    logger.info("Table created: Diagnostic_Lessons")
    
    conn.close()
    logger.info("Database initialization completed successfully.")

if __name__ == "__main__":
    init_database()
