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
            source_page_number INTEGER NOT NULL
        );
    """)
    logger.info("Table created: Unmapped_Staging")
    
    conn.close()
    logger.info("Database initialization completed successfully.")

if __name__ == "__main__":
    init_database()
