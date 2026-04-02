import duckdb
from loguru import logger

def init_database(db_path="fs_factbase.duckdb"):
    logger.info(f"Initializing DuckDB database at {db_path}...")
    
    # Connect (this will create the file if it doesn't exist)
    conn = duckdb.connect(db_path)
    
    # Create Institutions Table (Metadata Root)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Institutions (
            institution_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            sector VARCHAR NOT NULL, -- 'BANK', 'INSURANCE'
            country VARCHAR NOT NULL,
            base_currency VARCHAR NOT NULL,
            regulatory_regime VARCHAR,
            fiscal_year_end_month INTEGER DEFAULT 12
        );
    """)
    logger.info("Table created: Institutions")

    # Create Core_Metrics Table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Core_Metrics (
            metric_id VARCHAR PRIMARY KEY,
            standardized_metric_name VARCHAR NOT NULL,
            accounting_standard VARCHAR,
            sector VARCHAR DEFAULT 'universal', -- 'banking', 'insurance', 'universal'
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
            FOREIGN KEY (metric_id) REFERENCES Core_Metrics(metric_id),
            FOREIGN KEY (institution_id) REFERENCES Institutions(institution_id)
        );
    """)
    logger.info("Table created: Metric_Aliases")

    # Create Metric_Hierarchy Table (Additive Integrity)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Metric_Hierarchy (
            parent_metric_id VARCHAR NOT NULL,
            child_metric_id VARCHAR NOT NULL,
            weight DOUBLE DEFAULT 1.0, -- Usually 1.0 (addition), sometimes -1.0 (subtraction)
            PRIMARY KEY (parent_metric_id, child_metric_id),
            FOREIGN KEY (parent_metric_id) REFERENCES Core_Metrics(metric_id),
            FOREIGN KEY (child_metric_id) REFERENCES Core_Metrics(metric_id)
        );
    """)
    logger.info("Table created: Metric_Hierarchy")
    
    # Create Fact_Financials Table
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_fact_id;
        CREATE TABLE IF NOT EXISTS Fact_Financials (
            fact_id INTEGER PRIMARY KEY DEFAULT nextval('seq_fact_id'),
            metric_id VARCHAR NOT NULL,
            institution_id VARCHAR NOT NULL,
            reporting_period VARCHAR NOT NULL,
            value DOUBLE NOT NULL,
            currency_code VARCHAR,
            is_published BOOLEAN DEFAULT TRUE,
            formula_id VARCHAR,
            source_document VARCHAR NOT NULL,
            source_page_number INTEGER NOT NULL,
            confidence_score DOUBLE DEFAULT 1.0,
            confidence_reason VARCHAR,
            month_end INTEGER,
            is_cumulative BOOLEAN DEFAULT TRUE,
            scaling_factor INTEGER DEFAULT 1,
            FOREIGN KEY (metric_id) REFERENCES Core_Metrics(metric_id),
            FOREIGN KEY (institution_id) REFERENCES Institutions(institution_id),
            UNIQUE (institution_id, metric_id, reporting_period, is_published)
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
            confidence_reason VARCHAR,
            month_end INTEGER,
            is_cumulative BOOLEAN,
            scaling_factor INTEGER
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
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (institution_id) REFERENCES Institutions(institution_id)
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
            is_active BOOLEAN DEFAULT TRUE,
            FOREIGN KEY (institution_id) REFERENCES Institutions(institution_id)
        );
    """)
    logger.info("Table created: Diagnostic_Lessons")

    # Create Peer_Groups Table (User Defined)
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_group_id;
        CREATE TABLE IF NOT EXISTS Peer_Groups (
            group_id INTEGER PRIMARY KEY DEFAULT nextval('seq_group_id'),
            group_name VARCHAR NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    logger.info("Table created: Peer_Groups")

    # Create Peer_Group_Members Table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Peer_Group_Members (
            group_id INTEGER NOT NULL,
            institution_id VARCHAR NOT NULL,
            PRIMARY KEY (group_id, institution_id),
            FOREIGN KEY (group_id) REFERENCES Peer_Groups(group_id),
            FOREIGN KEY (institution_id) REFERENCES Institutions(institution_id)
        );
    """)
    logger.info("Table created: Peer_Group_Members")

    # Create Exchange_Rates Table (Query-Time Normalization)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Exchange_Rates (
            from_currency VARCHAR NOT NULL,
            to_currency VARCHAR NOT NULL,
            rate DOUBLE NOT NULL,
            as_of_date DATE DEFAULT CURRENT_DATE,
            PRIMARY KEY (from_currency, to_currency, as_of_date)
        );
    """)
    logger.info("Table created: Exchange_Rates")
    
    conn.close()
    logger.info("Database initialization completed successfully.")

if __name__ == "__main__":
    init_database()
