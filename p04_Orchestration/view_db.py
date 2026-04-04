import duckdb
import polars as pl
from loguru import logger
import os
import sys

# Ensure project root is in sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from p02_Database_and_Mapping import db_config

def get_db_summary():
    db_path = db_config.get_db_path()
    if not os.path.exists(db_path):
        logger.error(f"Database not found at {db_path}")
        return

    conn = duckdb.connect(db_path)
    
    # Tables summary
    tables = conn.execute("SHOW TABLES").fetchall()
    print("\n--- [STATUS] FS Factbase Overview ---")
    for (table,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"Table: {table:<25} | Records: {count}")

    # Balance Sheet Audit (Asset = Liab + Equity)
    print("\n--- [AUDIT] Balance Sheet Integrity Check (IFRS) ---")
    
    query = """
    SELECT 
        institution_id, 
        reporting_period,
        SUM(CASE WHEN metric_id = 'ifrs-full_Assets' THEN value ELSE 0 END) as Total_Assets,
        SUM(CASE WHEN metric_id = 'ifrs-full_Liabilities' THEN value ELSE 0 END) as Total_Liabilities,
        SUM(CASE WHEN metric_id = 'ifrs-full_Equity' THEN value ELSE 0 END) as Total_Equity
    FROM Fact_Financials
    GROUP BY institution_id, reporting_period
    ORDER BY institution_id, reporting_period DESC
    """
    
    results = conn.execute(query).arrow().read_all()
    
    if results.num_rows > 0:
        df = pl.from_arrow(results)
        df = df.with_columns([
            (pl.col("Total_Liabilities") + pl.col("Total_Equity")).alias("Liab_Plus_Eq")
        ])
        df = df.with_columns([
            (pl.col("Total_Assets") - pl.col("Liab_Plus_Eq")).alias("Variance")
        ])
        print(df)
    else:
        print("No mapped financial data available for integrity check.")

    # Unmapped Terms Priority
    print("\n--- [WARNING] Top Unmapped Terms (Action Required) ---")
    unmapped = conn.execute("""
        SELECT raw_term, COUNT(*) as frequency
        FROM Unmapped_Staging
        GROUP BY raw_term
        ORDER BY frequency DESC
        LIMIT 10
    """).fetchall()
    
    for term, freq in unmapped:
        print(f"[{freq:3}] {term}")

    # Core Metric Dictionary
    print("\n--- [HELP] Core Metric Dictionary (IFRS 2025) ---")
    metrics = conn.execute("SELECT standardized_metric_name FROM Core_Metrics ORDER BY standardized_metric_name").fetchall()
    for i, (m_name,) in enumerate(metrics):
        print(f"{i+1:2}. {m_name}")
    
    conn.close()

if __name__ == "__main__":
    get_db_summary()
