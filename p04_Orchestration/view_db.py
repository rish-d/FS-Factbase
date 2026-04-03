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
    print("\n--- 📊 FS Factbase Overview ---")
    for (table,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"Table: {table:<25} | Records: {count}")

    # Balance Sheet Audit (Asset = Liab + Equity)
    print("\n--- ⚖️ Balance Sheet Integrity Check (IFRS) ---")
    
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
    
    df = pl.from_arrow(conn.execute(query).arrow())
    
    if not df.is_empty():
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
    print("\n--- ⚠️ Top Unmapped Terms (Action Required) ---")
    unmapped = conn.execute("""
        SELECT raw_term, COUNT(*) as frequency
        FROM Unmapped_Staging
        GROUP BY raw_term
        ORDER BY frequency DESC
        LIMIT 10
    """).fetchall()
    
    for term, freq in unmapped:
        print(f"[{freq:3}] {term}")

    conn.close()

if __name__ == "__main__":
    get_db_summary()
