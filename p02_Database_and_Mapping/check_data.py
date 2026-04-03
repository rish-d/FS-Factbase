import duckdb
from loguru import logger
import db_config

def check_results(db_path=None):
    if db_path is None:
        db_path = db_config.get_db_path()
    conn = duckdb.connect(db_path)
    
    logger.info("--- Fact_Financials (Standardized Data) ---")
    rows_fact = conn.execute("SELECT * FROM Fact_Financials").fetchall()
    if not rows_fact:
        print("No data in Fact_Financials.")
    for row in rows_fact:
        print(row)
    
    logger.info("--- Unmapped_Staging ---")
    rows_staging = conn.execute("SELECT * FROM Unmapped_Staging").fetchall()
    if not rows_staging:
        print("No data in Unmapped_Staging.")
    for row in rows_staging:
        print(row)
    
    conn.close()

if __name__ == "__main__":
    check_results()
