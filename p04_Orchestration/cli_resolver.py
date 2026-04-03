import duckdb
from loguru import logger
import os
import sys

# Ensure project root is in sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from p02_Database_and_Mapping import db_config

def resolve_unmapped():
    db_path = db_config.get_db_path()
    if not os.path.exists(db_path):
        logger.error(f"Database not found at {db_path}")
        return

    conn = duckdb.connect(db_path)
    
    print("\n--- 🧩 CLI Metric Resolver ---")
    
    # Get top unmapped terms
    unmapped = conn.execute("""
        SELECT raw_term, COUNT(*) as frequency, institution_id, GROUP_CONCAT(DISTINCT reporting_period) as periods
        FROM Unmapped_Staging
        GROUP BY raw_term, institution_id
        ORDER BY frequency DESC
        LIMIT 20
    """).fetchall()

    if not unmapped:
        print("🎉 No unmapped terms found in staging! The pipeline is fully deterministic.")
        conn.close()
        return

    print(f"{'Freq':<6} | {'Raw Term':<40} | {'Institution':<30} | {'Periods'}")
    print("-" * 100)
    for freq, term, inst, periods in unmapped:
        print(f"{freq:<6} | {term[:40]:<40} | {inst[:30]:<30} | {periods}")

    print("\n[ACTION REQUIRED]")
    print("To resolve these, add them to 'Metric_Aliases' in p02/seed_data.py or use a helper script to bulk-insert them.")
    print("Example SQL: INSERT INTO Metric_Aliases (raw_term, metric_id, institution_id) VALUES ('Raw Term', 'ifrs-full_MetricID', 'INST_ID');")

    conn.close()

if __name__ == "__main__":
    resolve_unmapped()
