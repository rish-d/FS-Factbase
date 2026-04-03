import duckdb
import os

db_path = r"d:\FS Factbase\fs_factbase.duckdb"
conn = duckdb.connect(db_path)

print("🔍 Verifying malayan_banking_berhad (2022) in Fact_Financials...")
results = conn.execute("SELECT metric_id, reporting_period, value, source_page_number FROM Fact_Financials WHERE institution_id = 'malayan_banking_berhad' AND reporting_period = '2022'").fetchall()
for row in results:
    print(f"Fact: {row}")

print("\n🔍 Verifying malayan_banking_berhad (2022) in Unmapped_Staging...")
staging = conn.execute("SELECT raw_term, raw_value, reporting_period, source_page_number FROM Unmapped_Staging WHERE institution_id = 'malayan_banking_berhad' AND reporting_period = '2022'").fetchall()
for row in staging:
    print(f"Staging: {row}")

conn.close()
