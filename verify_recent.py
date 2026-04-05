import duckdb
import time

db_path = r"d:\FS Factbase\fs_factbase.duckdb"

def get_counts():
    conn = duckdb.connect(db_path)
    fact_count = conn.execute("SELECT COUNT(*) FROM Fact_Financials").fetchone()[0]
    staging_count = conn.execute("SELECT COUNT(*) FROM Unmapped_Staging").fetchone()[0]
    conn.close()
    return fact_count, staging_count

print("📊 Checking record counts...")
f1, s1 = get_counts()
print(f"Initial - Fact_Financials: {f1}, Unmapped_Staging: {s1}")

print("⏳ Waiting 30 seconds for expansion...")
time.sleep(30)

f2, s2 = get_counts()
print(f"Current - Fact_Financials: {f2}, Unmapped_Staging: {s2}")

if f2 > f1 or s2 > s1:
    print("✅ The factbase is expanding!")
else:
    print("⚠️ No change in record counts detected.")
