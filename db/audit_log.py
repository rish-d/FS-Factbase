import duckdb
from loguru import logger

def perform_audit(db_path: str = "fs_factbase.duckdb"):
    """Performs mathematical and consistency audits on the financial data."""
    logger.info(f"Starting Data Integrity Audit on {db_path}...")
    conn = duckdb.connect(db_path)
    
    # Audit 1: Balance Sheet Sum-Check (Assets = Liabilities + Equity)
    # We need to pivot the data or use aggregations
    audit_query = """
    WITH ValuesByReport AS (
        SELECT 
            institution_id, 
            reporting_period,
            MAX(CASE WHEN metric_id = 'total_assets' THEN value END) as assets,
            MAX(CASE WHEN metric_id = 'total_liabilities' THEN value END) as liabilities,
            MAX(CASE WHEN metric_id = 'total_shareholders_equity' THEN value END) as equity
        FROM Fact_Financials
        GROUP BY 1, 2
    )
    SELECT 
        institution_id, 
        reporting_period, 
        assets, 
        (liabilities + equity) as calc_sum,
        ABS(assets - (liabilities + equity)) as variance
    FROM ValuesByReport
    WHERE assets IS NOT NULL AND liabilities IS NOT NULL AND equity IS NOT NULL
    """
    
    results = conn.execute(audit_query).fetchall()
    
    print("\n--- BALANCE SHEET AUDIT: Assets = Liabilities + Equity ---")
    if not results:
        print("No complete balance sheets found for audit.")
    else:
        print(f"{'Institution':<30} | {'Year':<6} | {'Assets':<15} | {'L+E Sum':<15} | {'Variance':<10}")
        print("-" * 85)
        for r in results:
            status = "PASS" if r[4] < 2.0 else "FAIL" # Rounding tolerance
            print(f"{r[0]:<30} | {r[1]:<6} | {r[2]:>15,.0f} | {r[3]:>15,.0f} | {r[4]:>10.0f} ({status})")

    # Audit 2: Verification Loop (Verbatim Samples)
    print("\n--- VERBATIM VERIFICATION SAMPLES ---")
    samples = conn.execute("""
        SELECT institution_id, reporting_period, metric_id, value, source_document, source_page_number 
        FROM Fact_Financials 
        ORDER BY RANDOM() 
        LIMIT 5
    """).fetchall()
    
    if not samples:
        print("No facts available for verification.")
    else:
        for s in samples:
            print(f"SAMPLE: {s[0]} ({s[1]}) -> {s[2]}: {s[3]:,.0f} (Source: {s[4]} p.{s[5]})")

    # Audit 3: Coverage Audit
    print("\n--- COVERAGE STATUS ---")
    counts = conn.execute("""
        SELECT institution_id, count(*) as records 
        FROM Fact_Financials 
        GROUP BY 1
    """).fetchall()
    for c in counts:
        print(f"BANK: {c[0]:<30} | Records: {c[1]}")

    conn.close()

if __name__ == "__main__":
    perform_audit()
