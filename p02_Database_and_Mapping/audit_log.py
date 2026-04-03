import duckdb
import os
from loguru import logger

def perform_audit(db_path: str = None):
    """Performs mathematical and consistency audits on the financial data using IFRS standards."""
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fs_factbase.duckdb")
        
    logger.info(f"Starting Comprehensive Data Integrity Audit on {db_path}...")
    if not os.path.exists(db_path):
        logger.error(f"Database not found at {db_path}")
        return
        
    conn = duckdb.connect(db_path)
    threshold = 10.0 # Tolerance for rounding in absolute terms
    
    # 1. BALANCE SHEET AUDIT (Assets = Liabilities + Equity)
    audit_bs = """
    WITH ValuesByReport AS (
        SELECT 
            institution_id, 
            reporting_period,
            MAX(CASE WHEN metric_id = 'ifrs-full_Assets' THEN value END) as assets,
            MAX(CASE WHEN metric_id = 'ifrs-full_Liabilities' THEN value END) as liabilities,
            MAX(CASE WHEN metric_id = 'ifrs-full_Equity' THEN value END) as equity
        FROM Fact_Financials
        GROUP BY 1, 2
    )
    SELECT 
        institution_id, 
        reporting_period, 
        assets, 
        (COALESCE(liabilities, 0) + COALESCE(equity, 0)) as calc_sum,
        ABS(assets - (COALESCE(liabilities, 0) + COALESCE(equity, 0))) as variance
    FROM ValuesByReport
    WHERE assets IS NOT NULL
    """
    
    # 2. NET INTEREST INCOME AUDIT
    audit_nii = """
    WITH NIIValues AS (
        SELECT 
            institution_id, 
            reporting_period,
            MAX(CASE WHEN metric_id = 'ifrs-full_NetInterestIncome' THEN value END) as nii,
            MAX(CASE WHEN metric_id = 'ifrs-full_InterestIncome' THEN value END) as inc,
            MAX(CASE WHEN metric_id = 'ifrs-full_InterestExpense' THEN value END) as exp
        FROM Fact_Financials
        GROUP BY 1, 2
    )
    SELECT 
        institution_id, 
        reporting_period, 
        nii, 
        (COALESCE(inc, 0) - COALESCE(exp, 0)) as calc_nii,
        ABS(nii - (COALESCE(inc, 0) - COALESCE(exp, 0))) as variance
    FROM NIIValues
    WHERE nii IS NOT NULL
    """

    # 3. HIERARCHICAL ADDITIVE INTEGRITY (Generic)
    audit_hierarchy = """
    WITH ParentValue AS (
        SELECT institution_id, reporting_period, metric_id, value as parent_val
        FROM Fact_Financials
    ),
    ChildrenSum AS (
        SELECT f.institution_id, f.reporting_period, h.parent_metric_id, SUM(f.value * h.weight) as children_sum
        FROM Fact_Financials f
        JOIN Metric_Hierarchy h ON f.metric_id = h.child_metric_id
        GROUP BY 1, 2, 3
    )
    SELECT 
        p.institution_id, 
        p.reporting_period, 
        p.metric_id,
        p.parent_val, 
        c.children_sum,
        ABS(p.parent_val - c.children_sum) as variance
    FROM ParentValue p
    JOIN ChildrenSum c ON p.institution_id = c.institution_id 
        AND p.reporting_period = c.reporting_period 
        AND p.metric_id = c.parent_metric_id
    WHERE ABS(p.parent_val - c.children_sum) > 10.0
    """

    print("\n" + "="*100)
    print(f"{'DATA INTEGRITY AUDIT REPORT':^100}")
    print("="*100)

    # Executing BS Audit
    print("\n[CHECK 1] BALANCE SHEET: Assets = Liabilities + Equity")
    results = conn.execute(audit_bs).fetchall()
    if not results:
        print(">> No sufficient balance sheet data found.")
    else:
        print(f"{'Institution':<40} | {'Year':<6} | {'Assets':<15} | {'L+E Sum':<15} | {'Status'}")
        print("-" * 100)
        for r in results:
            status = "PASS" if r[4] < threshold else f"FAIL (Var: {r[4]:,.0f})"
            print(f"{r[0]:<40} | {r[1]:<6} | {r[2]:>15,.0f} | {r[3]:>15,.0f} | {status}")

    # Executing NII Audit
    print("\n[CHECK 2] INCOME STATEMENT: Net Interest Income = Inc - Exp")
    results = conn.execute(audit_nii).fetchall()
    if not results:
        print(">> No sufficient NII data found.")
    else:
        print(f"{'Institution':<40} | {'Year':<6} | {'Reported NII':<15} | {'Calc NII':<15} | {'Status'}")
        print("-" * 100)
        for r in results:
            status = "PASS" if r[4] < threshold else f"FAIL (Var: {r[4]:,.0f})"
            print(f"{r[0]:<40} | {r[1]:<6} | {r[2]:>15,.0f} | {r[3]:>15,.0f} | {status}")

    # Executing Hierarchy Audit
    print("\n[CHECK 3] HIERARCHY: Detailed Parent-Child Disaggregation")
    results = conn.execute(audit_hierarchy).fetchall()
    if not results:
        print(">> All hierarchical relationships passed consistency check.")
    else:
        print(f"{'Institution':<25} | {'Year':<6} | {'Parent Metric':<35} | {'Reported':<12} | {'Calced':<12}")
        print("-" * 100)
        for r in results:
            print(f"{r[0][:25]:<25} | {r[1]:<6} | {r[2][:35]:<35} | {r[3]:>12,.0f} | {r[4]:>12,.0f}")

    # Coverage Status
    print("\n" + "-"*100)
    print(f"{'COVERAGE STATUS':^100}")
    print("-"*100)
    counts = conn.execute("SELECT institution_id, count(*) as records FROM Fact_Financials GROUP BY 1 ORDER BY 2 DESC").fetchall()
    for c in counts:
        print(f"BANK: {c[0]:<40} | Total Fact Count: {c[1]}")

    conn.close()
    print("\nAudit completed.")

if __name__ == "__main__":
    perform_audit()
