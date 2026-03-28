import duckdb
from loguru import logger

MAPPINGS = [
    # Balance Sheet
    ("total_assets", "Total Assets", "IFRS 9", "numeric"),
    ("gross_loans_advances_customers", "Gross Loans and Advances to Customers", "IFRS 9", "numeric"),
    ("total_customer_deposits", "Total Customer Deposits", "IFRS 9", "numeric"),
    ("total_equity", "Total Equity", "IFRS 9", "numeric"),
    ("allowance_expected_credit_losses", "Allowance for Impairment Losses / ECL", "IFRS 9", "numeric"),
    
    # Income Statement
    ("net_interest_income", "Net Interest Income", "IFRS 9", "numeric"),
    ("non_interest_income", "Non-Interest Income", "IFRS 9", "numeric"),
    ("total_operating_expenses", "Operating Expenses", "IFRS 9", "numeric"),
    ("impairment_charges_for_credit_losses", "Impairment Charges / Specific Provisions", "IFRS 9", "numeric"),
    ("profit_before_tax", "Profit Before Tax", "IFRS 9", "numeric"),
    ("net_profit_attributable_to_shareholders", "Net Profit Attributable to Shareholders", "IFRS 9", "numeric"),
    
    # Capital Adequacy
    ("cet1_ratio", "Common Equity Tier 1 (CET1) Ratio", "Basel III", "percentage"),
    ("cost_to_income_ratio", "Cost-to-Income Ratio", "Basel III", "percentage"),
    ("gross_impaired_loan_ratio", "Gross Impaired Loan (GIL) Ratio", "Basel III", "percentage"),
    ("net_interest_margin", "Net Interest Margin (NIM)", "Basel III", "percentage"),
]

def seed_database(db_path="fs_factbase.duckdb"):
    logger.info("Connecting to DuckDB for seeding...")
    conn = duckdb.connect(db_path)
    
    # Check if data already exists to make script idempotent
    result = conn.execute("SELECT count(*) FROM Core_Metrics").fetchone()
    if result[0] > 0:
        logger.warning(f"Core_Metrics already contains {result[0]} rows. Skipping seeding.")
        conn.close()
        return

    logger.info(f"Seeding {len(MAPPINGS)} core metrics into Core_Metrics table...")
    
    # Insert mappings
    conn.executemany(
        "INSERT INTO Core_Metrics (metric_id, standardized_metric_name, accounting_standard, data_type) VALUES (?, ?, ?, ?)",
        MAPPINGS
    )
    
    conn.close()
    logger.info("Database seeding completed.")

if __name__ == "__main__":
    seed_database()
