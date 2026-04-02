import duckdb
from loguru import logger

MAPPINGS = [
    # Universal Metrics
    ("total_assets", "Total Assets", "IFRS 9 / 17", "universal", "numeric"),
    ("total_liabilities", "Total Liabilities", "IFRS 9 / 17", "universal", "numeric"),
    ("total_equity", "Total Equity", "IFRS 9 / 17", "universal", "numeric"),
    ("revenue", "Total Revenue / Income", "IFRS 9 / 17", "universal", "numeric"),
    ("net_profit_attributable_to_shareholders", "Net Profit Attributable to Shareholders", "IFRS 9 / 17", "universal", "numeric"),
    ("profit_before_tax", "Profit Before Tax", "IFRS 9 / 17", "universal", "numeric"),
    ("total_operating_expenses", "Operating Expenses", "IFRS 9 / 17", "universal", "numeric"),
    ("dividend_per_share", "Dividend per Share", "IFRS 9 / 17", "universal", "numeric"),
    ("earnings_per_share", "Earnings per Share", "IFRS 9 / 17", "universal", "numeric"),
    ("return_on_equity", "Return on Equity", "Derived", "universal", "percentage"),
    ("return_on_assets", "Return on Assets", "Derived", "universal", "percentage"),

    # Banking Specific (IFRS 9 / Basel III)
    ("gross_loans_advances_customers", "Gross Loans and Advances to Customers", "IFRS 9", "banking", "numeric"),
    ("allowance_expected_credit_losses", "Allowance for Impairment Losses and Expected Credit Losses", "IFRS 9", "banking", "numeric"),
    ("net_interest_income", "Net Interest Income", "IFRS 9", "banking", "numeric"),
    ("non_interest_income", "Non-Interest Income", "IFRS 9", "banking", "numeric"),
    ("impairment_charges_for_credit_losses", "Impairment Charges and Specific Provisions", "IFRS 9", "banking", "numeric"),
    ("cet1_ratio", "Common Equity Tier 1 Ratio", "Basel III", "banking", "percentage"),
    ("tier_1_capital_ratio", "Tier 1 Capital Ratio", "Basel III", "banking", "percentage"),
    ("total_capital_ratio", "Total Capital Ratio", "Basel III", "banking", "percentage"),
    ("total_risk_weighted_assets", "Total Risk Weighted Assets", "Basel III", "banking", "numeric"),
    ("cost_to_income_ratio", "Cost-to-Income Ratio", "Basel III", "banking", "percentage"),
    ("gross_impaired_loan_ratio", "Gross Impaired Loan Ratio", "Basel III", "banking", "percentage"),
    ("net_interest_margin", "Net Interest Margin", "Basel III", "banking", "percentage"),
    ("loan_to_deposit_ratio", "Loan-to-Deposit Ratio", "Basel III", "banking", "percentage"),
    ("liquidity_coverage_ratio", "Liquidity Coverage Ratio", "Basel III", "banking", "percentage"),
    ("net_stable_funding_ratio", "Net Stable Funding Ratio", "Basel III", "banking", "percentage"),

    # Insurance Specific (IFRS 17)
    ("insurance_revenue", "Insurance Revenue", "IFRS 17", "insurance", "numeric"),
    ("insurance_service_result", "Insurance Service Result", "IFRS 17", "insurance", "numeric"),
    ("insurance_contract_liabilities", "Insurance Contract Liabilities", "IFRS 17", "insurance", "numeric"),
    ("contractual_service_margin", "Contractual Service Margin", "IFRS 17", "insurance", "numeric"),
    ("reinsurance_contract_assets", "Reinsurance Contract Assets", "IFRS 17", "insurance", "numeric"),
    ("combined_ratio", "Combined Ratio", "IFRS 17", "insurance", "percentage"),
    ("loss_ratio", "Loss Ratio", "IFRS 17", "insurance", "percentage"),

    # Asset Components (Level 1)
    ("cash_and_short_term_funds", "Cash and Short-Term Funds", "IFRS 9", "banking", "numeric"),
    ("deposits_with_banks", "Deposits and Placements with Banks and Other Financial Institutions", "IFRS 9", "banking", "numeric"),
    ("financial_investments", "Financial Investments (at Fair Value or Amortised Cost)", "IFRS 9", "banking", "numeric"),
    ("loans_advances_financing", "Net Loans, Advances and Financing", "IFRS 9", "banking", "numeric"),
    ("derivative_financial_assets", "Derivative Financial Assets", "IFRS 9", "banking", "numeric"),
    ("statutory_deposits_with_central_bank", "Statutory Deposits with Central Bank", "IFRS 9", "banking", "numeric"),
    ("deferred_tax_assets", "Deferred Tax Assets", "IFRS 9", "banking", "numeric"),
    ("property_plant_equipment", "Property, Plant and Equipment", "IFRS 9", "banking", "numeric"),
    ("other_assets", "Other Assets", "IFRS 9 / 17", "universal", "numeric"),

    # Liability Components (Level 1)
    ("total_customer_deposits", "Deposits from Customers", "IFRS 9", "banking", "numeric"),
    ("deposits_from_banks", "Deposits and Placements of Banks and Other Financial Institutions", "IFRS 9", "banking", "numeric"),
    ("derivative_financial_liabilities", "Derivative Financial Liabilities", "IFRS 9", "banking", "numeric"),
    ("debt_securities_issued", "Debt Securities Issued", "IFRS 9", "banking", "numeric"),
    ("other_liabilities", "Other Liabilities", "IFRS 9 / 17", "universal", "numeric"),
]

# Hierarchical Relationships (Parent, Child, Weight)
HIERARCHY = [
    # Total Assets = Sum of Level 1 Components
    ("total_assets", "cash_and_short_term_funds", 1.0),
    ("total_assets", "deposits_with_banks", 1.0),
    ("total_assets", "financial_investments", 1.0),
    ("total_assets", "loans_advances_financing", 1.0),
    ("total_assets", "derivative_financial_assets", 1.0),
    ("total_assets", "statutory_deposits_with_central_bank", 1.0),
    ("total_assets", "property_plant_equipment", 1.0),
    ("total_assets", "other_assets", 1.0),

    # Total Liabilities = Sum of Level 1 Components
    ("total_liabilities", "total_customer_deposits", 1.0),
    ("total_liabilities", "deposits_from_banks", 1.0),
    ("total_liabilities", "derivative_financial_liabilities", 1.0),
    ("total_liabilities", "debt_securities_issued", 1.0),
    ("total_liabilities", "other_liabilities", 1.0),
]

# Exchange Rates (From, To, Rate, AsOf)
FX_RATES = [
    # Baseline 2024 rates
    ("MYR", "USD", 0.21, "2024-01-01"),
    ("SGD", "USD", 0.74, "2024-01-01"),
    ("USD", "MYR", 4.76, "2024-01-01"),
    ("SGD", "MYR", 3.52, "2024-01-01"),
]

def seed_database(db_path="fs_factbase.duckdb"):
    logger.info("Connecting to DuckDB for seeding...")
    conn = duckdb.connect(db_path)
    
    # Purge existing data to ensure a clean refresh (Incremental Growth focus)
    logger.info("Purging existing data to handle foreign key constraints...")
    conn.execute("DELETE FROM Peer_Group_Members")
    conn.execute("DELETE FROM Peer_Groups")
    conn.execute("DELETE FROM Exchange_Rates")
    conn.execute("DELETE FROM Metric_Hierarchy")
    conn.execute("DELETE FROM Metric_Aliases")
    conn.execute("DELETE FROM Fact_Financials")
    conn.execute("DELETE FROM Unmapped_Staging")
    conn.execute("DELETE FROM Core_Metrics")
    conn.execute("DELETE FROM Institutions")

    # Seeding Institutions
    logger.info("Seeding Initial Institutions (Malaysia)...")
    institutions = [
        ("cimb", "CIMB Group Holdings Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
        ("maybank", "Malayan Banking Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
        ("public_bank", "Public Bank Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
        ("hlfg", "Hong Leong Financial Group", "BANK", "Malaysia", "MYR", "BNM", 12),
    ]
    conn.executemany(
        "INSERT INTO Institutions (institution_id, name, sector, country, base_currency, regulatory_regime, fiscal_year_end_month) VALUES (?, ?, ?, ?, ?, ?, ?)",
        institutions
    )

    # Core Metrics
    logger.info(f"Seeding {len(MAPPINGS)} core metrics into Core_Metrics table...")
    conn.executemany(
        "INSERT INTO Core_Metrics (metric_id, standardized_metric_name, accounting_standard, sector, data_type) VALUES (?, ?, ?, ?, ?)",
        MAPPINGS
    )

    # Metric Hierarchy
    logger.info(f"Seeding {len(HIERARCHY)} hierarchical relationships into Metric_Hierarchy table...")
    conn.executemany(
        "INSERT INTO Metric_Hierarchy (parent_metric_id, child_metric_id, weight) VALUES (?, ?, ?)",
        HIERARCHY
    )

    # Peer Groups
    logger.info("Seeding Peer Groups (Malaysia Top 4)...")
    conn.execute("INSERT INTO Peer_Groups (group_name) VALUES ('Malaysia Top 4')")
    group_id = conn.execute("SELECT group_id FROM Peer_Groups WHERE group_name = 'Malaysia Top 4'").fetchone()[0]
    
    group_members = [(group_id, "cimb"), (group_id, "maybank"), (group_id, "public_bank"), (group_id, "hlfg")]
    conn.executemany("INSERT INTO Peer_Group_Members (group_id, institution_id) VALUES (?, ?)", group_members)

    # Exchange Rates
    logger.info("Seeding Exchange Rates...")
    conn.executemany("INSERT INTO Exchange_Rates (from_currency, to_currency, rate, as_of_date) VALUES (?, ?, ?, ?)", FX_RATES)

    conn.close()
    logger.info("Database seeding (Phase C) completed.")

if __name__ == "__main__":
    seed_database()
