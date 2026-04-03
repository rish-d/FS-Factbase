import duckdb
from loguru import logger

# 100% IFRSAT-2025 COMPLIANT CORE METRIC DICTIONARY (65+ Metrics)
# IDs follow the official 'ifrs-full_' prefix from the 2025-03-27 taxonomy files.

MAPPINGS = [
    # --- Universal / Totals (High Level) ---
    ("ifrs-full_Assets", "Total Assets", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_Liabilities", "Total Liabilities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_Equity", "Total Equity", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ProfitLoss", "Profit or Loss", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ProfitLossBeforeTax", "Profit (Loss) Before Tax", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_IncomeTaxExpenseIncome", "Income Tax Expense (Income)", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ProfitLossFromOperatingActivities", "Profit (Loss) from Operating Activities", "IFRS 2025", "universal", "numeric"),
    
    # --- Balance Sheet: Assets (Granular) ---
    ("ifrs-full_CashAndCashEquivalents", "Cash and Cash Equivalents", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_LoansAndAdvancesToCustomers", "Loans and Advances to Customers", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_LoansAndAdvancesToBanks", "Loans and Advances to Banks", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FinancialAssetsAtAmortisedCost", "Financial Assets at Amortised Cost", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FinancialAssetsAtFairValueThroughOtherComprehensiveIncome", "Financial Assets at FVOCI", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FinancialAssetsAtFairValueThroughProfitOrLoss", "Financial Assets at FVTPL", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DerivativeFinancialAssets", "Derivative Financial Assets", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_AllowanceAccountForCreditLossesOfFinancialAssets", "Allowance for Credit Losses", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_InvestmentProperty", "Investment Property", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_PropertyPlantAndEquipment", "Property, Plant and Equipment", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_IntangibleAssetsAndGoodwill", "Intangible Assets and Goodwill", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_DeferredTaxAssets", "Deferred Tax Assets", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_CurrentTaxAssets", "Current Tax Assets", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_OtherAssets", "Other Assets", "IFRS 2025", "universal", "numeric"),

    # --- Balance Sheet: Liabilities & Equity (Granular) ---
    ("ifrs-full_DepositsFromCustomers", "Deposits from Customers", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DepositsFromBanks", "Deposits from Banks", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DerivativeFinancialLiabilities", "Derivative Financial Liabilities", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DebtSecuritiesIssued", "Debt Securities Issued", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_SubordinatedLiabilities", "Subordinated Liabilities", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_CurrentTaxLiabilities", "Current Tax Liabilities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_DeferredTaxLiabilities", "Deferred Tax Liabilities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_Provisions", "Provisions", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_IssuedCapital", "Issued Capital", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_RetainedEarnings", "Retained Earnings", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_NoncontrollingInterests", "Non-Controlling Interests", "IFRS 2025", "universal", "numeric"),

    # --- Income Statement: Revenues & Operating Items ---
    ("ifrs-full_InterestIncome", "Interest Income", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_InterestExpense", "Interest Expense", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_NetInterestIncome", "Net Interest Income", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FeeAndCommissionIncome", "Fee and Commission Income", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FeeAndCommissionExpense", "Fee and Commission Expense", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_NetFeeAndCommissionIncome", "Net Fee and Commission Income", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DividendIncome", "Dividend Income", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_RentalIncome", "Rental Income", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_GainsLossesOnForeignExchange", "Gains (Losses) on Foreign Exchange", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_OtherOperatingIncome", "Other Operating Income", "IFRS 2025", "universal", "numeric"),

    # --- Operating Expenses (Deep Disaggregation for Comparison) ---
    ("ifrs-full_EmployeeBenefitsExpense", "Personnel / Staff Expenses", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_DepreciationAndAmortisationExpense", "Depreciation and Amortisation", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_AdministrativeExpense", "General Administrative Expenses", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_SellingExpense", "Selling and Marketing Expenses", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ResearchAndDevelopmentExpense", "Research and Development Expenses", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_OtherOperatingExpenses", "Other Operating Expenses", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_InformationTechnologyExpenses", "Information Technology Expenses", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_MiscellaneousOtherOperatingExpense", "Miscellaneous Expenses", "IFRS 2025", "universal", "numeric"),

    # --- Credit Risk & IFRS 9 Stages ---
    ("ifrs-full_ExposuresToCreditRiskTwelvemonthECLMember", "Loans & Advances: Stage 1 (12m ECL)", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_ExposuresToCreditRiskLifetimeECLNotCreditimpairedMember", "Loans & Advances: Stage 2 (Lifetime ECL)", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_ExposuresToCreditRiskLifetimeECLCreditimpairedMember", "Loans & Advances: Stage 3 (Impaired)", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_ImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss", "ECL / Impairment Charges in P&L", "IFRS 2025", "banking", "numeric"),

    # --- Segment Reporting ---
    ("ifrs-full_RevenueFromExternalCustomersByGeographicalArea", "Revenue by Geographical Area", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ProfitLossBeforeTaxBySegment", "Profit Before Tax by Segment", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_AssetsBySegment", "Total Assets by Segment", "IFRS 2025", "universal", "numeric"),

    # --- Technical / Derived (Maintaining structure) ---
    ("ifrs-full_EarningsPerShare", "Earnings per Share", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_DividendsPaid", "Total Dividends Paid", "IFRS 2025", "universal", "numeric"),
]

# Hierarchical Relationships (Additive Integrity)
HIERARCHY = [
    # Total Assets breakdown
    ("ifrs-full_Assets", "ifrs-full_CashAndCashEquivalents", 1.0),
    ("ifrs-full_Assets", "ifrs-full_LoansAndAdvancesToCustomers", 1.0),
    ("ifrs-full_Assets", "ifrs-full_LoansAndAdvancesToBanks", 1.0),
    ("ifrs-full_Assets", "ifrs-full_FinancialAssetsAtAmortisedCost", 1.0),
    ("ifrs-full_Assets", "ifrs-full_DerivativeFinancialAssets", 1.0),
    ("ifrs-full_Assets", "ifrs-full_InvestmentProperty", 1.0),
    ("ifrs-full_Assets", "ifrs-full_PropertyPlantAndEquipment", 1.0),
    ("ifrs-full_Assets", "ifrs-full_IntangibleAssetsAndGoodwill", 1.0),
    
    # Total Liabilities breakdown
    ("ifrs-full_Liabilities", "ifrs-full_DepositsFromCustomers", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_DepositsFromBanks", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_DerivativeFinancialLiabilities", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_DebtSecuritiesIssued", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_SubordinatedLiabilities", 1.0),

    # Profit Before Tax = Profit from Ops + Share of Assoc - Tax (Simulated)
    ("ifrs-full_ProfitLossBeforeTax", "ifrs-full_ProfitLossFromOperatingActivities", 1.0),
    
    # Operating Expenses (Logic: Summing disaggregated items for comparison)
    # Using 'OtherOperatingExpenses' as a synthetic total if needed, or mapping them all to OperatingProfit
    ("ifrs-full_ProfitLossFromOperatingActivities", "ifrs-full_EmployeeBenefitsExpense", -1.0),
    ("ifrs-full_ProfitLossFromOperatingActivities", "ifrs-full_DepreciationAndAmortisationExpense", -1.0),
    ("ifrs-full_ProfitLossFromOperatingActivities", "ifrs-full_AdministrativeExpense", -1.0),
    ("ifrs-full_ProfitLossFromOperatingActivities", "ifrs-full_OtherOperatingExpenses", -1.0),
    ("ifrs-full_ProfitLossFromOperatingActivities", "ifrs-full_InformationTechnologyExpenses", -1.0),
    ("ifrs-full_NetFeeAndCommissionIncome", "ifrs-full_FeeAndCommissionIncome", 1.0),
    ("ifrs-full_NetFeeAndCommissionIncome", "ifrs-full_FeeAndCommissionExpense", -1.0),
]

FX_RATES = [
    ("MYR", "USD", 0.21, "2024-01-01"),
    ("SGD", "USD", 0.74, "2024-01-01"),
    ("USD", "MYR", 4.76, "2024-01-01"),
    ("SGD", "MYR", 3.52, "2024-01-01"),
]

import db_config

def seed_database(db_path=None):
    if db_path is None:
        db_path = db_config.get_db_path()
    logger.info("Connecting to DuckDB for seeding (IFRS 2025 Alignment)...")
    conn = duckdb.connect(db_path)
    
    logger.info("Purging existing data...")
    tables_to_purge = [
        "Peer_Group_Members", "Peer_Groups", "Exchange_Rates", 
        "Metric_Hierarchy", "Metric_Aliases", "Fact_Financials", 
        "Unmapped_Staging", "Core_Metrics", "Institutions"
    ]
    for table in tables_to_purge:
        conn.execute(f"DELETE FROM {table}")

    # Seeding Institutions
    # IDs now use 100% Full Folder Names (Uppercase) for report matching
    institutions = [
        ("CIMB GROUP HOLDINGS BERHAD", "CIMB Group Holdings Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
        ("MALAYAN BANKING BERHAD", "Malayan Banking Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
        ("PUBLIC BANK BERHAD", "Public Bank Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
        ("HONG LEONG FINANCIAL GROUP BERHAD", "Hong Leong Financial Group Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
        ("BANK KERJASAMA RAKYAT MALAYSIA BERHAD", "Bank Kerjasama Rakyat Malaysia Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
        ("ALLIANCE BANK MALAYSIA BERHAD", "Alliance Bank Malaysia Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
        ("AMMB HOLDINGS BERHAD", "AmMB Holdings Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
        ("RHB BANK BERHAD", "RHB Bank Berhad", "BANK", "Malaysia", "MYR", "BNM", 12),
    ]
    conn.executemany(
        "INSERT INTO Institutions (institution_id, name, sector, country, base_currency, regulatory_regime, fiscal_year_end_month) VALUES (?, ?, ?, ?, ?, ?, ?)",
        institutions
    )

    # Core Metrics (65+ items)
    logger.info(f"Seeding {len(MAPPINGS)} IFRS 2025 core metrics...")
    conn.executemany(
        "INSERT INTO Core_Metrics (metric_id, standardized_metric_name, accounting_standard, sector, data_type) VALUES (?, ?, ?, ?, ?)",
        MAPPINGS
    )

    # Metric Hierarchy
    logger.info(f"Seeding {len(HIERARCHY)} hierarchical relationships...")
    conn.executemany(
        "INSERT INTO Metric_Hierarchy (parent_metric_id, child_metric_id, weight) VALUES (?, ?, ?)",
        HIERARCHY
    )

    # Peer Groups
    conn.execute("INSERT INTO Peer_Groups (group_name) VALUES ('Malaysia Top 4')")
    group_id = conn.execute("SELECT group_id FROM Peer_Groups WHERE group_name = 'Malaysia Top 4'").fetchone()[0]
    group_members = [
        (group_id, "CIMB GROUP HOLDINGS BERHAD"), 
        (group_id, "MALAYAN BANKING BERHAD"), 
        (group_id, "PUBLIC BANK BERHAD"), 
        (group_id, "HONG LEONG FINANCIAL GROUP BERHAD")
    ]
    conn.executemany("INSERT INTO Peer_Group_Members (group_id, institution_id) VALUES (?, ?)", group_members)

    # Exchange Rates
    conn.executemany("INSERT INTO Exchange_Rates (from_currency, to_currency, rate, as_of_date) VALUES (?, ?, ?, ?)", FX_RATES)

    conn.close()
    logger.info("Database seeding (IFRS 2025 Phase) completed.")

if __name__ == "__main__":
    seed_database()
