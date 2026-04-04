import duckdb
from loguru import logger
import os
import csv
import sys

# 100% IFRSAT-2025 COMPLIANT CORE METRIC DICTIONARY (65+ Metrics)
# IDs follow the official 'ifrs-full_' prefix from the 2025-03-27 taxonomy files.

MAPPINGS = [
    # --- L0: Universal Totals (The "Big Five") ---
    ("ifrs-full_Assets", "Total Assets", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_Liabilities", "Total Liabilities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_Equity", "Total Equity", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_EquityAndLiabilities", "Total Equity and Liabilities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ProfitLoss", "Profit or Loss (Net Profit)", "IFRS 2025", "universal", "numeric"),

    # --- L1: High-Level Components (Sub-Totals) ---
    ("ifrs-full_CurrentAssets", "Total Current Assets", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_NoncurrentAssets", "Total Non-Current Assets", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_CurrentLiabilities", "Total Current Liabilities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_NoncurrentLiabilities", "Total Non-Current Liabilities", "IFRS 2025", "universal", "numeric"),
    
    ("ifrs-full_ProfitLossBeforeTax", "Profit (Loss) Before Tax", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ProfitLossFromOperatingActivities", "Profit (Loss) from Operating Activities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_IncomeTaxExpenseIncome", "Income Tax Expense (Income)", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ProfitLossFromContinuingOperations", "Profit (Loss) from Continuing Operations", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ProfitLossFromDiscontinuedOperations", "Profit (Loss) from Discontinued Operations", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_OtherComprehensiveIncome", "Other Comprehensive Income (OCI)", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ComprehensiveIncome", "Total Comprehensive Income", "IFRS 2025", "universal", "numeric"),

    # --- L2: Banking Assets (The Core of the Factbase) ---
    ("ifrs-full_CashAndCashEquivalents", "Cash and Cash Equivalents", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_CashAndBalancesWithCentralBanks", "Cash and Balances with Central Banks", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DepositsAndPlacementsWithBanksAndOtherFinancialInstitutions", "Deposits and Placements with Banks", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_LoansAndAdvancesToCustomers", "Loans and Advances to Customers (Gross/Net)", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_LoansAndAdvancesToBanks", "Loans and Advances to Banks", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FinancialAssetsAtAmortisedCost", "Financial Assets at Amortised Cost", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FinancialAssetsAtFairValueThroughOtherComprehensiveIncome", "Financial Assets at FVOCI", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FinancialAssetsAtFairValueThroughProfitOrLoss", "Financial Assets at FVTPL", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DerivativeFinancialAssets", "Derivative Financial Assets", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_ReverseRepurchaseAgreements", "Reverse Repurchase Agreements", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_StatutoryDepositsWithCentralBanks", "Statutory Deposits with Central Banks", "IFRS 2025", "banking", "numeric"),
    
    # --- L2: Universal Assets ---
    ("ifrs-full_InvestmentProperty", "Investment Property", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_PropertyPlantAndEquipment", "Property, Plant and Equipment (PPE)", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_IntangibleAssetsAndGoodwill", "Intangible Assets and Goodwill", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_DeferredTaxAssets", "Deferred Tax Assets", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_CurrentTaxAssets", "Current Tax Assets", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_OtherAssets", "Other Assets", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_Inventories", "Inventories", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_TradeAndOtherReceivables", "Trade and Other Receivables", "IFRS 2025", "universal", "numeric"),

    # --- L2: Banking Liabilities ---
    ("ifrs-full_DepositsFromCustomers", "Deposits from Customers", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DepositsFromBanks", "Deposits and Placements of Banks", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DerivativeFinancialLiabilities", "Derivative Financial Liabilities", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DebtSecuritiesIssued", "Debt Securities Issued", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_SubordinatedLiabilities", "Subordinated Liabilities", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_RepurchaseAgreements", "Repurchase Agreements", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FinancialLiabilitiesAtFairValueThroughProfitOrLoss", "Financial Liabilities at FVTPL", "IFRS 2025", "banking", "numeric"),

    # --- L2: Universal Liabilities & Equity ---
    ("ifrs-full_Provisions", "Provisions", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_CurrentTaxLiabilities", "Current Tax Liabilities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_DeferredTaxLiabilities", "Deferred Tax Liabilities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_OtherLiabilities", "Other Liabilities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_TradeAndOtherPayables", "Trade and Other Payables", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_IssuedCapital", "Issued Capital", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_RetainedEarnings", "Retained Earnings", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_OtherReserves", "Other Reserves", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_NoncontrollingInterests", "Non-Controlling Interests", "IFRS 2025", "universal", "numeric"),

    # --- L2: Income Statement (Banking Focus) ---
    ("ifrs-full_InterestIncome", "Interest Income", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_InterestExpense", "Interest Expense", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_NetInterestIncome", "Net Interest Income", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FeeAndCommissionIncome", "Fee and Commission Income", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_FeeAndCommissionExpense", "Fee and Commission Expense", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_NetFeeAndCommissionIncome", "Net Fee and Commission Income", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_DividendIncome", "Dividend Income", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_GainsLossesOnForeignExchange", "Gains (Losses) on Foreign Exchange", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_NetGainsLossesOnFinancialAssetsAndLiabilitiesHeldForTrading", "Net Trading Income", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_OtherOperatingIncome", "Other Operating Income", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_Revenue", "Total Revenue / Income", "IFRS 2025", "universal", "numeric"),
    
    # --- L2: Expenses & Operating Deep-Dive ---
    ("ifrs-full_OperatingExpenses", "Total Operating Expenses", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_EmployeeBenefitsExpense", "Personnel / Staff Expenses", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_DepreciationAndAmortisationExpense", "Depreciation and Amortisation", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_AdministrativeExpense", "General Administrative Expenses", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_InformationTechnologyExpenses", "Information Technology Expenses", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_EstablishmentCosts", "Establishment / Premises Costs", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_MarketingAndAdvertisingExpense", "Marketing and Advertising", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_OtherOperatingExpenses", "Other Operating Expenses", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_FinanceIncome", "Finance Income", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_FinanceCosts", "Finance Costs", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_ShareOfProfitLossOfAssociatesAndJointVentures", "Share of Profit (Loss) of Associates", "IFRS 2025", "universal", "numeric"),

    # --- L3: Granular Components (Audit Depth) ---
    ("ifrs-full_InterestIncomeOnFinancialAssetsAtAmortisedCost", "Interest Income: Amortised Cost", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_InterestIncomeOnFinancialAssetsAtFairValueThroughOtherComprehensiveIncome", "Interest Income: FVOCI", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_InterestExpenseOnFinancialLiabilitiesAtAmortisedCost", "Interest Expense: Amortised Cost", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_ImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss", "ECL / Impairment Charges (P&L)", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_AllowanceAccountForCreditLossesOfFinancialAssets", "Total Allowance for Credit Losses (BS)", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_ExposuresToCreditRiskTwelvemonthECLMember", "ECL: Stage 1 (12-month)", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_ExposuresToCreditRiskLifetimeECLNotCreditimpairedMember", "ECL: Stage 2 (Lifetime Non-Impaired)", "IFRS 2025", "banking", "numeric"),
    ("ifrs-full_ExposuresToCreditRiskLifetimeECLCreditimpairedMember", "ECL: Stage 3 (Impaired)", "IFRS 2025", "banking", "numeric"),

    # --- Cash Flow Statement ---
    ("ifrs-full_CashFlowsFromUsedInOperatingActivities", "Net Cash from Operating Activities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_CashFlowsFromUsedInInvestingActivities", "Net Cash from Investing Activities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_CashFlowsFromUsedInFinancingActivities", "Net Cash from Financing Activities", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_IncreaseDecreaseInCashAndCashEquivalents", "Net Increase (Decrease) in Cash", "IFRS 2025", "universal", "numeric"),

    # --- Ratios & Meta ---
    ("ifrs-full_EarningsPerShare", "Earnings per Share", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_DividendsPaid", "Total Dividends Paid", "IFRS 2025", "universal", "numeric"),
    ("ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstanding", "Avg Shares Outstanding", "IFRS 2025", "universal", "numeric"),
]

# Hierarchical Relationships (Additive Integrity)
HIERARCHY = [
    # Top-Level Balance Sheet
    ("ifrs-full_EquityAndLiabilities", "ifrs-full_Equity", 1.0),
    ("ifrs-full_EquityAndLiabilities", "ifrs-full_Liabilities", 1.0),
    ("ifrs-full_Assets", "ifrs-full_CurrentAssets", 1.0),
    ("ifrs-full_Assets", "ifrs-full_NoncurrentAssets", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_CurrentLiabilities", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_NoncurrentLiabilities", 1.0),

    # Assets Breakdown (L2 into Assets or Current/Noncurrent)
    ("ifrs-full_Assets", "ifrs-full_CashAndBalancesWithCentralBanks", 1.0),
    ("ifrs-full_Assets", "ifrs-full_LoansAndAdvancesToCustomers", 1.0),
    ("ifrs-full_Assets", "ifrs-full_LoansAndAdvancesToBanks", 1.0),
    ("ifrs-full_Assets", "ifrs-full_FinancialAssetsAtAmortisedCost", 1.0),
    ("ifrs-full_Assets", "ifrs-full_FinancialAssetsAtFairValueThroughOtherComprehensiveIncome", 1.0),
    ("ifrs-full_Assets", "ifrs-full_FinancialAssetsAtFairValueThroughProfitOrLoss", 1.0),
    ("ifrs-full_Assets", "ifrs-full_DerivativeFinancialAssets", 1.0),
    ("ifrs-full_Assets", "ifrs-full_OtherAssets", 1.0),
    
    # Liabilities Breakdown
    ("ifrs-full_Liabilities", "ifrs-full_DepositsFromCustomers", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_DepositsFromBanks", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_DebtSecuritiesIssued", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_SubordinatedLiabilities", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_OtherLiabilities", 1.0),

    # Income Statement Arithmetic (Waterfall)
    ("ifrs-full_NetInterestIncome", "ifrs-full_InterestIncome", 1.0),
    ("ifrs-full_NetInterestIncome", "ifrs-full_InterestExpense", -1.0),
    ("ifrs-full_NetFeeAndCommissionIncome", "ifrs-full_FeeAndCommissionIncome", 1.0),
    ("ifrs-full_NetFeeAndCommissionIncome", "ifrs-full_FeeAndCommissionExpense", -1.0),
    
    # Profit Before Tax components
    ("ifrs-full_ProfitLossBeforeTax", "ifrs-full_ProfitLossFromOperatingActivities", 1.0),
    ("ifrs-full_ProfitLossBeforeTax", "ifrs-full_FinanceIncome", 1.0),
    ("ifrs-full_ProfitLossBeforeTax", "ifrs-full_FinanceCosts", -1.0),
    ("ifrs-full_ProfitLossBeforeTax", "ifrs-full_ShareOfProfitLossOfAssociatesAndJointVentures", 1.0),

    # Net Profit
    ("ifrs-full_ProfitLoss", "ifrs-full_ProfitLossBeforeTax", 1.0),
    ("ifrs-full_ProfitLoss", "ifrs-full_IncomeTaxExpenseIncome", -1.0),
]

FX_RATES = [
    ("MYR", "USD", 0.21, "2024-01-01"),
    ("SGD", "USD", 0.74, "2024-01-01"),
    ("USD", "MYR", 4.76, "2024-01-01"),
    ("SGD", "MYR", 3.52, "2024-01-01"),
]

# Ensure project root is in sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

try:
    from p02_Database_and_Mapping import db_config
except ImportError:
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

    # Custom Aliases Ingestion (Decoupled Persistence)
    custom_aliases_path = os.path.join(db_config.ROOT_DIR, "data", "dictionary", "custom_aliases.csv")
    if os.path.exists(custom_aliases_path):
        logger.info(f"Ingesting custom aliases from {custom_aliases_path}...")
        try:
            with open(custom_aliases_path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                custom_aliases = []
                for row in reader:
                    # Clean up empty strings to None (NULL in DB)
                    inst_id = row['institution_id'] if row['institution_id'] and row['institution_id'].strip() else None
                    custom_aliases.append((row['raw_term'], row['metric_id'], inst_id))
                
                if custom_aliases:
                    conn.executemany(
                        "INSERT OR IGNORE INTO Metric_Aliases (raw_term, metric_id, institution_id) VALUES (?, ?, ?)",
                        custom_aliases
                    )
                    logger.success(f"Seeded {len(custom_aliases)} custom aliases.")
        except Exception as e:
            logger.error(f"Failed to ingest custom aliases: {e}")
    else:
        logger.warning(f"Custom aliases file not found at {custom_aliases_path}")

    conn.close()
    logger.info("Database seeding (IFRS 2025 Phase) completed.")

if __name__ == "__main__":
    seed_database()
