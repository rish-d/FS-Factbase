import duckdb
from loguru import logger

# SEED ALIASES FOR IFRS 2025 ALIGNMENT
# This maps common raw terms from Malaysian bank reports to the standardized IFRS IDs.

ALIASES = [
    # --- Assets ---
    ("ifrs-full_Assets", "Total Assets", None),
    ("ifrs-full_Assets", "Total assets", None),
    ("ifrs-full_CashAndCashEquivalents", "Cash and short-term funds", None),
    ("ifrs-full_CashAndCashEquivalents", "Cash and bank balances", None),
    
    ("ifrs-full_LoansAndAdvancesToCustomers", "Loans, advances and financing", None),
    ("ifrs-full_LoansAndAdvancesToCustomers", "Gross loans, advances and financing", None),
    ("ifrs-full_LoansAndAdvancesToCustomers", "Net loans, advances and financing", None),
    
    ("ifrs-full_FinancialAssetsAtAmortisedCost", "Financial investments at amortised cost", None),
    ("ifrs-full_FinancialAssetsAtFairValueThroughOtherComprehensiveIncome", "Financial investments at FVOCI", None),
    ("ifrs-full_FinancialAssetsAtFairValueThroughProfitOrLoss", "Financial assets at FVTPL", None),
    
    ("ifrs-full_PropertyPlantAndEquipment", "Property, plant and equipment", None),
    ("ifrs-full_IntangibleAssetsAndGoodwill", "Intangible assets", None),
    ("ifrs-full_IntangibleAssetsAndGoodwill", "Goodwill", None),
    
    # --- Liabilities & Equity ---
    ("ifrs-full_DepositsFromCustomers", "Deposits from customers", None),
    ("ifrs-full_DepositsFromCustomers", "Customer deposits", None),
    ("ifrs-full_DepositsFromCustomers", "Current accounts", None),
    ("ifrs-full_DepositsFromCustomers", "Savings accounts", None),
    ("ifrs-full_DepositsFromCustomers", "Fixed deposits", None),
    ("ifrs-full_DepositsFromCustomers", "Negotiable instruments of deposits", None),
    ("ifrs-full_DepositsFromBanks", "Deposits and placements of banks and other financial institutions", None),
    ("ifrs-full_DepositsFromBanks", "Deposits from banks", None),
    
    ("ifrs-full_Liabilities", "Total Liabilities", None),
    ("ifrs-full_Liabilities", "Total liabilities", None),
    ("ifrs-full_Equity", "Shareholders' equity", None),
    ("ifrs-full_Equity", "Total equity", None),
    ("ifrs-full_Equity", "Total Equity", None),
    ("ifrs-full_IssuedCapital", "Share capital", None),
    
    # --- Income Statement ---
    ("ifrs-full_ProfitLoss", "Profit for the financial year", None),
    ("ifrs-full_ProfitLoss", "Net profit for the year", None),
    ("ifrs-full_ProfitLossBeforeTax", "Profit before taxation", None),
    ("ifrs-full_ProfitLossBeforeTax", "Profit before tax", None),
    ("ifrs-full_InterestIncome", "Interest income", None),
    
    ("ifrs-full_FeeAndCommissionIncome", "Fee and commission income", None),
    ("ifrs-full_FeeAndCommissionExpense", "Fee and commission expense", None),
    ("ifrs-full_NetFeeAndCommissionIncome", "Net fee and commission income", None),
    
    ("ifrs-full_OtherOperatingIncome", "Other operating income", None),
    ("ifrs-full_OtherOperatingIncome", "Non-interest income", None),
    
    # --- Operating Expenses (Deep Comparison) ---
    ("ifrs-full_EmployeeBenefitsExpense", "Personnel costs", None),
    ("ifrs-full_EmployeeBenefitsExpense", "Staff costs", None),
    ("ifrs-full_EmployeeBenefitsExpense", "Employee benefits expense", None),
    ("ifrs-full_EmployeeBenefitsExpense", "Salaries, allowances and bonuses", None),
    
    ("ifrs-full_DepreciationAndAmortisationExpense", "Depreciation and amortisation", None),
    ("ifrs-full_DepreciationAndAmortisationExpense", "Depreciation of property, plant and equipment", None),
    ("ifrs-full_DepreciationAndAmortisationExpense", "Amortisation of intangible assets", None),
    
    ("ifrs-full_AdministrativeExpense", "Other overheads and expenditures", None),
    ("ifrs-full_AdministrativeExpense", "General and administrative expenses", None),
    ("ifrs-full_AdministrativeExpense", "Establishment costs", None),
    ("ifrs-full_AdministrativeExpense", "Occupancy costs", None),
    ("ifrs-full_AdministrativeExpense", "Rental of premises", None),
    
    ("ifrs-full_OtherOperatingExpenses", "Shared service costs", None),
    
    # --- IT Specific (IFRS 2025 Granularity) ---
    ("ifrs-full_InformationTechnologyExpenses", "Information technology costs", None),
    ("ifrs-full_InformationTechnologyExpenses", "IT Expenses", None),
    ("ifrs-full_InformationTechnologyExpenses", "Computerisation costs", None),
    ("ifrs-full_InformationTechnologyExpenses", "Technological upkeep", None),
    ("ifrs-full_InformationTechnologyExpenses", "Software maintenance and licensing", None),
    ("ifrs-full_InformationTechnologyExpenses", "IT outsourcing fees", None),
    
    # --- Credit Risk ---
    ("ifrs-full_ImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss", "Allowance for impairment losses on loans, advances and financing", None),
    ("ifrs-full_ImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss", "Credit loss expense", None),
    ("ifrs-full_ImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss", "Net allowance for impairment losses", None),
    
    ("ifrs-full_ExposuresToCreditRiskTwelvemonthECLMember", "Stage 1", None),
    ("ifrs-full_ExposuresToCreditRiskLifetimeECLNotCreditimpairedMember", "Stage 2", None),
    ("ifrs-full_ExposuresToCreditRiskLifetimeECLCreditimpairedMember", "Stage 3", None),
    ("ifrs-full_ExposuresToCreditRiskLifetimeECLCreditimpairedMember", "Gross impaired loans", None),
]

import db_config

def seed_aliases(db_path=None):
    if db_path is None:
        db_path = db_config.get_db_path()
    logger.info("Connecting to DuckDB to seed IFRS 2025 aliases...")
    conn = duckdb.connect(db_path)
    
    # Clear existing aliases to avoid duplicates after schema reset
    conn.execute("DELETE FROM Metric_Aliases")
    
    logger.info(f"Seeding {len(ALIASES)} metric aliases...")
    
    # Insert aliases
    conn.executemany(
        "INSERT INTO Metric_Aliases (metric_id, raw_term, institution_id) VALUES (?, ?, ?)",
        ALIASES
    )
    
    conn.close()
    logger.info("Alias seeding (IFRS 2025 Phase) completed.")

if __name__ == "__main__":
    seed_aliases()
