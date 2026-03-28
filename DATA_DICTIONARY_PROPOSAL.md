# Seed Data Dictionary Proposal: Banking Core Metrics (IFRS 9)

To seed our `Core_Metrics` table, this initial proposal provides the critical 15 metrics fundamental to analyzing a banking institution's balance sheet, income statement, and capital adequacy under IFRS 9 / Basel III.

## A. Balance Sheet (Statement of Financial Position)

1. **Total Assets** (`total_assets`)
   - The definitive size of the institution. Everything owned or controlled.
2. **Gross Loans and Advances to Customers** (`gross_loans_advances_customers`)
   - The bank's primary core lending portfolio *before* applying expected credit loss (ECL) provisions.
3. **Total Customer Deposits** (`total_customer_deposits`)
   - The aggregate sum of retail and corporate deposits; the bank's primary funding source.
4. **Total Equity** (`total_equity`)
   - Shareholders' equity, representing the net worth of the bank.
5. **Allowance for Impairment Losses / ECL** (`allowance_expected_credit_losses`)
   - The total provisioned buffer set aside per IFRS 9 models for potentially bad loans.

## B. Income Statement (Statement of Profit or Loss)

6. **Net Interest Income** (`net_interest_income`)
   - Interest earned from lending minus interest paid out to depositors and debt holders.
7. **Non-Interest Income** (`non_interest_income`)
   - Fees, commissions, trading income, and other ancillary revenues.
8. **Operating Expenses** (`total_operating_expenses`)
   - The aggregated cost of running the bank (staff, IT, premises, marketing).
9. **Impairment Charges / Specific Provisions** (`impairment_charges_for_credit_losses`)
   - The P&L hit taken in the current reporting period reflecting newly expected credit losses.
10. **Profit Before Tax** (`profit_before_tax`)
    - The net operating profitability before regulatory taxation.
11. **Net Profit Attributable to Shareholders** (`net_profit_attributable_to_shareholders`)
    - The bottom-line ultimate profitability reported for the period.

## C. Key Ratios & Capital Adequacy (Basel III)

12. **Common Equity Tier 1 (CET1) Ratio** (`cet1_ratio`)
    - The highest quality of regulatory capital as a percentage of risk-weighted assets.
13. **Cost-to-Income Ratio** (`cost_to_income_ratio`)
    - Operating expenses divided by total operating income (efficiency metric).
14. **Gross Impaired Loan (GIL) Ratio** (`gross_impaired_loan_ratio`)
    - Non-performing loans as a percentage of the total gross loan portfolio. 
15. **Net Interest Margin (NIM)** (`net_interest_margin`)
    - The yield a bank generates on its credit products relative to the cost of its funding.

*Note: Whenever a new term is discovered, the pipeline must compare its semantic meaning against these 15 core definitions. If it maps mathematically or functionally to one of them, the alias is created. If it represents a genuinely new concept (e.g., "Green bonds portfolio"), a human reviewer will establish metric #16.*
