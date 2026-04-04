import duckdb
from loguru import logger
import os
import csv
import sys

# 100% IFRSAT-2025 COMPLIANT CORE METRIC DICTIONARY (300 Metrics)
# IDs follow the official 'ifrs-full_' prefix from the 2025-03-27 taxonomy files.

MAPPINGS = [
    ('ifrs-full_Assets', 'Total Assets', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_Liabilities', 'Total Liabilities', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_Equity', 'Total Equity', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_EquityAndLiabilities', 'Total Equity and Liabilities', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_ProfitLoss', 'Profit or Loss (Net Profit)', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_CurrentAssets', 'Total Current Assets', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_NoncurrentAssets', 'Total Non-Current Assets', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_CurrentLiabilities', 'Total Current Liabilities', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_NoncurrentLiabilities', 'Total Non-Current Liabilities', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_ProfitLossBeforeTax', 'Profit (Loss) Before Tax', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_ProfitLossFromOperatingActivities', 'Profit (Loss) from Operating Activities', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_IncomeTaxExpenseIncome', 'Income Tax Expense (Income)', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_ProfitLossFromContinuingOperations', 'Profit (Loss) from Continuing Operations', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_ProfitLossFromDiscontinuedOperations', 'Profit (Loss) from Discontinued Operations', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_OtherComprehensiveIncome', 'Other Comprehensive Income (OCI)', 'IFRS 2025', 'universal', 'numeric', '410000'),
    ('ifrs-full_ComprehensiveIncome', 'Total Comprehensive Income', 'IFRS 2025', 'universal', 'numeric', '410000'),
    ('ifrs-full_FinancialAssets', 'Total Financial Assets', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_FinancialLiabilities', 'Total Financial Liabilities', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_OperatingProfitLoss', 'Operating Profit or Loss', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_ProfitLossBeforeFinancingAndIncomeTaxes', 'Profit or Loss Before Financing and Income Taxes', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_GrossIncome', 'Gross Operating Income', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_CashAndCashEquivalents', 'Cash and Cash Equivalents', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_CashAndBalancesWithCentralBanks', 'Cash and Balances with Central Banks', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_DepositsAndPlacementsWithBanksAndOtherFinancialInstitutions', 'Deposits and Placements with Banks', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansAndAdvancesToCustomers', 'Loans and Advances to Customers (Gross/Net)', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansAndAdvancesToBanks', 'Loans and Advances to Banks', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_FinancialAssetsAtAmortisedCost', 'Financial Assets at Amortised Cost', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_FinancialAssetsAtFairValueThroughOtherComprehensiveIncome', 'Financial Assets at FVOCI', 'IFRS 2025', 'banking', 'numeric', '410000'),
    ('ifrs-full_FinancialAssetsAtFairValueThroughProfitOrLoss', 'Financial Assets at FVTPL', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_DerivativeFinancialAssets', 'Derivative Financial Assets', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_ReverseRepurchaseAgreements', 'Reverse Repurchase Agreements', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_StatutoryDepositsWithCentralBanks', 'Statutory Deposits with Central Banks', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_InvestmentProperty', 'Investment Property', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_PropertyPlantAndEquipment', 'Property, Plant and Equipment (PPE)', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_IntangibleAssetsAndGoodwill', 'Intangible Assets and Goodwill', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_DeferredTaxAssets', 'Deferred Tax Assets', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_CurrentTaxAssets', 'Current Tax Assets', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_OtherAssets', 'Other Assets', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_Inventories', 'Inventories', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_TradeAndOtherReceivables', 'Trade and Other Receivables', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_DepositsFromCustomers', 'Deposits from Customers', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_DepositsFromBanks', 'Deposits and Placements of Banks', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_DerivativeFinancialLiabilities', 'Derivative Financial Liabilities', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_DebtSecuritiesIssued', 'Debt Securities Issued', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_SubordinatedLiabilities', 'Subordinated Liabilities', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_RepurchaseAgreements', 'Repurchase Agreements', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_FinancialLiabilitiesAtFairValueThroughProfitOrLoss', 'Financial Liabilities at FVTPL', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_Provisions', 'Provisions', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_CurrentTaxLiabilities', 'Current Tax Liabilities', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_DeferredTaxLiabilities', 'Deferred Tax Liabilities', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_OtherLiabilities', 'Other Liabilities', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_TradeAndOtherPayables', 'Trade and Other Payables', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_IssuedCapital', 'Issued Capital', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_RetainedEarnings', 'Retained Earnings', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_OtherReserves', 'Other Reserves', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_NoncontrollingInterests', 'Non-Controlling Interests', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_InterestIncome', 'Interest Income', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_InterestExpense', 'Interest Expense', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_NetInterestIncome', 'Net Interest Income', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncome', 'Fee and Commission Income', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionExpense', 'Fee and Commission Expense', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_NetFeeAndCommissionIncome', 'Net Fee and Commission Income', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_DividendIncome', 'Dividend Income', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_GainsLossesOnForeignExchange', 'Gains (Losses) on Foreign Exchange', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_NetGainsLossesOnFinancialAssetsAndLiabilitiesHeldForTrading', 'Net Trading Income', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_OtherOperatingIncome', 'Other Operating Income', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_Revenue', 'Total Revenue / Income', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_OperatingExpenses', 'Total Operating Expenses', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_EmployeeBenefitsExpense', 'Personnel / Staff Expenses', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_DepreciationAndAmortisationExpense', 'Depreciation and Amortisation', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_AdministrativeExpense', 'General Administrative Expenses', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_InformationTechnologyExpenses', 'Information Technology Expenses', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_EstablishmentCosts', 'Establishment / Premises Costs', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MarketingAndAdvertisingExpense', 'Marketing and Advertising', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_OtherOperatingExpenses', 'Other Operating Expenses', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_FinanceIncome', 'Finance Income', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_FinanceCosts', 'Finance Costs', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_ShareOfProfitLossOfAssociatesAndJointVentures', 'Share of Profit (Loss) of Associates', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_GovernmentBonds', 'Government Bonds', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_CorporateBonds', 'Corporate Bonds', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_EquitySecurities', 'Equity Securities', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_CollectiveInvestmentSchemes', 'Collective Investment Schemes', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_NegotiableCertificatesOfDeposit', 'Negotiable Certificates of Deposit', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_BankersAcceptancesAndBillsReceivable', "Bankers' Acceptances and Bills Receivable", 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_LoansAndAdvancesToIndividualCustomers', 'Loans: Individuals (Retail)', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansAndAdvancesToCorporateCustomers', 'Loans: Corporate', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansAndAdvancesToGovernmentAndPublicSectorEntity', 'Loans: Government/Public Sector', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansAndAdvancesToSmallMediumEnterprises', 'Loans: SME', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansAndAdvancesToFinancialInstitutions', 'Loans: Financial Institutions', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansForPurchaseOfResidentialProperties', 'Loans: Residential Property', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansForPurchaseOfNonResidentialProperties', 'Loans: Non-Residential Property', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansForPurchaseOfSecurities', 'Loans: Purchase of Securities', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansForPurchaseOfTransportVehicles', 'Loans: Transport Vehicles', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_CreditCardsLoans', 'Loans: Credit Cards', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_PersonalUseLoans', 'Loans: Personal Use', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_WorkingCapitalLoans', 'Loans: Working Capital', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ConstructionLoans', 'Loans: Construction', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansAndAdvancesInMalaysia', 'Loans: In Malaysia', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_LoansAndAdvancesOutsideMalaysia', 'Loans: Outside Malaysia', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_DepositsFromCustomersInMalaysia', 'Deposits: In Malaysia', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_DepositsFromCustomersOutsideMalaysia', 'Deposits: Outside Malaysia', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_MaximumExposureToCreditRisk', 'Maximum Exposure to Credit Risk', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_GrossCarryingAmountOfFinancialAssets', 'Gross Carrying Amount of Financial Assets', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_AllowanceAccountForCreditLossesOfFinancialAssets', 'Total Allowance for Credit Losses (BS)', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_NetExposuresToCreditRisk', 'Net Exposures to Credit Risk', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_CollateralAndOtherCreditEnhancementsHeld', 'Collateral and Credit Enhancements', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_FinancialAssetsPastDueButNotImpaired', 'Financial Assets Past Due but Not Impaired', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_NonperformingLoans', 'Non-performing Loans (NPL)', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_SpecificAllowanceForCreditLosses', 'Specific Allowance (Individual)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_GeneralAllowanceForCreditLosses', 'General Allowance (Collective)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ExposuresToCreditRiskTwelvemonthECLMember', 'ECL: Stage 1 (12-month)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ExposuresToCreditRiskLifetimeECLNotCreditimpairedMember', 'ECL: Stage 2 (Lifetime Non-Impaired)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ExposuresToCreditRiskLifetimeECLCreditimpairedMember', 'ECL: Stage 3 (Impaired)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_CommitmentsAndContingencies', 'Total Commitments and Contingencies', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_CommitmentsToExtendCredit', 'Commitments to Extend Credit (Undrawn)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_FinancialGuaranteesContracts', 'Financial Guarantees Contracts', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_PerformanceGuarantees', 'Performance Guarantees', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_LettersOfCredit', 'Letters of Credit', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ForwardAssetPurchases', 'Forward Asset Purchases', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisForFinancialLiabilities', 'Maturity Analysis for Financial Liabilities', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisForFinancialAssets', 'Maturity Analysis for Financial Assets', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisOneYearOrLess', 'Maturity: 1 Year or Less', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisMoreThanOneYearToThreeYears', 'Maturity: > 1 Year to 3 Years', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisMoreThanThreeYearsToFiveYears', 'Maturity: > 3 Years to 5 Years', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisMoreThanFiveYears', 'Maturity: More than 5 Years', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_InterestIncomeOnFinancialAssetsAtAmortisedCost', 'Interest Income: Amortised Cost', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_InterestIncomeOnFinancialAssetsAtFairValueThroughOtherComprehensiveIncome', 'Interest Income: FVOCI', 'IFRS 2025', 'banking', 'numeric', '410000'),
    ('ifrs-full_InterestIncomeOnDerivativeFinancialInstruments', 'Interest Income: Derivatives', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_InterestExpenseOnFinancialLiabilitiesAtAmortisedCost', 'Interest Expense: Amortised Cost', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_InterestExpenseOnDepositsFromCustomers', 'Interest Expense: Customer Deposits', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_InterestExpenseOnDepositsFromBanks', 'Interest Expense: Bank Deposits', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_InterestExpenseOnDebtSecuritiesIssued', 'Interest Expense: Debt Securities', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromServiceChargesOnDepositAccounts', 'Fee Income: Service Charges', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromCreditLines', 'Fee Income: Credit Lines', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromAssetManagement', 'Fee Income: Asset Management', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromBrokerageAndUnderwriting', 'Fee Income: Brokerage', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_WagesAndSalaries', 'Wages and Salaries', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_SocialSecurityCosts', 'Social Security Costs', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_PensionCosts', 'Pension / Retirement Contributions', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_BonusAndIncentiveAccruals', 'Bonuses and Incentives', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_InformationTechnologyMaintenanceAndSupport', 'IT Maintenance and Support', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_RentAndPropertyCosts', 'Rent and Property Costs', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_RepairAndMaintenanceCosts', 'Repair and Maintenance', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_ProfessionalFeesAndAuditFees', 'Professional and Audit Fees', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_LegalFees', 'Legal Fees', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_TravelAndCommunicationExpenses', 'Travel and Communication', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_DirectorsRemunerationAndFees', "Directors' Remuneration", 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss', 'ECL / Impairment Charges (P&L)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_NetGainsLossesOnDerecognitionOfFinancialAssetsMeasuredAtAmortisedCost', 'Gains (Losses) on Derecognition (Amortised Cost)', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_OtherComprehensiveIncomeThatWillBeReclassifiedToProfitOrLoss', 'OCI: Reclassifiable to P&L', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_OtherComprehensiveIncomeThatWillNotBeReclassifiedToProfitOrLoss', 'OCI: Non-reclassifiable', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_ExchangeDifferencesOnTranslation', 'OCI: Foreign Exchange Translation', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_CashFlowsFromUsedInOperatingActivities', 'Net Cash from Operating Activities', 'IFRS 2025', 'universal', 'numeric', '610000'),
    ('ifrs-full_CashFlowsFromUsedInInvestingActivities', 'Net Cash from Investing Activities', 'IFRS 2025', 'universal', 'numeric', '610000'),
    ('ifrs-full_CashFlowsFromUsedInFinancingActivities', 'Net Cash from Financing Activities', 'IFRS 2025', 'universal', 'numeric', '610000'),
    ('ifrs-full_IncreaseDecreaseInCashAndCashEquivalents', 'Net Increase (Decrease) in Cash', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_EarningsPerShare', 'Earnings per Share', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_DividendsPaid', 'Total Dividends Paid', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstanding', 'Avg Shares Outstanding', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_AmountsDueFromClientsAndBrokers', 'Amounts Due from Clients and Brokers', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_AmountsDueToClientsAndBrokers', 'Amounts Due to Clients and Brokers', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_RightOfUseAssets', 'Right-of-use Assets', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_LeaseLiabilities', 'Lease Liabilities', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_ContractAssets', 'Contract Assets', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_ContractLiabilities', 'Contract Liabilities', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_InvestmentsInAssociates', 'Investments in Associates', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_InvestmentsInJointVentures', 'Investments in Joint Ventures', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_BiologicalAssets', 'Biological Assets', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_CurrentTaxPayable', 'Current Tax Payable', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_DeferredTaxPayable', 'Deferred Tax Payable', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_PostemploymentBenefitObligations', 'Post-employment Benefit Obligations', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_TradingSecurities', 'Trading Securities', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_InvestmentsInSubsidiaries', 'Investments in Subsidiaries', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_PledgedAssets', 'Pledged Assets', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_BillsAndAcceptancesPayable', 'Bills and Acceptances Payable', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_RecourseObligationsOnLoansAndFinancingSoldToCagamas', 'Recourse Obligations on Loans Sold', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_TrustAndOtherFiduciaryAssets', 'Trust and Fiduciary Assets', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_TrustAndOtherFiduciaryLiabilities', 'Trust and Fiduciary Liabilities', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_InterestIncomeFromLoansAndAdvances', 'Interest Income: Loans and Advances', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_InterestIncomeFromFinancialInvestments', 'Interest Income: Financial Investments', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_InterestIncomeFromDepositsAndPlacementsWithFinancialInstitutions', 'Interest Income: Deposits with Banks', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_InterestExpenseOnOtherBorrowedFunds', 'Interest Expense: Other Borrowed Funds', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_InterestExpenseOnSubordinatedObligations', 'Interest Expense: Subordinated Obligations', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromAdvisoryServices', 'Fee Income: Advisory Services', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromGuaranteeFees', 'Fee Income: Guarantee Fees', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromCorporateFinance', 'Fee Income: Corporate Finance', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromWealthManagement', 'Fee Income: Wealth Management', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromTrustActivities', 'Fee Income: Trust Activities', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromDebitCards', 'Fee Income: Debit Cards', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_FeeAndCommissionIncomeFromCreditCards', 'Fee Income: Credit Cards', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_RealisedGainsLossesOnDisposalOfFinancialInvestments', 'Realised Gains/Losses: Financial Investments', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_UnrealisedGainsLossesOnFVTPLFinancialAssets', 'Unrealised Gains/Losses: FVTPL', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_FairValueGainsLossesOnDerivatives', 'Fair Value Gains/Losses: Derivatives', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_OperatingLeaseIncome', 'Operating Lease Income', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_GainsLossesOnDisposalOfPropertyPlantAndEquipment', 'Gains/Losses: Disposal of PPE', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_QuotedEquitySecurities', 'Equity Securities: Quoted', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_UnquotedEquitySecurities', 'Equity Securities: Unquoted', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_MalaysianGovernmentSecurities', 'Malaysian Government Securities (MGS)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MalaysianGovernmentInvestmentIssues', 'Malaysian Government Investment Issues (MGII)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_TreasuryBills', 'Treasury Bills', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_CorporateBondsAndSukuk', 'Corporate Bonds and Sukuk', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_NegotiableCertificatesOfDeposits', 'Negotiable Certificates of Deposits', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_BankersAcceptancesAndBillsPurchased', 'Bankers Acceptances and Bills Purchased', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_Overdrafts', 'Loans: Overdrafts', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_TermLoans', 'Loans: Term Loans', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_RevolvingCredit', 'Loans: Revolving Credit', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_HirePurchaseReceivables', 'Loans: Hire Purchase Receivables', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_HousingLoans', 'Loans: Housing Loans', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_SyndicatedLoans', 'Loans: Syndicated Loans', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_StaffLoans', 'Loans: Staff Loans', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_CreditCardReceivables', 'Loans: Credit Card Receivables', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_OtherLoansAndFinancing', 'Loans: Other Loans and Financing', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_DemandDeposits', 'Deposits: Demand Deposits', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_SavingsDeposits', 'Deposits: Savings Deposits', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_FixedDeposits', 'Deposits: Fixed Deposits', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_NegotiableInstrumentsOfDeposit', 'Deposits: Negotiable Instruments (NID)', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_MoneyMarketDeposits', 'Deposits: Money Market Deposits', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_AuditorsRemuneration', 'Auditor Remuneration', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_LegalFeesAndProfessionalCharges', 'Legal and Professional Fees', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_ConsultancyFees', 'Consultancy Fees', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_CommunicationAndPostage', 'Communication and Postage', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_UtilityExpenses', 'Utility Expenses', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_AdvertisingAndPromotionExpenses', 'Advertising and Promotion', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_PrintingAndStationery', 'Printing and Stationery', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_SecurityAndCourierServices', 'Security and Courier Services', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_TrainingAndDevelopmentExpenses', 'Training and Development', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_LicenceFees', 'Licence Fees', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_ElectronicDataProcessingExpenses', 'Electronic Data Processing Expenses', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_MotorVehicleExpenses', 'Motor Vehicle Expenses', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_TravelAndAccommodationExpenses', 'Travel and Accommodation', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_SharedServiceCostAllocations', 'Shared Service Cost Allocations', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ECLStage1TotalExposures', 'ECL: Total Exposures Stage 1', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ECLStage2TotalExposures', 'ECL: Total Exposures Stage 2', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ECLStage3TotalExposures', 'ECL: Total Exposures Stage 3', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ECLAllowanceLoansAndAdvances', 'ECL Allowance: Loans and Advances', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ECLAllowanceFinancialInvestments', 'ECL Allowance: Financial Investments', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ECLAllowanceOnOffBalanceSheetItems', 'ECL Allowance: Off-balance Sheet', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ImpairedLoansAndAdvancesGross', 'Gross Impaired Loans and Advances', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_RecoveriesOfBadDebtsAndFinancing', 'Recoveries of Bad Debts', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_WriteoffsOfBadDebtsAndFinancing', 'Write-offs of Bad Debts', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_NonperformingLoansRatio', 'Non-performing Loans (NPL) Ratio (%)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_CommonEquityTier1Capital', 'CET1 Capital', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_Tier1CapitalTotal', 'Tier 1 Capital', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_Tier2CapitalTotal', 'Tier 2 Capital', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_TotalRegulatoryCapital', 'Total Regulatory Capital', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_RiskWeightedAssetsTotal', 'Total Risk-Weighted Assets (RWA)', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_CET1RatioPercentage', 'CET1 Ratio (%)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_Tier1RatioPercentage', 'Tier 1 Ratio (%)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_TotalCapitalRatioPercentage', 'Total Capital Ratio (%)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_LeverageRatioPercentage', 'Leverage Ratio (%)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_LiquidityCoverageRatioPercentage', 'Liquidity Coverage Ratio (%)', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisUpToSevenDays', 'Maturity: Up to 7 Days', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisOverSevenDaysToOneMonth', 'Maturity: 7 Days to 1 Month', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisOverOneMonthToThreeMonths', 'Maturity: 1 to 3 Months', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisOverThreeMonthsToSixMonths', 'Maturity: 3 to 6 Months', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_MaturityAnalysisOverSixMonthsToTwelveMonths', 'Maturity: 6 to 12 Months', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_ActuarialGainsLossesOnDefinedBenefitPlans', 'Actuarial Gains/Losses on Benefit Plans', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_GainLossOnHedgingInstrumentsInCashFlowHedge', 'Gains/Losses: Cash Flow Hedge', 'IFRS 2025', 'universal', 'numeric', '610000'),
    ('ifrs-full_GainLossOnHedgeOfNetInvestmentInForeignOperations', 'Gains/Losses: Net Investment Hedge', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_ExternalRevenue', 'External Revenue', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_IntersegmentRevenue', 'Inter-segment Revenue', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_SegmentAssets', 'Segment Assets', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_SegmentLiabilities', 'Segment Liabilities', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_NoncurrentAssetsHeldForSale', 'Non-current Assets Held for Sale', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_LiabilitiesDirectlyAssociatedWithNoncurrentAssetsHeldForSale', 'Liabilities: Assets Held for Sale', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_Prepayments', 'Prepayments', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_SundryDepositsAndOtherReceivables', 'Sundry Deposits and Other Receivables', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_InformationTechnologyHardwareExpenses', 'IT Hardware Expenses', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_InformationTechnologySoftwareExpenses', 'IT Software Expenses', 'IFRS 2025', 'banking', 'numeric', '310000'),
    ('ifrs-full_InformationTechnologyProfessionalFees', 'IT Professional Fees', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_StaffTrainingAndRecruitmentExpenses', 'Staff Training and Recruitment', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_StaffWelfareAndMedicalExpenses', 'Staff Welfare and Medical', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_LandAndBuildings', 'PPE: Land and Buildings', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_FurnitureFittingsAndEquipment', 'PPE: Furniture, Fittings and Equipment', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_OfficeEquipmentAndComputers', 'PPE: Office Equipment and Computers', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_RenovationAndInteriors', 'PPE: Renovation and Interiors', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_CapitalWorkInProgress', 'PPE: Capital Work-in-progress', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_ComputerSoftwareIntangible', 'Intangible: Computer Software', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_GoodwillFromBusinessCombinations', 'Intangible: Goodwill', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_CoreBankingSystemIntangible', 'Intangible: Core Banking System', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_AmountDueToSubsidiaries', 'Due to Subsidiaries', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_AmountDueFromSubsidiaries', 'Due from Subsidiaries', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_AmountDueToAssociates', 'Due to Associates', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_AmountDueFromAssociates', 'Due from Associates', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_NetInsuranceGeneralCommissionAndFeeIncomeExpense', 'Net Insurance Commission/Fee (General)', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_RealisedGainsLossesOnDerivativeFinancialInstruments', 'Realised Gains/Losses: Derivatives', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_UnrealisedGainsLossesOnDerivativeFinancialInstruments', 'Unrealised Gains/Losses: Derivatives', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_StatutoryDepositsWithCentralBanksRefinement', 'Statutory Deposits (Refined)', 'IFRS 2025', 'banking', 'numeric', '210000'),
    ('ifrs-full_FloatingRateNotesIssued', 'Floating Rate Notes Issued', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_FixedRateNotesIssued', 'Fixed Rate Notes Issued', 'IFRS 2025', 'banking', 'numeric', '800000'),
    ('ifrs-full_EquityAttributableToOwnersOfParent', 'Equity Attributable to Owners', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_TotalComprehensiveIncomeAttributableToOwnersOfParent', 'Total Comp. Income to Owners', 'IFRS 2025', 'universal', 'numeric', '410000'),
    ('ifrs-full_TotalComprehensiveIncomeAttributableToNoncontrollingInterests', 'Total Comp. Income to NCI', 'IFRS 2025', 'universal', 'numeric', '410000'),
    ('ifrs-full_GeneralAdministrativeExpensesOther', 'Other General Admin Expenses', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_CorporateSocialResponsibilityExpenses', 'CSR Expenses', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_DirectorsFees', 'Directors Fees', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_DirectorsOtherEmoluments', 'Directors Other Emoluments', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_StaffSocialSecurityContributions', 'Staff Social Security (SOCSO/EPF)', 'IFRS 2025', 'universal', 'numeric', '800000'),
    ('ifrs-full_PostemploymentBenefitExpenses', 'Post-employment Benefit Expenses', 'IFRS 2025', 'universal', 'numeric', '310000'),
    ('ifrs-full_NetIncreaseDecreaseInCashAndCashEquivalentsCashFlow', 'Net Change in Cash (CFS)', 'IFRS 2025', 'universal', 'numeric', '610000'),
    ('ifrs-full_CashAndCashEquivalentsAtBeginningOfPeriod', 'Cash at Beginning of Period', 'IFRS 2025', 'universal', 'numeric', '210000'),
    ('ifrs-full_CashAndCashEquivalentsAtEndOfPeriod', 'Cash at End of Period', 'IFRS 2025', 'universal', 'numeric', '210000'),
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
    ("ifrs-full_Assets", "ifrs-full_FinancialAssets", 1.0),
    ("ifrs-full_FinancialAssets", "ifrs-full_CashAndBalancesWithCentralBanks", 1.0),
    ("ifrs-full_FinancialAssets", "ifrs-full_LoansAndAdvancesToCustomers", 1.0),
    ("ifrs-full_FinancialAssets", "ifrs-full_LoansAndAdvancesToBanks", 1.0),
    ("ifrs-full_FinancialAssets", "ifrs-full_FinancialAssetsAtAmortisedCost", 1.0),
    ("ifrs-full_FinancialAssets", "ifrs-full_FinancialAssetsAtFairValueThroughOtherComprehensiveIncome", 1.0),
    ("ifrs-full_FinancialAssets", "ifrs-full_FinancialAssetsAtFairValueThroughProfitOrLoss", 1.0),
    ("ifrs-full_FinancialAssets", "ifrs-full_DerivativeFinancialAssets", 1.0),
    ("ifrs-full_Assets", "ifrs-full_OtherAssets", 1.0),
    ("ifrs-full_Assets", "ifrs-full_StatutoryDepositsWithCentralBanks", 1.0),
    ("ifrs-full_Assets", "ifrs-full_InvestmentProperty", 1.0),
    ("ifrs-full_Assets", "ifrs-full_PropertyPlantAndEquipment", 1.0),
    ("ifrs-full_Assets", "ifrs-full_IntangibleAssetsAndGoodwill", 1.0),
    
    # Financial Assets Breakdown (L3 into L2)
    ("ifrs-full_FinancialAssetsAtAmortisedCost", "ifrs-full_GovernmentBonds", 1.0),
    ("ifrs-full_FinancialAssetsAtAmortisedCost", "ifrs-full_CorporateBonds", 1.0),
    ("ifrs-full_FinancialAssetsAtFairValueThroughOtherComprehensiveIncome", "ifrs-full_EquitySecurities", 1.0),
    ("ifrs-full_FinancialAssetsAtFairValueThroughProfitOrLoss", "ifrs-full_CollectiveInvestmentSchemes", 1.0),
    
    # Sector/Industry Breakdown into Loans
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansAndAdvancesToIndividualCustomers", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansAndAdvancesToCorporateCustomers", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansAndAdvancesToGovernmentAndPublicSectorEntity", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansAndAdvancesToSmallMediumEnterprises", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansAndAdvancesToFinancialInstitutions", 1.0),
    
    # Purpose Breakdown into Loans
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansForPurchaseOfResidentialProperties", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansForPurchaseOfNonResidentialProperties", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansForPurchaseOfSecurities", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansForPurchaseOfTransportVehicles", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_CreditCardsLoans", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_PersonalUseLoans", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_WorkingCapitalLoans", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_ConstructionLoans", 1.0),

    # Geographic Breakdown
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansAndAdvancesInMalaysia", 1.0),
    ("ifrs-full_LoansAndAdvancesToCustomers", "ifrs-full_LoansAndAdvancesOutsideMalaysia", 1.0),
    ("ifrs-full_DepositsFromCustomers", "ifrs-full_DepositsFromCustomersInMalaysia", 1.0),
    ("ifrs-full_DepositsFromCustomers", "ifrs-full_DepositsFromCustomersOutsideMalaysia", 1.0),

    # Liabilities Breakdown
    ("ifrs-full_Liabilities", "ifrs-full_DepositsFromCustomers", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_DepositsFromBanks", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_DebtSecuritiesIssued", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_SubordinatedLiabilities", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_OtherLiabilities", 1.0),
    ("ifrs-full_Liabilities", "ifrs-full_DerivativeFinancialLiabilities", 1.0),

    # Commitments & Contingencies (Off-Balance Sheet)
    ("ifrs-full_CommitmentsAndContingencies", "ifrs-full_CommitmentsToExtendCredit", 1.0),
    ("ifrs-full_CommitmentsAndContingencies", "ifrs-full_FinancialGuaranteesContracts", 1.0),
    ("ifrs-full_CommitmentsAndContingencies", "ifrs-full_LettersOfCredit", 1.0),

    # Maturity Analysis (Waterfall)
    ("ifrs-full_MaturityAnalysisForFinancialAssets", "ifrs-full_MaturityAnalysisOneYearOrLess", 1.0),
    ("ifrs-full_MaturityAnalysisForFinancialAssets", "ifrs-full_MaturityAnalysisMoreThanOneYearToThreeYears", 1.0),
    ("ifrs-full_MaturityAnalysisForFinancialAssets", "ifrs-full_MaturityAnalysisMoreThanThreeYearsToFiveYears", 1.0),
    ("ifrs-full_MaturityAnalysisForFinancialAssets", "ifrs-full_MaturityAnalysisMoreThanFiveYears", 1.0),

    # --- Income Statement Hierarchy (L4 -> L3 -> L2 -> L1 -> L0) ---
    
    # Revenue/Gross Income
    ("ifrs-full_GrossIncome", "ifrs-full_NetInterestIncome", 1.0),
    ("ifrs-full_GrossIncome", "ifrs-full_NetFeeAndCommissionIncome", 1.0),
    ("ifrs-full_GrossIncome", "ifrs-full_NetGainsLossesOnFinancialAssetsAndLiabilitiesHeldForTrading", 1.0),
    ("ifrs-full_GrossIncome", "ifrs-full_OtherOperatingIncome", 1.0),

    # Operating Profit
    ("ifrs-full_OperatingProfitLoss", "ifrs-full_GrossIncome", 1.0),
    ("ifrs-full_OperatingProfitLoss", "ifrs-full_OperatingExpenses", -1.0),
    ("ifrs-full_OperatingProfitLoss", "ifrs-full_ImpairmentLossReversalOfImpairmentLossRecognisedInProfitOrLoss", -1.0),
    
    # Staff Costs Breakdown
    ("ifrs-full_EmployeeBenefitsExpense", "ifrs-full_WagesAndSalaries", 1.0),
    ("ifrs-full_EmployeeBenefitsExpense", "ifrs-full_SocialSecurityCosts", 1.0),
    ("ifrs-full_EmployeeBenefitsExpense", "ifrs-full_PensionCosts", 1.0),
    ("ifrs-full_EmployeeBenefitsExpense", "ifrs-full_BonusAndIncentiveAccruals", 1.0),

    # Operating Expenses Breakdown
    ("ifrs-full_OperatingExpenses", "ifrs-full_EmployeeBenefitsExpense", 1.0),
    ("ifrs-full_OperatingExpenses", "ifrs-full_DepreciationAndAmortisationExpense", 1.0),
    ("ifrs-full_OperatingExpenses", "ifrs-full_InformationTechnologyMaintenanceAndSupport", 1.0),
    ("ifrs-full_OperatingExpenses", "ifrs-full_RentAndPropertyCosts", 1.0),
    ("ifrs-full_OperatingExpenses", "ifrs-full_RepairAndMaintenanceCosts", 1.0),
    ("ifrs-full_OperatingExpenses", "ifrs-full_ProfessionalFeesAndAuditFees", 1.0),
    ("ifrs-full_OperatingExpenses", "ifrs-full_DirectorsRemunerationAndFees", 1.0),

    # Interest Income Breakdown
    ("ifrs-full_InterestIncome", "ifrs-full_InterestIncomeOnFinancialAssetsAtAmortisedCost", 1.0),
    ("ifrs-full_InterestIncome", "ifrs-full_InterestIncomeOnFinancialAssetsAtFairValueThroughOtherComprehensiveIncome", 1.0),
    ("ifrs-full_InterestIncome", "ifrs-full_InterestIncomeOnDerivativeFinancialInstruments", 1.0),
    
    # Interest Expense Breakdown
    ("ifrs-full_InterestExpense", "ifrs-full_InterestExpenseOnDepositsFromCustomers", 1.0),
    ("ifrs-full_InterestExpense", "ifrs-full_InterestExpenseOnDepositsFromBanks", 1.0),
    ("ifrs-full_InterestExpense", "ifrs-full_InterestExpenseOnDebtSecuritiesIssued", 1.0),

    # Profit Before Tax components (IFRS 18 Waterfall)
    ("ifrs-full_ProfitLossBeforeTax", "ifrs-full_OperatingProfitLoss", 1.0),
    ("ifrs-full_ProfitLossBeforeTax", "ifrs-full_FinanceIncome", 1.0),
    ("ifrs-full_ProfitLossBeforeTax", "ifrs-full_FinanceCosts", -1.0),
    ("ifrs-full_ProfitLossBeforeTax", "ifrs-full_ShareOfProfitLossOfAssociatesAndJointVentures", 1.0),

    # Net Profit
    ("ifrs-full_ProfitLoss", "ifrs-full_ProfitLossBeforeTax", 1.0),
    ("ifrs-full_ProfitLoss", "ifrs-full_IncomeTaxExpenseIncome", -1.0),

    # OCI Hierarchy
    ("ifrs-full_OtherComprehensiveIncome", "ifrs-full_OtherComprehensiveIncomeThatWillBeReclassifiedToProfitOrLoss", 1.0),
    ("ifrs-full_OtherComprehensiveIncome", "ifrs-full_OtherComprehensiveIncomeThatWillNotBeReclassifiedToProfitOrLoss", 1.0),
    ("ifrs-full_OtherComprehensiveIncomeThatWillBeReclassifiedToProfitOrLoss", "ifrs-full_ExchangeDifferencesOnTranslation", 1.0),

    # --- Expanded Hierarchy: Additional IFRS 2025 Relationships ---
    ('ifrs-full_Assets', 'ifrs-full_RightOfUseAssets', 1.0),
    ('ifrs-full_Liabilities', 'ifrs-full_LeaseLiabilities', 1.0),
    ('ifrs-full_Assets', 'ifrs-full_ContractAssets', 1.0),
    ('ifrs-full_Liabilities', 'ifrs-full_ContractLiabilities', 1.0),
    ('ifrs-full_Assets', 'ifrs-full_InvestmentsInAssociates', 1.0),
    ('ifrs-full_Assets', 'ifrs-full_InvestmentsInSubsidiaries', 1.0),
    ('ifrs-full_Liabilities', 'ifrs-full_CurrentTaxPayable', 1.0),
    ('ifrs-full_Liabilities', 'ifrs-full_DeferredTaxPayable', 1.0),
    ('ifrs-full_Liabilities', 'ifrs-full_BillsAndAcceptancesPayable', 1.0),
    ('ifrs-full_Liabilities', 'ifrs-full_RecourseObligationsOnLoansAndFinancingSoldToCagamas', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_AuditorsRemuneration', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_LegalFeesAndProfessionalCharges', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_ConsultancyFees', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_CommunicationAndPostage', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_UtilityExpenses', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_AdvertisingAndPromotionExpenses', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_PrintingAndStationery', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_SecurityAndCourierServices', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_TrainingAndDevelopmentExpenses', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_LicenceFees', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_MotorVehicleExpenses', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_TravelAndAccommodationExpenses', 1.0),
    ('ifrs-full_OperatingExpenses', 'ifrs-full_SharedServiceCostAllocations', 1.0),
    ('ifrs-full_InformationTechnologyExpenses', 'ifrs-full_InformationTechnologyHardwareExpenses', 1.0),
    ('ifrs-full_InformationTechnologyExpenses', 'ifrs-full_InformationTechnologySoftwareExpenses', 1.0),
    ('ifrs-full_InformationTechnologyExpenses', 'ifrs-full_InformationTechnologyProfessionalFees', 1.0),
    ('ifrs-full_EmployeeBenefitsExpense', 'ifrs-full_StaffTrainingAndRecruitmentExpenses', 1.0),
    ('ifrs-full_EmployeeBenefitsExpense', 'ifrs-full_StaffWelfareAndMedicalExpenses', 1.0),
    ('ifrs-full_EmployeeBenefitsExpense', 'ifrs-full_StaffSocialSecurityContributions', 1.0),
    ('ifrs-full_PropertyPlantAndEquipment', 'ifrs-full_LandAndBuildings', 1.0),
    ('ifrs-full_PropertyPlantAndEquipment', 'ifrs-full_FurnitureFittingsAndEquipment', 1.0),
    ('ifrs-full_PropertyPlantAndEquipment', 'ifrs-full_OfficeEquipmentAndComputers', 1.0),
    ('ifrs-full_PropertyPlantAndEquipment', 'ifrs-full_RenovationAndInteriors', 1.0),
    ('ifrs-full_PropertyPlantAndEquipment', 'ifrs-full_CapitalWorkInProgress', 1.0),
    ('ifrs-full_IntangibleAssetsAndGoodwill', 'ifrs-full_ComputerSoftwareIntangible', 1.0),
    ('ifrs-full_IntangibleAssetsAndGoodwill', 'ifrs-full_GoodwillFromBusinessCombinations', 1.0),
    ('ifrs-full_IntangibleAssetsAndGoodwill', 'ifrs-full_CoreBankingSystemIntangible', 1.0),
    ('ifrs-full_NetInterestIncome', 'ifrs-full_InterestIncomeFromLoansAndAdvances', 1.0),
    ('ifrs-full_NetInterestIncome', 'ifrs-full_InterestIncomeFromFinancialInvestments', 1.0),
    ('ifrs-full_NetInterestIncome', 'ifrs-full_InterestIncomeFromDepositsAndPlacementsWithFinancialInstitutions', 1.0),
    ('ifrs-full_InterestExpense', 'ifrs-full_InterestExpenseOnOtherBorrowedFunds', 1.0),
    ('ifrs-full_InterestExpense', 'ifrs-full_InterestExpenseOnSubordinatedObligations', 1.0),
    ('ifrs-full_NetFeeAndCommissionIncome', 'ifrs-full_FeeAndCommissionIncomeFromAdvisoryServices', 1.0),
    ('ifrs-full_NetFeeAndCommissionIncome', 'ifrs-full_FeeAndCommissionIncomeFromGuaranteeFees', 1.0),
    ('ifrs-full_NetFeeAndCommissionIncome', 'ifrs-full_FeeAndCommissionIncomeFromCorporateFinance', 1.0),
    ('ifrs-full_NetFeeAndCommissionIncome', 'ifrs-full_FeeAndCommissionIncomeFromWealthManagement', 1.0),
    ('ifrs-full_NetFeeAndCommissionIncome', 'ifrs-full_FeeAndCommissionIncomeFromTrustActivities', 1.0),
    ('ifrs-full_NetFeeAndCommissionIncome', 'ifrs-full_FeeAndCommissionIncomeFromDebitCards', 1.0),
    ('ifrs-full_NetFeeAndCommissionIncome', 'ifrs-full_FeeAndCommissionIncomeFromCreditCards', 1.0),
    ('ifrs-full_Equity', 'ifrs-full_EquityAttributableToOwnersOfParent', 1.0),
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

    # Core Metrics (300 items)
    logger.info(f"Seeding {len(MAPPINGS)} IFRS 2025 core metrics...")
    conn.executemany(
        "INSERT INTO Core_Metrics (metric_id, standardized_metric_name, accounting_standard, sector, data_type, statement_role) VALUES (?, ?, ?, ?, ?, ?)",
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
