# FS Factbase Architecture Blueprint

## 1. System Overview
The FS Factbase is a scalable ETL pipeline designed to extract banking and insurance financial data from unstructured PDFs, standardizing it via a Master-Alias Paradigm to support high-precision benchmarking and Text-to-SQL analytics.

## 2. Core Operational Pillars
1. **AI-Assisted Raw Extraction**: Uses LLMs (Gemini) to extract targeted tables from PDFs into structured JSON.
2. **100% Deterministic Mapping**: Maps raw report terms to canonical metrics.
3. **The Variance Engine (Dual-Fact Model)**: Stores both report-published values and system-computed ones to reconcile and audit data integrity.
4. **Selective Growth**: Employs "Micro-Staging" (limited batch sizes) to keep the human-in-the-loop (HITL) queue manageable and the database growth intentional.

## 3. Recommended Tech Stack
- **Database**: **DuckDB** - Local-first OLAP for lightning-fast analytical queries and Text-to-SQL compatibility.
- **Language**: **Python 3.11+**
- **Validation**: **Pydantic** - Strict schema enforcement on all LLM outputs.
- **Orchestration**: **Local Order of Operations** (Extract -> Map -> Compute Variance -> Load).

## 4. Database Schema (Master-Alias Paradigm)

### `Institutions` (The Metadata Root)
- `institution_id` (PK, String)
- `name` (String)
- `sector` (Enum: 'BANK', 'INSURANCE')
- `country` (String)
- `base_currency` (String)
- `regulatory_regime` (String)

### `Core_Metrics` (The Immutable Center)
- `metric_id` (PK, String)
- `standardized_metric_name` (String)
- `accounting_standard` (String, e.g., "IFRS 9", "IFRS 17")
- `sector` (String: 'banking', 'insurance', 'universal')
- `data_type` (Enum)

### `Metric_Aliases` (The Translation Engine)
- `alias_id` (PK)
- `metric_id` (FK to `Core_Metrics`)
- `raw_term` (String)
- `institution_id` (FK to `Institutions`)

### `Fact_Financials` (The Target Tidy Data)
- `fact_id` (PK)
- `metric_id` (FK to `Core_Metrics`)
- `institution_id` (FK to `Institutions`)
- `reporting_period` (String, e.g., "2024")
- `value` (Numeric)
- `currency_code` (String, as reported)
- `is_published` (Boolean: True if from report, False if computed)
- `formula_id` (Nullable String, for computed facts)
- `source_document` (String)
- `source_page_number` (Integer)

### `Unmapped_Staging` (The Human Review Queue)
- `staging_id` (PK)
- `raw_term` (String)
- `raw_value` (Numeric)
- `institution_id` (String)
- `reporting_period` (String)

## 5. Operational Features

### 5.1 The Variance Engine
To ensure data integrity, the system implements a post-extraction computation layer. For every derived metric (e.g., ROE), the system:
1.  Extracts the published ROE from the PDF (stored with `is_published=True`).
2.  Independently calculates ROE from component facts (e.g., Net Profit / Total Equity) and stores it with `is_published=False`.
3.  Flags any variance above 0.5% for manual audit.

### 5.2 Multi-Currency Scaling
All values are stored in their native currency as found in the report. Multi-company benchmarking queries apply exchange rates at **runtime** (query-side) to prevent data staleness and allow for various FX sources.

### 5.3 Asynchronous Bulk Transacting
All transformation scripts must accumulate results into memory and perform a single `conn.executemany()` bulk transaction to ensure performance and avoid I/O blocking in DuckDB.
