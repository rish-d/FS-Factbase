# FS Factbase Architecture Blueprint

## 1. System Overview
The FS Factbase is a scalable ETL (Extract, Transform, Load) pipeline designed to extract banking financial tables from unstructured PDFs, parse them into structured arrays, and map esoteric reporting terminology securely against a globally standardized dictionary (Master-Alias Paradigm) to support a Text-to-SQL layer safely.

## 2. Core Operational Pillars
1. **AI-Assisted Raw Extraction**: Uses an LLM to reliably pull tables from complex PDFs into JSON, replacing brittle regex heuristics.
2. **100% Deterministic Mapping**: The mapping logic relies on predefined aliases. 
3. **The Soft-Halt Mechanism**: Unrecognized terminology is temporarily sent to an `Unmapped_Staging` database table without breaking pipeline progress, allowing human-in-the-loop review at the end of the extraction process.

## 3. Recommended Tech Stack
- **Database**: **DuckDB** - Provides lighting-fast analytical query processing on flat files, making it completely local, scalable, and fully compatible with Text-to-SQL LLM paradigms.
- **Language**: **Python 3.11+**
- **Data Model Validation**: **Pydantic** - Enforces strict static typing on parsed variables before they enter the local mappings layer or DuckDB.
- **Data Manipulation**: **Polars / Pandas** - Clean and transform mapped JSON data into flat, relational database rows intuitively.
- **Orchestration**: **Dagster / Prefect** (Local) - Ensures distinct extraction, transformation, and load steps are modular, trackable, and individually idempotent. 
- **LLM/API Extraction**: Google **Gemini 1.5 Pro** or equivalent Document AI for layout-heavy tabular data extraction to robust JSON.

## 4. Database Schema (Master-Alias Paradigm)
### `Core_Metrics` (The Immutable Center)
- `metric_id` (PK, String or UUID)
- `standardized_metric_name` (String, e.g., "Total Assets")
- `accounting_standard` (String, e.g., "IFRS 9", "Basel III")
- `data_type` (Enum)

### `Metric_Aliases` (The Translation Engine)
- `alias_id` (PK)
- `metric_id` (FK to `Core_Metrics`)
- `raw_term` (String, e.g., "Advances to customers")
- `institution_id` (Nullable String, to distinguish varying terms across specific banks)

### `Fact_Financials` (The Target Tidy Data)
- `fact_id` (PK)
- `metric_id` (FK to `Core_Metrics`)
- `institution_id` (String)
- `reporting_period` (String or Date, e.g., "2023-Q4")
- `value` (Float/Numeric)
- `source_document` (String)
- `source_page_number` (Integer)

### `Unmapped_Staging` (The Human Review Queue)
- `staging_id` (PK)
- `raw_term` (String - unrecognized metric names)
- `raw_value` (Float/Numeric)
- `institution_id` (String)
- `reporting_period` (String or Date)
- `source_document` (String)
- `source_page_number` (Integer)

## 5. Performance & Scaling Architecture

To process hundreds of 300+ page financial reports efficiently without hitting LLM rate limits or freezing the UI, the architecture adheres to these operational rules:

### 5.1 Targeted Semantic Extraction (Solving the $O(N^2)$ Staging Bottleneck)
Rather than extracting every row from a financial statement, the orchestrator dynamically prompts the LLM to extract only a highly specific, pre-defined list of metrics (e.g., "Extract ONLY Total Assets, Deposits, and Gross Loans"). 
* **The Benefit:** This limits the flow of unrecognized terms into the `Unmapped_Staging` queue. Semantic string-matching algorithms (used for alias clustering) run in exponential $O(N^2)$ time. By utilizing "Micro-Staging" (keeping $N$ under 50 per batch), the mapping layer processes instantly, allowing the database to scale selectively over time.

### 5.2 Hybrid AI Diagnostics
The system learns from human corrections via a hybrid approach:
1. **Python monitors (Cheap/Deterministic):** Standard SQL queries monitor the database for recurring human corrections and categorize them (e.g., `SCALE_ERROR`).
2. **LLM resolves (High-Value Compute):** When an error threshold is crossed, the LLM is invoked to rewrite the exact `extraction_prompt` for that specific institution, creating a permanent, self-healing feedback loop.

### 5.3 Asynchronous Bulk Transacting
DuckDB is optimized for column-store analytics, not transactional row-by-row inserts. All data transformations in the `transformers` directory must accumulate records into memory arrays or Polars DataFrames and execute a single `conn.executemany()` bulk insert at the end of the script to prevent I/O locking and pipeline bottlenecks.
