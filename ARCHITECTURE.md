# FS Factbase Architecture Blueprint (v9)

## 1. System Overview
The FS Factbase is a deterministic ETL pipeline designed to extract banking and insurance financial data from unstructured PDFs. It standardizes data via a **Master-Alias Paradigm** into an IFRS-compliant DuckDB factbase, purpose-built for CLI-driven auditing and downstream BI integration.

## 2. Core Operational Pillars
1.  **AI-Assisted Raw Extraction:** Uses LLMs (Gemini) as "Universal Readers" to extract targeted tables into structured JSON schemas.
2.  **100% Deterministic Mapping:** A strict Python/SQL translation layer that maps raw report terms to canonical IFRS metrics. Hallucination is prevented by a "Soft-Halt" protocol.
3.  **Auditable Traceability:** Every stored fact maintains a direct link to the `source_document` and `source_page_number` for 100% auditability.
4.  **CLI-First Human-in-the-Loop (HITL):** Replaces heavy UI dashboards with lightweight terminal tools for cluster-based metric resolution and database auditing.

## 3. Technology Stack
-   **Database:** **DuckDB** - Local-first OLAP for high-performance analytical queries.
-   **Extraction:** **Google Gemini SDK** (Tiered Fallbacks: Flash-Lite -> Flash -> Pro).
-   **Validation:** **Pydantic** - Strict schema enforcement on all LLM data payloads.
-   **Processing:** **Python 3.11+** with Polars for CLI data previews.

## 4. Database Schema (Master-Alias Paradigm)

### `Institutions` (The Metadata Root)
-   `institution_id` (PK, String - e.g., Full Folder Name)
-   `name` (String)
-   `sector` (Enum: 'BANK', 'INSURANCE')
-   `country`, `base_currency`, `regulatory_regime`

### `Core_Metrics` (The Immutable Center)
-   `metric_id` (PK, String - e.g., `ifrs-full_Assets`)
-   `standardized_metric_name` (String)
-   `accounting_standard` (String, e.g., "IFRS 2025")
-   `data_type` (Enum)

### `Metric_Aliases` (The Translation Engine)
-   `alias_id` (PK)
-   `metric_id` (FK to `Core_Metrics`)
-   `raw_term` (String - Exact text from PDF)
-   `institution_id` (FK to `Institutions`)

### `Fact_Financials` (The Target Tidy Data)
-   `fact_id` (PK)
-   `metric_id` (FK to `Core_Metrics`)
-   `institution_id` (FK to `Institutions`)
-   `reporting_period` (String)
-   `value` (Numeric)
-   `is_published` (Boolean: True if extracted, False if computed)
-   `source_document`, `source_page_number`

### `Unmapped_Staging` (The Terminal Review Queue)
-   `staging_id` (PK)
-   `raw_term`, `raw_value`, `institution_id`, `reporting_period`

## 5. Multi-Agent Workflow & Delegation

The system is designed for **Multi-Agent Isolation** to ensure modularity and prevent context bleed.

### 5.1 Numbered Workspaces
-   `p00_Shared_Utils`: Common IO, logging, and date utilities.
-   `p01_Data_Extraction`: PDF slicing, tiered LLM extraction, and JSON ingestion.
-   `p02_Database_and_Mapping`: DuckDB schema, IFRS seeding, and deterministic mapping.
-   `p04_Orchestration`: The "Glue" layer for cross-workspace workflow and CLI tools.

### 5.2 Agent Roles
-   **Root Agent (Lead Architect):** PM role. Manages cross-component orchestration and overarching system standards. Forbidden from writing application code.
-   **Workspace Agents (Engineers):** Specialized agents authorized to write/refactor code within their numbered directories, following `tasks.md` tickets.

---

## 6. Engineering Guarantees
-   **Zero-Hallucination:** System MUST route unrecognized terms to `Unmapped_Staging` instead of guessing.
-   **Bulk Transactions:** Use `conn.executemany()` for all database writes to ensure DuckDB performance.
-   **Tiered Reliability:** Automatic fallback to heavier LLM models on 429 Resource Exhausted errors.
