# Master Project Plan: FS Factbase (Revised v5)

This document is the absolute ground truth for the FS Factbase ETL pipeline. It has been updated to include rigorous data integrity standards for accounting scale and temporal alignment.

## 🏁 High-Level Roadmap & Status

| Phase | Description | Status | Reference |
| :--- | :--- | :--- | :--- |
| **Phase 1** | **IFRS Dictionary & Standards** | ✅ DONE | `DATA_DICTIONARY_PROPOSAL.md` |
| **Phase 2** | **DuckDB Foundation** | ✅ DONE | `db/seed_data.py` |
| **Phase 3** | **Extraction Pipeline** | ✅ DONE | `extractors/pdf_extractor.py` |
| **Phase 4.0** | **Master-Alias Mapping** | ✅ DONE | `transformers/mapper.py` |
| **Phase 4.1** | **HITL Resolultion (UI)** | ✅ DONE | `analytics/app.py` |
| **Phase 4.2** | **Semantic Clustering (Intelligence)** | ✅ DONE | `transformers/cluster_analyzer.py` |
| **Phase 5.0** | **Financial Dashboard** | ✅ DONE | `analytics/app.py` |
| **Phase 5.1** | **Text-to-SQL Analytics** | ✅ DONE | `analytics/sql_agent.py` |
| **Phase 6.0** | **Self-Correction Loop (Hybrid Diagnostics)** | 🏗️ REFACTOR | `scripts/run_diagnostics.py` |
| **Phase 7.0** | **Precision Scaling & Temporal Align** | 🏗️ ACTIVE | `extractors/pdf_extractor.py` |
| **Phase 8.0** | **AI-Led Standard Mandates** | 🏗️ PLANNED | `db/batch_resolver.py` |
| **Phase 9.0** | **Selective Growth & Micro-Staging** | 🏗️ PLANNED | `orchestrator.py`, `transformers/cluster_analyzer.py` |

---

## 🏗️ Phase 6.0: Self-Correction Loop (Hybrid Diagnostics) [REVISED]
**Goal:** Create a true feedback loop where the system updates its own extraction prompts based on human corrections, without wasting LLM tokens on simple math checks.
- **Deterministic Detection:** Python queries `Extraction_Corrections` to count categorical errors (e.g., `SCALE_ERROR`, `SIGN_FLIP`) grouped by `institution_id`.
- **Threshold Triggers:** Once an error type hits a frequency threshold, Python invokes the LLM.
- **AI-Led Rule Rewriting:** The LLM is fed the **Active Prompt**, the **Source Text Snippet**, and the **Error Log**, and is tasked with rewriting the extraction ruleset for that specific institution to prevent future failures.

## 🏗️ Phase 7.0: Precision Scaling & Temporal Alignment (Active)
**Goal:** Solve the "Missing Zeros" problem and ensure apples-to-apples comparisons across reporting periods.
- **Scaling Guardrail:** Extracts must explicitly identify units (Thousands/Millions) and normalize all values (e.g., RM'000 * 1000) before storage.
- **Temporal Guardrail:** Captures the specific **Month-End Date** (1-12) and a **`is_cumulative` flag** to prevent comparing skewed 6-month interim results with 12-month annual snapshots.

## 🏗️ Phase 8.0: AI-Led Standard-Aligned Dictionary (Planned)
**Goal:** Transition the Data Dictionary from a rigid list to an evolving, standard-linked hierarchy.
- **Standard Linkage:** All new cluster resolutions must be linked to an explicit **Accounting Standard** (IFRS 9, Basel III, etc.).
- **AI Suggestion**: The platform will leverage an LLM to suggest appropriate standards and canonical metric IDs based on semantic cluster patterns.

## 🏗️ Phase 9.0: Selective Growth & Micro-Staging
**Goal:** Prevent $O(N^2)$ algorithm freezing in the mapping layer and conserve API costs by abandoning "extract everything" approaches.
- **Targeted Semantic Extraction:** The pipeline must only extract a tightly defined subset of metrics per run (e.g., "The Top 20 Core Metrics"). 
- **Micro-Staging:** By keeping the unmapped items ($N$) artificially low per batch, the clustering algorithms execute in milliseconds. The database grows selectively over time as new targeted batches are processed.

---

## 🤖 Agent Directives (MANDATORY)

1. **Dashboard-First Verification:** Always check `http://127.0.0.1:8000/` to verify if data has been processed correctly.
2. **Never allow an LLM to "guess" standardized metrics.** Extraction stays RAW; Mapping uses the `Metric_Aliases` and `BatchResolver` systems.
3. **Soft-Halt for Safety:** If a mapping is missing, the system MUST send it to `Unmapped_Staging`. Do NOT attempt to "repair" mappings with code logic outside the resolver.
4. **Temporal Consistency:** Never compare incremental interim data with cumulative annual data. Use the `is_cumulative` flag in `Fact_Financials` to filter comparisons.
5. **Scale Factor Enforcement:** All numerical values stored in the DB MUST be fully normalized (e.g., if PDF says RM1,000 and Scale is RM'000, DB must store 1,000,000).
6. **Accounting Standard Mandate:** Every fact in the core dictionary must eventually link to a standard. Do not create "standard-less" metrics during cluster resolution.
7. **Sampled Re-Extraction Protocol:** Before a full run, always perform a **Sampled verification** of exactly 2 reports per bank to validate integrity upgrades.
8. **LLM Budgeting:** Never use an LLM for a task that deterministic Python/SQL can handle. Do not use the LLM to summarize UI tables or detect mathematical variances. Reserve LLM compute strictly for semantic table extraction and diagnostic rule-rewriting.
9. **Enforce Structured Outputs:** All extraction LLM calls MUST utilize native JSON Schema enforcement (e.g., Pydantic through the GenAI SDK) to eliminate markdown parsing failures and reduce output token generation.
10. **Bulk Operations Only:** DuckDB is an OLAP database. Iterative, row-by-row `INSERT` operations in loops are strictly forbidden. Accumulate data in memory and use `conn.executemany()` for bulk transactions.
