# Master Project Plan: FS Factbase (Revised v4)

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
| **Phase 6.0** | **Self-Correction Loop (AI Lessons)** | ✅ DONE | `db/audit_log.py` |
| **Phase 7.0** | **Precision Scaling & Temporal Align** | 🏗️ ACTIVE | `extractors/pdf_extractor.py` |
| **Phase 8.0** | **AI-Led Standard Mandates** | 🏗️ PLANNED | `db/batch_resolver.py` |

---

## 🏗️ Phase 7.0: Precision Scaling & Temporal Alignment (Active)
**Goal:** Solve the "Missing Zeros" problem and ensure apples-to-apples comparisons across reporting periods.
- **Scaling Guardrail:** Extracts must explicitly identify units (Thousands/Millions) and normalize all values (e.g., RM'000 * 1000) before storage.
- **Temporal Guardrail:** Captures the specific **Month-End Date** (1-12) and a **`is_cumulative` flag** to prevent comparing skewed 6-month interim results with 12-month annual snapshots.

## 🏗️ Phase 8.0: AI-Led Standard-Aligned Dictionary (Planned)
**Goal:** Transition the Data Dictionary from a rigid list to an evolving, standard-linked hierarchy.
- **Standard Linkage:** All new cluster resolutions must be linked to an explicit **Accounting Standard** (IFRS 9, Basel III, etc.).
- **AI Suggestion**: The platform will leverage an LLM to suggest appropriate standards and canonical metric IDs based on semantic cluster patterns.

---

## 🤖 Agent Directives (MANDATORY)

1. **Dashboard-First Verification:** Always check `http://127.0.0.1:8000/` to verify if data has been processed correctly.
2. **Never allow an LLM to "guess" standardized metrics.** Extraction stays RAW; Mapping uses the `Metric_Aliases` and `BatchResolver` systems.
3. **Soft-Halt for Safety:** If a mapping is missing, the system MUST send it to `Unmapped_Staging`. Do NOT attempt to "repair" mappings with code logic outside the resolver.
4. **Temporal Consistency:** Never compare incremental interim data with cumulative annual data. Use the `is_cumulative` flag in `Fact_Financials` to filter comparisons.
5. **Scale Factor Enforcement:** All numerical values stored in the DB MUST be fully normalized (e.g., if PDF says RM1,000 and Scale is RM'000, DB must store 1,000,000).
6. **Accounting Standard Mandate:** Every fact in the core dictionary must eventually link to a standard. Do not create "standard-less" metrics during cluster resolution.
7. **Sampled Re-Extraction Protocol:** Before a full run, always perform a **Sampled verification** of exactly 2 reports per bank to validate integrity upgrades.
