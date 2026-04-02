# Master Project Plan: FS Factbase (Revised v6)

This document is the absolute ground truth for the FS Factbase ETL pipeline. It has been updated to reflect the mandate for high-precision benchmarking of Banks and Insurance companies, starting with the Malaysian market.

## 🏁 High-Level Roadmap & Status

| Phase | Description | Status | Reference |
| :--- | :--- | :--- | :--- |
| **Phase 1** | **IFRS Dictionary & Standards** | ✅ DONE | `DATA_DICTIONARY_PROPOSAL.md` |
| **Phase 2** | **DuckDB Foundation** | ✅ DONE | `db/init_db.py` |
| **Phase 3** | **Extraction Pipeline** | ✅ DONE | `extractors/pdf_extractor.py` |
| **Phase 4** | **HITL & Mapping** | ✅ DONE | `transformers/mapper.py`, `analytics/app.py` |
| **Phase A** | **Foundation Hardening** | ✅ DONE | `extractors/pdf_extractor.py` (Gemini SDK) |
| **Phase B** | **Hierarchical Integrity (Variance Engine)** | ✅ DONE | `transformers/variance_engine.py` (Additive Auditor) |
| **Phase C** | **Benchmarking & Comparison Engine** | ✅ DONE | `api/benchmarking.py` (FX & Peer Groups) |
| **Phase D** | **Data Quality & Incremental Growth** | 🏗️ ACTIVE | `db/init_db.py` (UPSERT Logic / Sector Tags) |
| **Phase E** | **Insurance Extension** | 🏗️ PLANNED | `data/dictionaries/insurance.json` |

---

## 🏗️ Phase C: Benchmarking & Comparison Engine (Current Priority)
**Goal:** Deep financial side-by-side analysis, peer ranking, and custom grouping.
- **Comparison Engine:** Logic for ranked lists, peer group calculations, and multi-period trend matrices.
- **Benchmarking UI:** High-DPI interactive charts (Radar, Bar) for cross-institutional comparison.
- **Peer Groups:** User-defined institutional groups (e.g., "Malaysian Big 4", "Islamic Banks").
- **Unified Suite:** Integrate benchmarking directly into the existing Review/Admin dashboard.

---

## 🤖 Agent Directives (MANDATORY)

1. **Accounting Standard Mandate:** Every fact in the core dictionary MUST eventually link to an explicit standard (IFRS 9, 17, Basel III). Do not create "standard-less" metrics during cluster resolution.
2. **Soft-Halt for Safety:** If a mapping is missing, the system MUST send it to `Unmapped_Staging`. Do NOT attempt to "repair" mappings with code logic outside the resolver.
3. **Temporal Consistency:** Never compare incremental interim data with cumulative annual data. Use the `is_cumulative` flag in `Fact_Financials` to filter comparisons.
4. **Scale Factor Enforcement:** All numerical values stored in the DB MUST be fully normalized (e.g., if PDF says RM1,000 and Scale is RM'000, DB must store 1,000,000). Storing integers is preferred for analysis performance.
5. **Sampled Re-Extraction Protocol:** Before a full run, always perform a **Sampled verification** of exactly 2 reports per bank to validate integrity upgrades.
6. **LLM Budgeting:** Never use an LLM for a task that deterministic Python/SQL can handle. Do not use the LLM to summarize UI tables or detect mathematical variances. Reserve LLM compute strictly for semantic table extraction and diagnostic rule-rewriting.
7. **Enforce Structured Outputs:** All extraction LLM calls MUST utilize native JSON Schema enforcement (e.g., Pydantic through the GenAI SDK) to eliminate markdown parsing failures.
8. **Bulk Operations Only:** DuckDB is an OLAP database. Iterative, row-by-row `INSERT` operations in loops are strictly forbidden. Use `conn.executemany()`.
9. **Dual-Fact Strategy:** Extracted ratios are `is_published=True`. System-computed ratios are `is_published=False`. Flag any variance > 1.0%.
10. **Local-First Mandate:** DuckDB only. No cloud/multi-user migrations unless explicitly requested.
