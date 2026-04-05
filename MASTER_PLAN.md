# Master Project Plan: FS Factbase (Revised v9)

This document is the absolute ground truth for the FS Factbase ETL pipeline. It dictates the architecture, phased roadmap, and the specific rationale behind the current design to ensure all agents execute their tasks in perfect alignment with the project's primary mandate.

## 🧠 Architectural Context & Decision Rationale (For Agent Alignment)

**To all operating agents:** This project has undergone a strategic realignment. You must understand the *why* behind this architecture to avoid reverting to out-of-scope behaviors.

1.  **The Core Mandate over Scope Creep:** The ultimate deliverable is a pristine, 100% deterministic, auditable DuckDB database (`fs_factbase.duckdb`) aligned to the official IFRS Accounting Taxonomy. We have explicitly decommissioned the FastAPI dashboard, Text-to-SQL agents, and automated AI diagnostics. 
    *   **Rationale:** Custom UIs and experimental AI inference introduce massive technical debt and instability. The database is the product; downstream BI tools will handle visualization.
2.  **The "Master-Alias" Paradigm (Structure vs. Semantics):** We use LLMs for extraction, but strictly Python/SQL for mapping. 
    *   **Rationale:** LLMs are powerful "Universal Readers" capable of navigating diverse PDF layouts to output strict JSON schemas, but they are non-deterministic accountants. By extracting the exact printed text (e.g., "Financing and advances") and using a deterministic Python mapper to translate it to an IFRS standard (e.g., "Gross Loans"), we prevent hallucinated accounting and guarantee an auditable paper trail to the source page.
3.  **The API Quota Survival Strategy (Tiered & Offline):** The extraction phase uses a tiered fallback mechanism and a manual "Fast-Track" bypass.
    *   **Rationale:** Free Tier APIs are severely rate-limited. We survive batch processing via "Lite" models and the **Offline Fast-Track Mode** (generating prompts for Gemini Web UI). This ensures high-priority reports can move through the pipeline even when API quotas are exhausted.
4.  **Local-First Inference (Future-Proofing):** We are transitioning toward Local LLM support (Ollama/Llama 3).
    *   **Rationale:** To achieve 24/7 autonomous extraction without API costs or token caps, shifting the "Universal Reader" role to local hardware is mandatory.
5.  **The Role of the Orchestrator:** The `orchestrator.py` script remains as a lightweight workflow manager. 
    *   **Rationale:** Separation of Concerns. The extraction layer (p01) and database layer (p02) must remain completely decoupled. The orchestrator acts as the "glue" that loops through files, enforces the "Claimed" status for manual runs, and manages rate-limiting pauses.

## 🏁 High-Level Roadmap & Status

| Phase | Description | Status | Target Scripts |
| :--- | :--- | :--- | :--- |
| **Phase 0** | **Decommissioning & Cleanup** | ✅ DONE | `.archive/p03`, `rm` logs |
| **Phase 1** | **IFRS Dictionary Seeding** | ✅ DONE | `p02.../seed_data.py` |
| **Phase 2** | **DuckDB Foundation** | ✅ DONE | `p02.../init_db.py` |
| **Phase 3** | **Extraction with Fallbacks** | ✅ DONE | `p01.../pdf_extractor.py` |
| **Phase 4** | **Deterministic Mapping** | ✅ DONE | `p02.../mapper.py` |
| **Phase 5** | **Batch AI & CLI HITL** | ✅ DONE | `ai_batch_manager.py`, `rollback_batch.py` |
| **Phase 6** | **Local LLM Inference** | ✅ DONE | `llm_factory.py`, `ollama` |

---

## 🏗️ Phase 0: Decommissioning (Pre-requisite)
**Goal:** Eliminate out-of-scope components.
*   **Archive Analytics:** Move `p03_Analytics_Dashboard` to `.archive/`.
*   **Prune Orchestration:** Delete legacy diagnostics scripts (`run_diagnostics.py`, `verify_hitl.py`).

## 🏗️ Phase 1 & 2: The Immutable Core
**Goal:** Seed the database with the official IFRS Accounting Taxonomy.
*   **Action:** Update `seed_data.py` to refine the 80+ core metrics sourced from the official IFRS taxonomy, ensuring precise `data_type` and `accounting_standard` mappings.
*   **Action:** Execute `init_db.py` and `seed_data.py` to create a fresh, clean `fs_factbase.duckdb` file.

## 🏗️ Phase 3: AI-Assisted Raw Extraction
**Goal:** Extract targeted tables into JSON without hitting 429 quota crashes.
*   **Text-Clipping Mandate:** Always pass PDFs through `text_clipper.py` to isolate target pages before extracting.
*   **Tiered Fallback Mechanism:** Update `pdf_extractor.py` to handle 429 Resource Exhausted errors via model fallbacks.
    *   **Tier 1:** `gemini-2.0-flash-lite` (High Quota)
    *   **Tier 2:** `gemini-2.0-flash` (Reliable)
    *   **Tier 3:** `gemini-2.0-pro` / `gemini-1.5-pro` (Deep Reasoning)
*   **Strict Pydantic JSON:** Ensure `response_schema` parameter is strictly enforced using the `FSDataPayload` model.

## 🏗️ Phase 4: Deterministic Mapping
**Goal:** Translate raw PDF text to IFRS standards deterministically.
*   **Zero-Hallucination Guardrail:** Strictly forbidden from bypassing alias checks. Any `raw_term` failing a match against `Metric_Aliases` MUST be routed to `Unmapped_Staging`.
*   **Traceability:** Every fact in `Fact_Financials` MUST include `source_document` and `source_page_number`.

## 🏗️ Phase 5: Optimistic AI & CLI HITL
**Goal:** Build lightweight terminal tools for verification and high-speed queue clearing.
*   **Optimistic AI Batcher (`ai_batch_manager.py`):** Automated resolution of unmapped terms. Groups frequent terms and maps them using high-confidence LLM suggestions (90%+ threshold).
*   **Batch Rollback Engine (`rollback_batch.py`):** Ensures 100% auditability and safety. Allows reversing any AI-generated batch without metadata loss.
*   **Terminal Auditing (`view_db.py` / `view_facts.py`):** Lightweight terminal scripts to audit balance sheet sum-checks and explore standardized facts across banks and years.

## 🏗️ Phase 6: Local-First Inference (Upcoming)
**Goal:** Eliminate API dependencies by routing extraction and reconciliation to local models.
*   **Unified LLM Factory**: Implement `llm_factory.py` to abstract providers (Gemini vs. Ollama).
*   **Hardware-Aware Extraction**: Configure high-throughput local models (Llama 3.1 8B, DeepSeek) to run constant extraction loops without cost.
*   **Continuous Orchestration Loop**: [continuous_orchestrator.py](file:///d:/FS%20Factbase/p04_Orchestration/continuous_orchestrator.py) implements the end-to-end Extraction -> Mapping -> Expansion cycle.
*   **Decoupled Monitoring Dashboard**: [dashboard_server.py](file:///d:/FS%20Factbase/api/dashboard_server.py) provides a lightweight, premium window into the factbase's growth and pipeline state.

---

## 🤖 Agent Directives (MANDATORY)

1.  **Accounting Standard Mandate:** Every fact in the core dictionary MUST eventually link to an explicit standard (IFRS 9, IFRS 17, Basel III). Do not create "standard-less" metrics during cluster resolution.
2.  **Soft-Halt for Safety:** If a mapping is missing, the system MUST send it to `Unmapped_Staging`. Do NOT attempt to "repair" mappings with code logic outside the resolver.
3.  **Temporal Consistency:** Never compare incremental interim data with cumulative annual data. Use the `is_cumulative` flag in `Fact_Financials` to filter comparisons.
4.  **Scale Factor Enforcement:** All numerical values stored in the DB MUST be fully normalized (e.g., if PDF says RM1,000 and Scale is RM'000, DB must store 1,000,000). Storing integers is preferred for analysis performance.
5.  **Sampled Re-Extraction Protocol:** Before a full run, always perform a **Sampled verification** of exactly 2 reports per bank to validate integrity upgrades.
6.  **LLM Budgeting:** Never use an LLM for a task that deterministic Python/SQL can handle. Do not use the LLM to summarize UI tables or detect mathematical variances. Reserve LLM compute strictly for semantic table extraction and diagnostic rule-rewriting.
7.  **Enforce Structured Outputs:** All extraction LLM calls MUST utilize native JSON Schema enforcement (e.g., Pydantic through the GenAI SDK) to eliminate markdown parsing failures.
8.  **Bulk DB Operations:** DuckDB is an OLAP database. Iterative, row-by-row `INSERT` operations in loops are strictly forbidden. Use `conn.executemany()`.
9.  **Dual-Fact Strategy:** Extracted ratios are `is_published=True`. System-computed ratios are `is_published=False`. Flag any variance > 1.0%.
10. **Local-First Isolation:** Keep modules decoupled. p01 handles PDFs/LLMs. p02 handles DuckDB. Do not mix dependencies.
11. **Patience & Persistence:** If Tier 1 fails, gracefully log the warning and automatically try Tier 2 before halting.
12. **Audit Trail:** Maintain the link from fact to source page for 100% auditability.
13. **Local-First Mandate:** DuckDB only. No cloud/multi-user migrations unless explicitly requested.
