# FS Factbase: Deterministic Financial ETL Pipeline (v9)

## 🚩 PROJECT ROADMAP & STATUS

| Phase | Description | Status | Target Scripts |
| :--- | :--- | :--- | :--- |
| **Phase 0** | **Decommissioning & Cleanup** | ✅ DONE | `.archive/` |
| **Phase 1** | **IFRS Dictionary Seeding** | ✅ DONE | `p02.../seed_data.py` |
| **Phase 2** | **DuckDB Foundation** | ✅ DONE | `p02.../init_db.py` |
| **Phase 3** | **Extraction with Fallbacks** | ✅ DONE | `p01.../pdf_extractor.py` |
| **Phase 4** | **Deterministic Mapping** | ✅ DONE | `p02.../mapper.py` |
| **Phase 5** | **Batch AI & CLI HITL** | ✅ DONE | `ai_batch_manager.py`, `rollback_batch.py` |

## 🚀 Core Architectural Mandates

-   **Master-Alias Paradigm:** A deterministic mapping layer that transforms inconsistent banking terms (e.g., "Financing and advances") into IFRS-compliant core metrics (e.g., "Gross Loans").
-   **Universal Reader (LLM):** LLMs are used strictly for raw extraction into structured JSON. No autonomous mapping or accounting "guessing" is permitted.
-   **Tiered Fallback Mechanism:** Survival strategy for API rate-limits. Automatically switches from High-Throughput models (`gemini-2.0-flash-lite`) to Reliable/Deep-Reasoning models if quotas are exhausted.
-   **Terminal-First Verification:** Lightweight CLI tools replace heavy dashboards for human-in-the-loop (HITL) auditing and mapping.
-   **Optimistic AI Batching:** A hybrid resolution strategy. Frequent unmapped terms are automatically mapped by high-confidence AI batches, significantly reducing manual intervention while maintaining a perfect "undo" (rollback) capability.

> [!NOTE]
> For a detailed technical breakdown of each phase and agent instructions, please see the [Master Plan](file:///d:/FS%20Factbase/MASTER_PLAN.md).

---

## 1. THE NORTH STAR
The ultimate deliverable is a pristine, **100% deterministic, auditable DuckDB database** (`fs_factbase.duckdb`) aligned to the official IFRS Accounting Taxonomy.

The database is built to act as a high-fidelity foundation for downstream benchmarking and BI tools. We prioritize **mathematical flawless integrity** over experimental features.

## 2. THE "MASTER-ALIAS" PARADIGM
To solve terminology inconsistency across different institutions (e.g., Bank A vs Bank B), the architecture strictly adheres to:
1.  **The Immutable Core:** Standardized metrics aligned with IFRS 2025.
2.  **The Translation Engine:** A mapping layer linking bespoke PDF text to core metrics.
3.  **The Zero-Hallucination Guardrail:** Unrecognized terms are routed to an `Unmapped_Staging` queue for manual CLI resolution.

## 3. ENGINEERING PRINCIPLES
-   **Tidy Data:** Optimized for high-performance analytical querying in DuckDB.
-   **Traceability:** Every data point tracks its `source_document` and `source_page_number`.
-   **Bulk Operations:** Strict use of `conn.executemany()` to ensure OLAP performance.
-   **Local-First:** Built for robust local execution without heavy cloud dependencies.

## 🤖 Multi-Agent Workflow (Ticket-Driven)
We use a **Multi-Agent Orchestration** pattern to manage the repository. Work is delegated via `tasks.md` tickets to specialized engineers operating in isolated, numbered workspaces (p00-p04).