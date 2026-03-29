# FS Factbase: Autonomous Financial ETL Pipeline

## 🚩 PROJECT ROADMAP & STATUS (BIRD'S EYE VIEW)

| Phase | Description | Status | Reference |
| :--- | :--- | :--- | :--- |
| **Phase 1** | **Design & Standardization** | ✅ DONE | `ARCHITECTURE.md`, `DATA_DICTIONARY_PROPOSAL.md` |
| **Phase 2** | **Database Foundations** | ✅ DONE | `db/seed_data.py`, `fs_factbase.duckdb` |
| **Phase 3** | **Precision AI Extraction** | ✅ DONE | `extractors/pdf_extractor.py` |
| **Phase 4** | **Master-Alias Mapping** | ✅ DONE | `transformers/mapper.py` |
| **Phase 5** | **Agentic Text-to-SQL** | ✅ DONE | `analytics/sql_agent.py` |
| **Phase 6** | **Self-Correction Loop** | ✅ DONE | `db/audit_log.py` |

## 🚀 Key Autonomous Features

- **Precision Targeting:** `text_clipper.py` uses semantic scoring to find financial tables in 300+ page PDFs, bypassing token limits.
- **Master-Alias Paradigm:** A deterministic mapping layer that transforms inconsistent banking terms into IFRS-compliant core metrics.
- **Semantic Clustering:** Automatically identifies and groups similar unmapped terms for one-click dictionary expansion.
- **AI Diagnostics:** Learns from human-in-the-loop corrections to improve extraction confidence and trend analysis.
- **Conversational Analytics:** Integrated Text-to-SQL agent for natural language querying of the DuckDB factbase.

> [!NOTE]
> For a detailed technical breakdown of each phase, agent instructions, and current blockers, please see the [Master Plan](file:///.agents/MASTER_PLAN.md).

---

## 1. THE NORTH STAR & ULTIMATE END-STATE
You are acting as the Lead Data Architect. We are building an automated, highly accurate ETL pipeline to extract financial tables from public Annual Reports (starting with PDFs of Malaysian banks) and transform them into a pristine database.

The Ultimate Goal: This data will not just be read by humans; it is being purpose-built to act as the foundation for an LLM-driven application. We will eventually connect an AI to this database using Text-to-SQL to generate instant, mathematically flawless benchmarking insights, business cases, and target operating models for consulting engagements.

The Threat to the Goal: LLMs are notoriously bad at doing raw math on messy data. Furthermore, financial PDFs contain deeply inconsistent terminology across different institutions (e.g., Bank A uses "Advances to customers"; Bank B uses "Gross Loans"). If our extraction pipeline guesses or hallucinates these mappings, our database becomes untrustworthy, and the final Text-to-SQL application fails. 100% data integrity is our highest priority. 

2. THE ARCHITECTURAL MANDATE: THE "MASTER-ALIAS" PARADIGM
To solve the terminology inconsistency, your architecture must strictly adhere to a "Master-Alias" conceptual framework. You are responsible for designing the optimal database schema to support this, but it must obey these logical rules:

- The Immutable Core: The primary data tables must only store strictly standardized, universal metric names aligned with international accounting standards (e.g., IFRS 9 / Basel III for banking). 

- The Translation Engine: The system must contain a mapping layer that links bespoke terminology scraped from individual PDFs to the standardized core metrics.

- The Zero-Hallucination Guardrail: When the extraction logic encounters a line item it has never seen before, it is strictly forbidden from autonomously creating a new core metric. The system must apply a "Soft Halt": processing the rest of the document but isolating that specific unrecognized data point in an `Unmapped_Staging` queue for batch "Human-in-the-Loop" review and manual mapping.

Note: We are starting with the Banking sector, but your architecture must be globally scalable and adaptable enough to eventually handle Insurance (IFRS 17) and international institutions without requiring a structural rewrite.

3. ENGINEERING PRINCIPLES & TECH STACK CONSTRAINTS
I am not prescribing the specific libraries you must use, but your proposed tech stack and the coding standards you establish must adhere to these principles:

- Tidy Data: The final storage format must be heavily optimized for Text-to-SQL querying (flat, normalized, and strictly typed). Do not optimize for human readability in the database layer; we will build presentation views/exports later.

- Portability & Independence: Keep the stack lightweight, robust, and local-first for now. I want to avoid heavy cloud infrastructure bloat during this build phase.

- AI-Assisted Raw Extraction: Instead of brittle regex/heuristic parsing, the initial extraction of complex banking tables from PDFs should leverage LLMs (e.g., Gemini 1.5 Pro) or Document AI APIs to produce structured JSON. However, the subsequent mapping phase must remain 100% deterministic Python/SQL.

- Idempotency & Modularity: Do not write monolithic, shortcut scripts. Extraction, mapping, and database insertion must be distinct, repeatable steps. If a script fails halfway through a 300-page PDF, it should be able to restart cleanly.

- Traceability: Every data point in the final database should ideally have a paper trail (e.g., tracking which source document, year, and page number it came from) to ensure we can audit the AI's extraction accuracy.

4. PHASE 1: PLANNING & STANDARDIZATION (YOUR IMMEDIATE TASK)
Do not write any PDF extraction code or database creation scripts yet. Your first task is to set up the rules of engagement and propose the blueprint. 

Please analyze the constraints above and generate the following Artifacts:

- ARCHITECTURE.md: Propose the technical architecture, the specific tech stack/libraries you recommend using to achieve our goals safely, and how you plan to structure the database to support the Master-Alias paradigm.

- CODING_STANDARDS.md: Establish the strict coding rules, error-handling protocols, and logging conventions you will use to ensure this pipeline remains robust and avoids taking "quick and dirty" shortcuts.

- DATA_DICTIONARY_PROPOSAL.md: Research and propose a starting list of the 10-15 most critical standardized IFRS banking metrics we should use to seed our "Immutable Core".

Review these requirements and provide the artifacts. If you see any logical flaws in the requested approach, point them out now.