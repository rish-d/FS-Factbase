# Workspace Tasks: Database and Mapping

This file contains the tickets for the Database Agent.

## 🎯 Context & Alignment (v9)
- **Global Strategy:** [MASTER_PLAN.md](file:///d:/FS%20Factbase/MASTER_PLAN.md)
- **System Architecture:** [ARCHITECTURE.md](file:///d:/FS%20Factbase/ARCHITECTURE.md)
- **Workspace Mission:** [_AGENT_MISSION.md](file:///d:/FS%20Factbase/p02_Database_and_Mapping/_AGENT_MISSION.md)

## 📋 Ticket Queue

- [x] Initial setup of DuckDB schema and Master-Alias paradigm.
- [x] Implementation of IFRSAT-2025 seeding in `seed_data.py`.
- [/] **[SEED REFINEMENT]** Refine and expand the 80+ core metrics in `seed_data.py` based on the official IFRS taxonomy files. Ensure all IDs use the `ifrs-full_` prefix accurately. (v9: Islamic metrics deferred).
- [/] **[MAPPER REFACTOR]** Update `mapper.py` to remove ALL `TrendAnalyzer` imports and related logic. Decouple DB insertion from analytical overhead.
- [/] **[ZERO-HALLUCINATION]** Enforce strict routing to `Unmapped_Staging` in `mapper.py`. Any `raw_term` failing an alias match must be isolated.
- [/] **[TRACEABILITY]** Verify that `mapper.py` correctly handles `source_document` and `source_page_number`. 
    - **Note:** Blocked by p01. Extraction Agent must fix the missing page numbers in raw JSONs before mapping can be finalized.

---

## 🛠️ Instructions for Database Agent
1. **Scope:** You are responsible only for code within `p02_Database_and_Mapping`.
2. **Contract:** Only execute tasks listed in the "Ticket Queue" above.
3. **Updates:** Mark tasks as `[/]` when starting and `[x]` when completed.
