# Workspace Tasks: Shared Utilities

This file contains the tickets for the Utilities Agent.

## 🎯 Context & Alignment (v9)
- **Global Strategy:** [MASTER_PLAN.md](file:///d:/FS%20Factbase/MASTER_PLAN.md)
- **System Architecture:** [ARCHITECTURE.md](file:///d:/FS%20Factbase/ARCHITECTURE.md)
- **Workspace Mission:** [_AGENT_MISSION.md](file:///d:/FS%20Factbase/p00_Shared_Utils/_AGENT_MISSION.md)

## 📋 Ticket Queue

- [x] Initial setup of `io_utils.py`, `logging_utils.py`, and `date_utils.py`.
- [ ] **[PATH RESOLUTION]** Update `io_utils.py` to provide a robust `get_root_dir()` function that accurately resolves the project root regardless of where a script is executed from.
- [ ] **[LOGGING HARDENING]** Standardize log formatting across all workspaces to ensure 100% auditability of extraction and mapping steps.
- [ ] **[AUDIT TRAIL UTILS]** Create a helper function to validate `source_document` paths and `source_page_number` types for Phase 4 compliance.

---

## 🛠️ Instructions for Utilities Agent
1. **Scope:** You are responsible only for code within `p00_Shared_Utils`.
2. **Contract:** Only execute tasks listed in the "Ticket Queue" above.
3. **Updates:** Mark tasks as `[/]` when starting and `[x]` when completed.
