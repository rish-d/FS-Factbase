# Workspace Tasks: Orchestration

This file contains the tickets for the Orchestration Agent.

## 🎯 Context & Alignment (v9)
- **Global Strategy:** [MASTER_PLAN.md](file:///d:/FS%20Factbase/MASTER_PLAN.md)
- **System Architecture:** [ARCHITECTURE.md](file:///d:/FS%20Factbase/ARCHITECTURE.md)
- **Workspace Mission:** [_AGENT_MISSION.md](file:///d:/FS%20Factbase/p04_Orchestration/_AGENT_MISSION.md)

## 📋 Ticket Queue

- [x] Initial setup of `orchestrator.py`.
- [ ] **[CLI RESOLVER]** New Script: `cli_resolver.py`.
    - Provide a terminal-based interface to query `Unmapped_Staging`.
    - Group similar terms for batch mapping to core IDs.
- [ ] **[TERMINAL AUDITOR]** New Script: `view_db.py`.
    - Lightweight data preview tool using the Polars library.
    - Provide balance sheet sum-check reports (Asset/Liab/Eq).
- [ ] **[ORCHESTRATOR REFINEMENT]** Update `orchestrator.py` to act as the primary CLI "glue" for loop-based file extraction and mapping.

---

## 🛠️ Instructions for Orchestration Agent
1. **Scope:** You are responsible only for code within `p04_Orchestration`.
2. **Contract:** Only execute tasks listed in the "Ticket Queue" above.
3. **Updates:** Mark tasks as `[/]` when starting and `[x]` when completed.
