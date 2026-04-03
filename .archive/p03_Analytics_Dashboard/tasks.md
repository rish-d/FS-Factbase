# Workspace Tasks: Analytics Dashboard

This file contains the tickets for implementation work within this workspace.

## 🎯 Context & Alignment
- **Global Strategy:** [MASTER_PLAN.md](file:///d:/FS%20Factbase/MASTER_PLAN.md)
- **System Architecture:** [ARCHITECTURE.md](file:///d:/FS%20Factbase/ARCHITECTURE.md)
- **Workspace Mission:** [_AGENT_MISSION.md](file:///d:/FS%20Factbase/p03_Analytics_Dashboard/_AGENT_MISSION.md)
- **Current Objective:** Build dynamic tables for IT Spend and Customer Deposits.
- **Micro-Goal:** Enable 2021-2024 peer-comparison table view.

## 📋 Ticket Queue

- [ ] initialize dashboard UI with benchmarking charts
- [ ] implementation of Text-to-SQL logic for natural language queries
- [ ] add institutional benchmarking UI with peer ranking
- [ ] **[RESULTS TABLE]** Create a primary results viewer for the 2021-2024 batch.
    - Requirements: Table format only.
    - Features: Toggle between "Information technology expenses" and "Total customer deposits" to display bank performance across the 2021-2024 timeline.
- [ ] **[RESTORE]** Resolve the `500 Internal Server Error` on the Dashboard root route (likely due to relative pathing for `index.html`).
- [ ] **[MAINTENANCE]** Implement a `restart_dashboard.ps1` script to handle stale PID termination and safe port binding for port 8000.
- [ ] **[PREMIUM CHECK]** Verify UI styling and Chart.js responsiveness across all dashboard modules.
- [ ] **[DELEGATION]** Ensure that any PDF preview links or file-system-dependent features in the dashboard point to the NEW centralized `data\raw\reports` folder at the project root.

---

## 🛠️ Instructions for Analytics Dashboard Agent
1. **Scope:** You are responsible only for code within `p03_Analytics_Dashboard`.
2. **Contract:** Only execute tasks listed in the "Ticket Queue" above.
3. **Updates:** Mark tasks as `[/]` when starting and `[x]` when completed.
4. **No Side Effects:** Do not modify code in other `pXX` folders.
