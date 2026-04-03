# Workspace Tasks: Data Extraction

This file contains the tickets for the Extraction Agent.

## 🎯 Context & Alignment (v9)
- **Global Strategy:** [MASTER_PLAN.md](file:///d:/FS%20Factbase/MASTER_PLAN.md)
- **System Architecture:** [ARCHITECTURE.md](file:///d:/FS%20Factbase/ARCHITECTURE.md)
- **Workspace Mission:** [_AGENT_MISSION.md](file:///d:/FS%20Factbase/p01_Data_Extraction/_AGENT_MISSION.md)

## 📋 Ticket Queue

- [x] Initial setup of Gemini SDK and `pdf_extractor.py`.
- [x] Implementation of `text_clipper.py` for semantic page targeting.
- [ ] **[TIERED FALLBACKS]** Update `pdf_extractor.py` to handle 429 Resource Exhausted errors with the following fallback sequence:
    1. `gemini-2.0-flash-lite-preview-02-05`
    2. `gemini-2.0-flash`
    3. `gemini-1.5-pro` (or similar deep reasoning model)
- [ ] **[TRACEABILITY]** Ensure `pdf_extractor.py` and `batch_extractor.py` strictly capture `source_document` and `source_page_number` in the `FSDataPayload` Pydantic model.
- [ ] **[INGESTION REFINE]** Update `ingestor.py` to use absolute package imports and ensure it outputs only clean, validated JSON strings (no markdown blocks).

---

## 🛠️ Instructions for Extraction Agent
1. **Scope:** You are responsible only for code within `p01_Data_Extraction`.
2. **Contract:** Only execute tasks listed in the "Ticket Queue" above.
3. **Updates:** Mark tasks as `[/]` when starting and `[x]` when completed.
