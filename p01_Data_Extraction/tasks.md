# Workspace Tasks: Data Extraction

This file contains the tickets for the Extraction Agent.

## 🎯 Context & Alignment (v9)
- **Global Strategy:** [MASTER_PLAN.md](file:///d:/FS%20Factbase/MASTER_PLAN.md)
- **System Architecture:** [ARCHITECTURE.md](file:///d:/FS%20Factbase/ARCHITECTURE.md)
- **Workspace Mission:** [_AGENT_MISSION.md](file:///d:/FS%20Factbase/p01_Data_Extraction/_AGENT_MISSION.md)

## 📋 Ticket Queue

- [x] Initial setup of Gemini SDK and `pdf_extractor.py`.
- [x] Implementation of `text_clipper.py` for semantic page targeting.
- [x] **[TIERED FALLBACKS]** Update `pdf_extractor.py` to handle 429 Resource Exhausted errors with the following fallback sequence:
    1. `gemini-2.5-flash-lite`
    2. `gemini-2.0-flash-lite`
    3. `gemini-2.0-flash`
    4. `gemini-2.5-flash`
- [x] **[TRACEABILITY]** (v9 Regression) `pdf_extractor.py` and `batch_extractor.py` include the `source_page_number` field in the schema, and now successfully extract it for each data point.
    - **Requirement:** Re-verify the LLM prompt and ensure page markers (e.g., `--- TARGET FINANCIAL PAGE {page_num} ---`) are correctly parsed by the vision/model layer.
- [x] **[INGESTION REFINE]** Update `ingestor.py` to use absolute package imports and ensure it outputs only clean, validated JSON strings (no markdown blocks).

---

## 🛠️ Instructions for Extraction Agent
1. **Scope:** You are responsible only for code within `p01_Data_Extraction`.
2. **Contract:** Only execute tasks listed in the "Ticket Queue" above.
3. **Updates:** Mark tasks as `[/]` when starting and `[x]` when completed.
