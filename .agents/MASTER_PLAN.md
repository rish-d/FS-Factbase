# Master Project Plan: FS Factbase (Revised v2)

This document is the absolute ground truth for the FS Factbase ETL pipeline.

## 🏁 High-Level Roadmap & Status

| Phase | Description | Status | Reference |
| :--- | :--- | :--- | :--- |
| **Phase 1** | **IFRS Dictionary & Standards** | ✅ DONE | `DATA_DICTIONARY_PROPOSAL.md` |
| **Phase 2** | **DuckDB Foundation** | ✅ DONE | `db/seed_data.py` |
| **Phase 3** | **Extraction Pipeline** | 🚧 90% | `extractors/` |
| **Phase 4** | **Master-Alias Mapping** | ✅ DONE | `transformers/mapper.py` |
| **Phase 5** | **Financial Dashboard** | 🚧 ACTIVE | `analytics/app.py` |

---

## 🏗️ Phase 3: AI-Assisted Raw Extraction (Active)
**Goal:** Extract targeted tables from 300+ page PDFs without hit quotas or hallucinating.
- **The Text-Clipper:** Successfully built `extractors/text_clipper.py` to target Page 39/41 based on semantic scores.
- **The Extractor:** `extractors/pdf_extractor.py` is written. Stalled by API `429` errors.
- **Status:** Functional but awaiting unblocked API access.

## 🏗️ Phase 4.1: Human-in-the-Loop Resolution (NEW TASK)
**Goal:** Seal the mapping gap for unrecognized bank terms.
- **Task:** When `mapper.py` detects a term it doesn't know, it sends it to `Unmapped_Staging`.
- **Task:** Expand `analytics/app.py` with an `/api/resolve` endpoint to allow manual "Alias Seeding" from the UI.
- **Task:** Add a "Map to Core" button on the Dashboard for pending unmapped items.

## 🏗️ Phase 5: Financial Dashboard & Text-to-SQL (Active)
**Goal:** Visualize the extracted data and provide a conversational query interface.
- **Dashboard:** A FastAPI/Vanilla JS dashboard is operational at `http://127.0.0.1:8000/`.
- **Text-to-SQL:** Once data is mapped, we will add an LLM layer to convert natural language into DuckDB SQL.

---

## 🤖 Agent Directives (MANDATORY)
1. **Never allow an LLM to "guess" standardized metrics.** AI extracts raw text; Python/SQL maps it.
2. **Dashboard-First Verification:** Always check `http://127.0.0.1:8000/` to verify if data has been processed correctly.
3. **Soft-Halt for Safety:** If a mapping is missing, the system MUST send it to `Unmapped_Staging`. Do NOT attempt to "repair" mappings with code logic.
4. **API Simulation:** If the Gemini API is blocked, simulate extraction by manually writing JSON payloads to `data/interim/extracted_metrics/` to test downstream mapping.
