# FS Factbase: Project Handover Document

Welcome! You are resuming an active software engineering session. Please read this entire document carefully before proceeding.

## 1. Project Objective
Build a hallucination-free financial ETL pipeline that extracts deep tabular data from Bank Annual Report PDFs (e.g., CIMB, Maybank) using a **Master-Alias Paradigm** (mapping bank-specific jargon to an immutable IFRS/Basel III core representation), storing it in DuckDB for downstream RAG / Text-to-SQL logic.

## 2. Project Status & Architecture
We have successfully completed Phases 1 through 3.

*   **Phase 1 (Design):** Established 15 core financial metrics in standard IFRS terminology.
*   **Phase 2 (Database):** Initialized `fs_factbase.duckdb` and seeded the core framework (Core Metrics, Aliases, Fact Tables, Unmapped Staging schemas).
*   **Phase 3 (Extraction Setup):** 
    *   Migrated 5 CIMB reports and 4 Maybank reports into `data/raw/reports/{institution_id}/YYYY_fs.pdf`.
    *   **The Text-Clipper Architecture:** Instead of using the Gemini File API (which hit severe quota limits), we created `extractors/text_clipper.py`. It uses `PyMuPDF` to instantly read the entire PDF, score every page for structural keyword density, and bypasses the Table of Contents to output pure text strings for *only* the specific pages containing the Balance Sheet and Income Statement (e.g., Page 39 & 41).
    *   **The LLM Extractor:** `extractors/pdf_extractor.py` accepts the tiny plain-text payload from the Clipper and prompts `gemini-2.0-flash` (using the official `google-genai` active SDK and `Pydantic` JSON schemas) to deterministically output structured metric lists.

## 3. Known Issues & Blockers
During the previous session, the script `python -m extractors.pdf_extractor` was abruptly interrupted every time it was executed due to a `KeyboardInterrupt`. 
We suspect this was either an IDE timeout or an SSL certificate hang in the Python environment, compounded by potential geo-blocked `limit: 0` Free Tier quotas for `gemini-2.0-flash`. 

## 4. Immediate Next Steps for the New Agent
1. **Verify the Environment:** Check if `python -m extractors.pdf_extractor` executes cleanly in this fresh terminal thread. 
2. **Handle the Quota Issue:** If Google's API returns `429 RESOURCE_EXHAUSTED` (Limit 0), you must inform the user. The user is in Malaysia trying to use the Free Tier; consider testing `gemini-1.5-flash-8b` or `gemini-1.5-pro` strictly as text (not File API) to see if it bypasses the zero-quota billing check.
3. **Execute Phase 4:** If the API continues to crash, bypass Phase 3 entirely. Create a "dummy" extracted JSON payload resembling the Pydantic schema `FSDataPayload`, and immediately begin building **Phase 4: Deterministic Transformation**. Phase 4 requires writing `/transformers/mapper.py` to route the JSON data into `fs_factbase.duckdb` utilizing the "Soft Halt" logic for unrecognized aliases.

## 5. Technical Stack
- Python 3.11 (`.venv` activated)
- `DuckDB`, `Polars`, `Loguru`
- `google-genai` (For Gemini API)
- `PyMuPDF` (For local Text Clipping)
