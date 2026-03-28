# Master Project Plan: FS Factbase

This document serves as the "Bird's Eye" strategic roadmap for all agents and developers working on the FS Factbase extraction pipeline. 

## Project Objective
Transform massive, unstandardized Bank Annual Report PDFs (ranging from 50 to 350+ pages) into a structured, highly accurate local DuckDB database. This is achieved using a **Master-Alias Mapping Paradigm** to guarantee 100% data integrity while leveraging AI strictly for OCR and layout extraction.

---

## Phase 1: Planning & Foundational Architecture ✅ (Completed)
**Goal:** Define the immutable core logic to prevent LLM hallucination.
- Documented a standard dictionary of 15 IFRS/Basel III banking metrics.
- Enforced a hard rule: AI is never allowed to "guess" or "normalize" a metric's meaning. AI only extracts verbatim text.
- Defined the "Soft Halt" mechanism for unknown metric tagging.

## Phase 2: Database & Engineering Foundation ✅ (Completed)
**Goal:** Initialize the local data layer.
- Set up `fs_factbase.duckdb` via `db/seed_data.py`.
- Architected 4 rigid internal schemas:
  1. `Core_Metrics` (The Immutable Standard)
  2. `Metric_Aliases` (The Translation Engine)
  3. `Fact_Financials` (The Production Query Table)
  4. `Unmapped_Staging` (The Human-in-the-Loop Quarantine Zone)

## Phase 3: AI-Assisted Raw Extraction 🔄 (Active / Structurally Complete)
**Goal:** Convert huge, noisy PDFs into a clean, predictable JSON payload.
- **The Text-Clipper:** Large PDFs broke the Google File API quotas. We built `extractors/text_clipper.py` to use `PyMuPDF` locally. It automatically scores and cuts down 300-page Annual Reports into two precise text blobs (Balance Sheet and Income Statement) in < 1 second.
- **The Extractor:** `extractors/pdf_extractor.py` passes the clipped text snippet to `google-genai` using Pydantic schemas to strictly format the output into `FactMetric` models (Raw Term, Value, Page Number).
- **Blocker:** Currently diagnosing strict Google Cloud Free Tier quotas (`429 Limit: 0`) in the user's localized region when pulling `gemini-2.0-flash`. 

## Phase 4: Deterministic Transformation & Loading ⏳ (Pending)
**Goal:** Map the LLM's raw JSON payload into the DuckDB ecosystem.
- Build `transformers/mapper.py`.
- Ingest the JSON output from Phase 3.
- Run a `SELECT` against `Metric_Aliases` based on the AI's literal `raw_term`:
  - If a match exists: Route instantly into `Fact_Financials`.
  - If no match exists (e.g., the bank invented a new line item): Route that item to `Unmapped_Staging` for manual human-in-the-loop review.
- This phase completely guarantees 0% hallucinated data enters production.

## Phase 5: Agentic Text-to-SQL Analytics ⏳ (Pending)
**Goal:** Provide an end-user chat interface.
- With data perfectly standardized in `Fact_Financials`, attach an LLM that translates simple user questions (e.g., *"Show me Maybank's loan growth vs CIMB in 2024"*) into high-accuracy DuckDB SQL queries.

---

## Agent Directives
Any AI working on this repository must abide by the following unshakeable rules:
1. **Never allow an LLM to "standardize" financial data on the fly.** Read verbatim from the PDF, map deterministically in Python/SQL.
2. **Respect the API Quotas.** The Gemini File API is heavy; passing plain text strings is vastly preferred for Free Tier limits.
3. **If something fails to execute, don't guess.** Debug the local environment (e.g., SSL Hangs or Terminal Timeouts) before refactoring flawlessly written code.
