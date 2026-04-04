# FS Factbase Coding Standards & Protocols

## 1. Zero-Hallucination Data Integrity Protocol
- **Strict Typing Override**: Every line of data processed from external systems/files MUST pass through a Pydantic Model specification before reaching Database insertion logic.
- **No Implicit Casting**: Missing values must be encoded uniformly as explicit NULL/`None` representations, not implied zeros.
- **No Autonomous Metric Creation**: Code is strictly forbidden from bypassing alias checks. Any term failing a `LEFT JOIN` match against `Metric_Aliases` MUST be logged and routed to `Unmapped_Staging`.

## 2. Idempotency & Fault Tolerance
- **Atomic Operations**: Each workflow stage (Extract, Map, Load) should be completely isolated and stateless. 
- **Crash Recovery**: You must be able to run an extraction or load script over the same document 100 times, and the database should look identical to having run it once (e.g., using `UPSERT`/`MERGE` operations driven by composite unique keys).

## 3. Error Handling & Traceability
- **Traceability Metadata**: No value is allowed in the `Fact_Financials` table without a `source_document` string and `source_page_number` integer.
- **Verbose Logging**: A standard logger must be invoked (e.g., `Loguru`) to log step status:
   - `INFO`: Starting PDF extraction, successfully executed DAG module.
   - `WARNING`: An unrecognized alias encountered, routing item to Staging Queue.
   - `ERROR`: Unexpected PDF formatting failure / missing required JSON keys during API pull.

## 4. Local-First Configuration
- **Environment Variables**: Avoid hardcoded URIs. Rely on `.venv` or `.env` implementations (via `pydantic-settings`) for database paths, API keys for the extraction models, and debug flags.
- **Modular Scripts**: Code organization should visually mirror the pipeline:
  ```
  /p01_Data_Extraction/       (LLM prompts, PDF clipping, and API calls)
  /p02_Database_and_Mapping/  (Schema, IFRS seeding, and Master-Alias mapping)
  /p04_Orchestration/         (Workflow glue and CLI HITL tools)
  ```

## 5. LLM API Governance
- **Zero-Redundancy Calls:** Do not chain LLM calls if the data is already structured. Reserve LLM compute strictly for semantic table extraction and diagnostic rule-rewriting.
- **Strict JSON Enforcement:** Do not rely on "prompt engineering" alone to get JSON. You must pass explicit Pydantic schemas to the LLM API using the `response_schema` parameter to guarantee deterministic structuring.

## 6. Database I/O Optimization
- **Ban on Iterative Inserts:** Never place a `conn.execute("INSERT INTO...")` statement inside a `for` loop. 
- **Bulk Execution:** Accumulate tuples in a list during iteration, then perform a single `conn.executemany()` outside the loop.
- **Connection Management:** For CLI tools and extraction loops, avoid opening and closing `duckdb.connect()` unnecessarily. Maintain a stable connection for the duration of the batch operation to minimize overhead.
