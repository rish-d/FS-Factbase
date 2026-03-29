import os
import json
import time
from typing import List, Dict, Any, Union
from pydantic import BaseModel, Field
from loguru import logger
from dotenv import load_dotenv

# Modern SDK Migration
from google import genai
from google.genai import types

from extractors.text_clipper import get_clipped_financial_text_dynamic

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
DUMMY_MODE = os.getenv("DUMMY_MODE", "false").lower() == "true"

if not API_KEY or API_KEY == "your_google_api_key_here":
    if not DUMMY_MODE:
        logger.error("GEMINI_API_KEY is missing from the .env file.")
        exit(1)

class YearValue(BaseModel):
    year: int = Field(description="The reporting year, e.g., 2024", validation_alias="reporting_year")
    value: float | None = Field(description="The numerical value. Use null for headers or empty cells.", validation_alias="amount")
    confidence: float = Field(description="AI's confidence in this specific value (0.0 to 1.0)", default=1.0)
    notes: str | None = Field(description="Brief notes on any extraction difficulty.", default=None)

class LineItem(BaseModel):
    item: str = Field(description="The exact name of the line item as seen in the table.", validation_alias="item_name")
    group: str | None = Field(description="The category group if discernible, e.g., 'Assets'.", default=None)
    values: List[Any] = Field(description="List of values for available years. Can be numbers or year/value objects.", validation_alias="data_points")

class Statement(BaseModel):
    statement_type: str = Field(description="e.g., 'Balance Sheet' or 'Income Statement'")
    statement_name: str | None = Field(description="The full name of the statement.", default=None)
    currency: str | None = Field(description="The currency unit, e.g., RM'000", default=None)
    line_items: List[LineItem] = Field(description="List of all extracted line items for this statement.")

class FSDataPayload(BaseModel):
    institution_id: str | None = Field(description="The canonical ID of the institution", default=None)
    reporting_period: str | None = Field(description="The primary reporting period, e.g., 2024", default=None)
    source_document: str | None = Field(description="The name of the PDF file", default=None)
    statements: Union[List[Statement], Dict[str, Statement]] = Field(
        description="The extracted financial statements.",
        validation_alias="financial_statements"
    )

def get_diagnostic_lessons(institution_id: str) -> str:
    """Fetches active diagnostic lessons for the bank from DuckDB."""
    try:
        import duckdb
        conn = duckdb.connect("fs_factbase.duckdb", read_only=True)
        rows = conn.execute("SELECT error_pattern, advice FROM Diagnostic_Lessons WHERE institution_id = ? AND is_active = TRUE", [institution_id]).fetchall()
        conn.close()
        if not rows:
            return ""
        lessons_text = "\n### LESSONS FROM PAST ERRORS ###\n"
        for r in rows:
            lessons_text += f"- {r[0]}: {r[1]}\n"
        return lessons_text
    except:
        return ""

def extract_financials_from_text(clipped_text: str, institution_id: str, reporting_period: str, filename: str, user_prompt: str) -> str:
    if DUMMY_MODE:
        logger.warning("Running in DUMMY_MODE. Returning static JSON payload.")
        dummy_data = {
            "institution_id": institution_id,
            "reporting_period": reporting_period,
            "source_document": filename,
            "statements": [
                {
                    "statement_type": "Balance Sheet",
                    "statement_name": "Consolidated Statement of Financial Position",
                    "currency": "RM'000",
                    "line_items": [
                        {
                            "item": "Cash and short-term funds",
                            "group": "Assets",
                            "values": [{"year": 2024, "value": 45000000.0}]
                        }
                    ]
                }
            ]
        }
        return json.dumps(dummy_data)

    system_instruction = (
        f"You are a master financial OCR AI. Your task is to extract tabular data based on this specific user request: '{user_prompt}'. "
        "Return a SINGLE JSON object matching the schema exactly. "
        "IMPORTANT: Use the key 'financial_statements' for the list of extracted statements. "
        "Extract both the current and comparative years (e.g., 2024 and 2023) if present. "
        "Use the 'values' list to store year-specific numbers. Convert (X) to -X. "
        "Always prioritize verbatim accuracy."
    )
    
    lessons = get_diagnostic_lessons(institution_id)
    
    prompt = f"""
    Extact financial statements for:
    - institution_id: {institution_id}
    - reporting_period: {reporting_period}
    - source_document: {filename}
    {lessons}
    
    ### TEXT SOURCE ###
    {clipped_text}
    ### END TEXT ###
    """

    logger.info("Prompting Gemini (gemini-1.5-flash) via Modern SDK...")
    
    try:
        client = genai.Client(api_key=API_KEY)
        
        # Exponential Backoff Retry Loop for Quota Issues
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = client.models.generate_content(
                    model='gemini-1.5-flash',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=system_instruction,
                        response_mime_type='application/json'
                    )
                )
                if response and response.text:
                    return response.text
                else:
                    logger.error("Empty response or blocked content.")
                    return ""
            except Exception as e:
                # Quota exceeded retry logic
                if "429" in str(e) and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 30
                    logger.warning(f"Quota exceeded. Retrying in {wait_time}s... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"GenAI Call failed after retries: {e}")
                    raise e
                    
    except Exception as e:
        logger.error(f"Ultimate Extraction failure: {e}")
        return ""

def process_report(pdf_path: str, institution_id: str, reporting_period: str, user_prompt: str = "Balance Sheet and Income Statement"):
    filename = os.path.basename(pdf_path)
    logger.info(f"--- Starting Extraction: {institution_id} ({reporting_period}) ---")
    logger.info(f"Task: {user_prompt}")
    
    # Step 1: Text-First Clipping
    logger.info(f"Step 1: Text-First Clipping for {pdf_path}")
    clipped_text = get_clipped_financial_text_dynamic(pdf_path, user_prompt)
    
    if not clipped_text:
        logger.error(f"Failed to find relevant pages for {filename}")
        return None
        
    # Step 2: Extract structured JSON using Modern SDK
    logger.info("Step 2: Sending pure text to LLM API")
    raw_json = extract_financials_from_text(clipped_text, institution_id, reporting_period, filename, user_prompt)
    
    if not raw_json:
        logger.error(f"Extraction returned empty payload for {filename}!")
        return None
        
    # Step 3: Validate and Save
    try:
        # Pydantic validation
        data_dict = json.loads(raw_json)
        payload = FSDataPayload.model_validate(data_dict)
        
        # Save interim result
        output_dir = "data/interim/extracted_metrics"
        os.makedirs(output_dir, exist_ok=True)
        # Using specific filename to keep track
        output_path = os.path.join(output_dir, f"{institution_id}_{reporting_period}_extracted.json")
        
        with open(output_path, "w") as f:
            json.dump(payload.model_dump(), f, indent=2)
            
        logger.success(f"Successfully wrote extracted JSON to {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Validation failed for {filename}: {e}")
        # Save the raw JSON for debugging anyway
        failed_path = os.path.join("data/interim/extracted_metrics", f"{institution_id}_{reporting_period}_FAILED.json")
        os.makedirs("data/interim/extracted_metrics", exist_ok=True)
        with open(failed_path, "w") as f:
            f.write(raw_json)
        return None
