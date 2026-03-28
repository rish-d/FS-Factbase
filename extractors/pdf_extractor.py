import os
import json
import google.generativeai as genai
from pydantic import BaseModel, Field
from typing import List
from loguru import logger
from dotenv import load_dotenv

from extractors.text_clipper import get_clipped_financial_text

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
DUMMY_MODE = os.getenv("DUMMY_MODE", "false").lower() == "true"

if not API_KEY or API_KEY == "your_google_api_key_here":
    if not DUMMY_MODE:
        logger.error("GEMINI_API_KEY is missing from the .env file.")
        exit(1)

genai.configure(api_key=API_KEY)

from typing import List, Dict, Any, Union

class YearValue(BaseModel):
    year: int = Field(description="The reporting year, e.g., 2024", validation_alias="reporting_year")
    value: float | None = Field(description="The numerical value. Use null for headers or empty cells.", validation_alias="amount")
    confidence: float = Field(description="AI's confidence in this specific value (0.0 to 1.0)", default=1.0)
    notes: str | None = Field(description="Brief notes on any extraction difficulty.", default=None)

class LineItem(BaseModel):
    item: str = Field(description="The exact name of the line item as seen in the table.", validation_alias="item_name")
    group: str | None = Field(description="The category group if discernible, e.g., 'Assets'.", default=None)
    values: List[YearValue] = Field(description="List of values for available years.", validation_alias="data_points")

class Statement(BaseModel):
    statement_type: str = Field(description="e.g., 'Balance Sheet' or 'Income Statement'")
    statement_name: str | None = Field(description="The full name of the statement.", default=None)
    currency: str | None = Field(description="The currency unit, e.g., RM'000", default=None)
    line_items: List[LineItem] = Field(description="List of all extracted line items for this statement.")

class FSDataPayload(BaseModel):
    institution_id: str | None = Field(description="The canonical ID of the institution", default=None)
    reporting_period: str | None = Field(description="The primary reporting period, e.g., 2024", default=None)
    source_document: str | None = Field(description="The name of the PDF file", default=None)
    # Flexible: Accepts either a list of statements or a dictionary keyed by statement type
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

def extract_financials_from_text(clipped_text: str, institution_id: str, reporting_period: str, filename: str) -> str:
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
        "You are a master financial OCR AI. Your task is to extract Balance Sheet and Income Statement "
        "tables from the text. Return a SINGLE JSON object matching the schema exactly. "
        "IMPORTANT: Use the key 'financial_statements' for the list of extracted statements. "
        "Extract both the current and comparative years (e.g., 2024 and 2023) if present. "
        "Use the 'values' list to store year-specific numbers. Convert (X) to -X."
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

    logger.info("Prompting Gemini (gemini-2.5-flash) for validated structured extraction...")
    
    try:
        model = genai.GenerativeModel(
            model_name='gemini-2.5-flash',
            system_instruction=system_instruction
        )
        
        response = model.generate_content(
            prompt,
            generation_config={"response_mime_type": "application/json"}
        )
        
        if response and response.text:
            return response.text
        else:
            logger.error("Empty response or blocked content from Gemini.")
            return ""
            
    except Exception as e:
        logger.error(f"Legacy API Call failed: {e}")
        return ""

def process_report(pdf_path: str, institution_id: str, reporting_period: str):
    filename = os.path.basename(pdf_path)
    logger.info(f"--- Starting Extraction: {institution_id} ({reporting_period}) ---")
    
    logger.info(f"Step 1: Text-First Clipping for {pdf_path}")
    clipped_text = get_clipped_financial_text(pdf_path)
    
    if not clipped_text:
        logger.error(f"Failed to clip text for {pdf_path}. Skipping.")
        return None
        
    logger.info("Step 2: Sending pure text to LLM API")
    output_json = extract_financials_from_text(
        clipped_text=clipped_text,
        institution_id=institution_id,
        reporting_period=reporting_period,
        filename=filename
    )
    
    if output_json:
        try:
            validated_data = FSDataPayload.model_validate_json(output_json)
            # Create a unique interim filename
            safe_id = institution_id.replace(" ", "_").lower()
            output_path = f"data/interim/extracted_metrics/{safe_id}_{reporting_period}_extracted.json"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, "w") as f:
                json.dump(validated_data.model_dump(), f, indent=4)
                
            logger.success(f"Successfully wrote extracted JSON to {output_path}")
            return output_path
        except Exception as ve:
            logger.error(f"JSON Validation failed for {filename}: {ve}")
            fail_path = f"data/interim/extracted_metrics/{institution_id}_{reporting_period}_FAILED.json"
            with open(fail_path, "w") as f:
                f.write(output_json)
            return None
    else:
        logger.error(f"Extraction returned empty payload for {filename}!")
        return None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extract financials from a single PDF.")
    parser.add_argument("--pdf", required=True, help="Path to the PDF file")
    parser.add_argument("--id", required=True, help="Institution ID")
    parser.add_argument("--period", required=True, help="Reporting period, e.g. 2024")
    
    args = parser.parse_args()
    process_report(args.pdf, args.id, args.period)
