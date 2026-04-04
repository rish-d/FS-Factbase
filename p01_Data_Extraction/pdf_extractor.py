import os
import json
import time
import requests
import re
from typing import List, Dict, Any, Union, Optional, Annotated
from pydantic import BaseModel, Field, model_validator, field_validator, AliasChoices
from loguru import logger
from dotenv import load_dotenv

from p00_Shared_Utils.io_utils import get_root_dir, save_json
from p01_Data_Extraction.text_clipper import get_clipped_financial_text_dynamic

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
DUMMY_MODE = os.getenv("DUMMY_MODE", "false").lower() == "true"

from pydantic import BaseModel, Field, model_validator

class YearValue(BaseModel):
    year: int = Field(description="The reporting year, e.g., 2024", validation_alias=AliasChoices("reporting_year", "year"))
    value: Optional[float] = Field(None, description="The numerical value.", validation_alias=AliasChoices("amount", "value"))
    month_end: int = Field(description="The month number of the reporting date (1-12).")
    is_cumulative: bool = Field(description="True if the value is cumulative (e.g. Full Year, Balance Sheet), False if incremental (e.g. 3 months only).")
    scaling_factor: int = Field(description="The multiplier found in headers (1, 1000, 1000000).")
    confidence: float = Field(description="AI's confidence in this specific value (0.0 to 1.0)")
    source_page_number: Optional[int] = Field(None, description="The specific page number where this value was found.", validation_alias=AliasChoices("page_number", "source_page_number"))
    entity_scope: str = Field(default="Group", description="Scope of the figures: 'Group', 'Bank', or 'Company'. Defaults to 'Group'")
    notes: Optional[str] = Field(None, description="Brief notes on any extraction difficulty.")

class LineItem(BaseModel):
    item: str = Field(description="The name of the header/account.", validation_alias=AliasChoices("item_name", "line_item", "item", "name"))
    group: Optional[str] = Field(None, description="Group/Category")
    values: List[YearValue] = Field(description="List of values.", validation_alias=AliasChoices("data_points", "data", "values", "items"))

    @model_validator(mode='before')
    @classmethod
    def handle_dynamic_keys(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
            
        # If 'data_points' or 'values' is already there, don't interfere
        if 'data_points' in data or 'values' in data or 'line_items' in data:
             return data
             
        # Look for potential year columns (e.g., "2024", "GROUP_2024", "BANK_2025")
        extracted_values = []
        for key, val in data.items():
            if key == "item" or key == "item_name" or key == "group":
                continue
            
            # Extract year from key if possible
            match = re.search(r"(GROUP|BANK|COMPANY)?.*?(\d{4})", key.upper())
            if match and (isinstance(val, (int, float)) or val is None):
                scope_prefix = match.group(1)
                year = int(match.group(2))
                entity_scope = scope_prefix.capitalize() if scope_prefix else "Group"
                extracted_values.append({"reporting_year": year, "amount": val, "entity_scope": entity_scope})
        
        if extracted_values:
            data["data_points"] = extracted_values
            
        # Handle flat structure: {"line_item": "...", "value": 123}
        if "value" in data and not data.get("data_points"):
             data["data_points"] = [{
                 "amount": data["value"],
                 "reporting_year": data.get("year", data.get("period", 0))
             }]
            
        return data

class Statement(BaseModel):
    statement_type: str = Field(description="e.g., 'Balance Sheet' or 'Income Statement'", validation_alias=AliasChoices("statement_type", "report_type", "type", "table_name"))
    statement_name: Optional[str] = Field(None, description="Specific name from document.")
    items: List[LineItem] = Field(description="List of extracted row data.", validation_alias=AliasChoices("line_items", "data", "items"))

    @model_validator(mode='before')
    @classmethod
    def merge_item_lists(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
            
        items = data.get('items', data.get('line_items', []))
        if not isinstance(items, list): items = []
        
        # Merge categorized lists if they exist (common in Gemini outputs)
        for key in ['assets', 'liabilities', 'equity', 'income', 'expenses', 'other']:
            if key in data and isinstance(data[key], list):
                for entry in data[key]:
                    if isinstance(entry, dict) and 'group' not in entry:
                        entry['group'] = key.capitalize()
                items.extend(data[key])
        
        data['line_items'] = items
        
        # Propagate temporal/scaling metadata to items if they are at statement level
        period = data.get("period", data.get("year", data.get("reporting_period")))
        month_end = data.get("month_end")
        is_cumulative = data.get("is_cumulative")
        scaling_factor = data.get("scaling_factor")
        source_page_number = data.get("source_page_number")
        
        for item in items:
            if not isinstance(item, dict): continue
            dps = item.get("data_points", item.get("values", item.get("data", [])))
            for dp in dps:
                if not isinstance(dp, dict): continue
                if "reporting_year" not in dp and "year" not in dp: dp["reporting_year"] = period
                if "month_end" not in dp: dp["month_end"] = month_end
                if "is_cumulative" not in dp: dp["is_cumulative"] = is_cumulative
                if "scaling_factor" not in dp: dp["scaling_factor"] = scaling_factor
                if "source_page_number" not in dp and "page_number" not in dp: dp["source_page_number"] = source_page_number
                if "entity_scope" not in dp: dp["entity_scope"] = "Group"
        
        return data

class FSDataPayload(BaseModel):
    institution_id: Optional[str] = Field(None, description="The canonical ID of the institution")
    reporting_period: Optional[str] = Field(None, description="The primary reporting period, e.g., 2024")
    source_document: Optional[str] = Field(None, description="The name of the PDF file")
    source_page_number: Optional[int] = Field(None, description="Default page number for all statements if not specified per item.")
    statements: List[Statement] = Field(
        description="The extracted financial statements.",
        validation_alias=AliasChoices("financial_statements", "statements")
    )

import google.generativeai as genai
from google.api_core import exceptions as g_exceptions

# Load Gemini API Key
genai.configure(api_key=API_KEY)

def clean_json_output(raw_text: str) -> str:
    """
    Removes markdown code blocks (```json ... ```) from Gemini's response 
    to ensure it's a valid JSON string.
    """
    if not raw_text:
        return ""
    
    # Remove markdown backticks if present
    cleaned = re.sub(r"```json\s*", "", raw_text)
    cleaned = re.sub(r"```\s*", "", cleaned)
    return cleaned.strip()

MANUAL_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "institution_id": {"type": "STRING"},
        "reporting_period": {"type": "STRING"},
        "source_document": {"type": "STRING"},
        "source_page_number": {"type": "INTEGER"},
        "statements": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "statement_type": {"type": "STRING"},
                    "statement_name": {"type": "STRING"},
                    "items": {
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "item": {"type": "STRING"},
                                "group": {"type": "STRING"},
                                "values": {
                                    "type": "ARRAY",
                                    "items": {
                                        "type": "OBJECT",
                                        "properties": {
                                            "reporting_year": {"type": "INTEGER"},
                                            "amount": {"type": "NUMBER"},
                                            "month_end": {"type": "INTEGER"},
                                            "is_cumulative": {"type": "BOOLEAN"},
                                            "scaling_factor": {"type": "INTEGER"},
                                            "confidence": {"type": "NUMBER"},
                                            "page_number": {"type": "INTEGER"},
                                            "entity_scope": {"type": "STRING"},
                                            "notes": {"type": "STRING"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

def build_extraction_prompt(clipped_text: str, institution_id: str, reporting_period: str, filename: str, user_prompt: str, include_schema_text: bool = False) -> str:
    prompt_text = (
        "TASK: Extract standardized financial data from the provided text snippet.\n"
        f"INSTITUTION: {institution_id}\n"
        f"PRIMARY REPORTING PERIOD: {reporting_period}\n"
        f"SOURCE FILE: {filename}\n"
        f"USER TARGET: {user_prompt}\n\n"
        "### EXTRACTION RULES ###\n"
        "1. RAW SCALING: Extract the RAWEST numerical value exactly as it appears in the table. Identify multipliers from headers (RM'000, Millions) and store as 'scaling_factor' metadata. DO NOT multiply the value yourself.\n"
        "2. TEMPORAL & ENTITY: Detect 'month_end' (1-12) and 'is_cumulative' (True for annual/balance sheet). If the table presents multiple entity columns (e.g., 'Group' vs 'Bank'/'Company'), extract figures for BOTH and tag them appropriately in the 'entity_scope' field. If only one set of figures is present, assume it is 'Group'.\n"
        "3. PRECISION: Extract ONLY from the provided text. Do not hallucinate data points.\n"
        "4. TRACEABILITY: Locate the '--- TARGET FINANCIAL PAGE {page_num} ---' markers in the text. For EACH extracted data point (YearValue), you MUST set 'source_page_number' to the {page_num} specified in the marker immediately preceding that section of text.\n"
        "5. STRUCTURE: Comply exactly with the target JSON schema. Keep 'statement_type' and 'statement_name' SHORT AND CONCISE (e.g., 'Balance Sheet'). DO NOT include long descriptions in these fields.\n\n"
    )

    if include_schema_text:
        prompt_text += (
            "6. STRICT NO-FLUFF JSON ONLY: Return ONLY the raw JSON string matching the schema below. Do not provide ANY conversational text before or after (e.g. no 'Here is the extracted JSON'). Do not use markdown backticks around the JSON.\n\n"
            "### TARGET JSON SCHEMA ###\n"
            f"{json.dumps(MANUAL_SCHEMA, indent=2)}\n\n"
        )
        
    prompt_text += (
        "### TEXT SNIPPET ###\n"
        f"{clipped_text}"
    )

    return prompt_text

def extract_financials_from_text(clipped_text: str, institution_id: str, reporting_period: str, filename: str, user_prompt: str) -> str:
    """
    Uses the Gemini SDK with native JSON Schema enforcement to extract financial data.
    Implements exponential backoff for 429 RESOURCE_EXHAUSTED errors and model fallback.
    """
    if DUMMY_MODE:
        logger.warning("Running in DUMMY_MODE. Returning simulated JSON.")
        return json.dumps({
            "institution_id": institution_id,
            "reporting_period": reporting_period,
            "source_document": filename,
            "financial_statements": [
                {
                    "statement_type": "Balance Sheet",
                    "line_items": [{
                        "item": "Total Assets", 
                        "values": [{"year": int(reporting_period), "value": 1000000, "month_end": 12, "is_cumulative": True, "scaling_factor": 1000}]
                    }]
                }
            ]
        })

    prompt_text = build_extraction_prompt(clipped_text, institution_id, reporting_period, filename, user_prompt, include_schema_text=False)

    # Model tiered hierarchy for resilience
    trial_configs = [
        {"name": "gemini-2.0-flash", "retries": 3, "use_schema": True},
        {"name": "gemini-2.0-flash-lite", "retries": 2, "use_schema": True},
        {"name": "gemini-1.5-flash", "retries": 2, "use_schema": True}
    ]

    for config in trial_configs:
        model_name = config["name"]
        max_retries = config["retries"]
        use_schema = config["use_schema"]
        
        # Prepare the prompt for the specific model
        current_prompt = prompt_text
        if not use_schema:
            current_prompt += "\n\nCRITICAL: Return valid JSON following the schema requirements for FSDataPayload."

        for attempt in range(max_retries):
            try:
                model = genai.GenerativeModel(model_name=model_name)
                logger.info(f"Targeting model: {model_name} (Attempt {attempt + 1}/{max_retries})")
                
                generation_config = {
                    "response_mime_type": "application/json",
                }
                
                # GenAI-native schema dict (Proto compatible)
                manual_schema = MANUAL_SCHEMA
    
                if use_schema:
                    generation_config["response_schema"] = manual_schema
                    logger.debug(f"Using manual schema for {model_name}")

                response = model.generate_content(
                    current_prompt,
                    generation_config=generation_config,
                    request_options={"timeout": 120}
                )
                
                if response.text:
                    logger.success(f"Successfully extracted data using {model_name}")
                    return clean_json_output(response.text)
                else:
                    logger.error(f"Empty response from {model_name}")
                    
            except g_exceptions.ResourceExhausted:
                wait_time = (2 ** attempt) * 10 # 10, 20, 40s
                logger.warning(f"429 RESOURCE_EXHAUSTED on {model_name}. Backing off for {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                logger.error(f"Unexpected error with {model_name}: {e}")
                # For non-retriable errors, we break retry loop and try next model or fail
                break 

    logger.critical("All models and retries exhausted. Extraction failed.")
    return ""

def process_report(pdf_path: str, institution_id: str, reporting_period: str, user_prompt: str = "Balance Sheet and Income Statement"):
    filename = os.path.basename(pdf_path)
    logger.info(f"--- Processing: {institution_id} ({reporting_period}) ---")
    
    clipped_text = get_clipped_financial_text_dynamic(pdf_path, user_prompt)
    if not clipped_text: return None
        
    raw_json = extract_financials_from_text(clipped_text, institution_id, reporting_period, filename, user_prompt)
    if not raw_json: return None
        
    output_dir = "data/interim/extracted_metrics"
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        data_dict = json.loads(raw_json)
        payload = FSDataPayload.model_validate(data_dict)
        
        output_path = os.path.join(output_dir, f"{institution_id}_{reporting_period}_extracted.json")
        with open(output_path, "w") as f:
            json.dump(payload.model_dump(), f, indent=2)
            
        logger.success(f"Saved: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Validation Error: {e}")
        # Save raw failing JSON for analysis
        fail_path = os.path.join(output_dir, f"{institution_id}_{reporting_period}_FAILED.json")
        with open(fail_path, "w") as f:
            f.write(raw_json)
        return None
