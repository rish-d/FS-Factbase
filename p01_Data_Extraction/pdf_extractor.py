import os
import json
import time
import requests
from typing import List, Dict, Any, Union
from pydantic import BaseModel, Field, model_validator, field_validator, AliasChoices
from loguru import logger
from dotenv import load_dotenv

from p01_Data_Extraction.text_clipper import get_clipped_financial_text_dynamic

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
DUMMY_MODE = os.getenv("DUMMY_MODE", "false").lower() == "true"

from pydantic import BaseModel, Field, model_validator

class YearValue(BaseModel):
    year: int = Field(description="The reporting year, e.g., 2024", validation_alias="reporting_year")
    value: float | None = Field(description="The numerical value.", validation_alias="amount")
    month_end: int = Field(description="The month number of the reporting date (1-12).", default=12)
    is_cumulative: bool = Field(description="True if the value is cumulative (e.g. Full Year, Balance Sheet), False if incremental (e.g. 3 months only).", default=True)
    scaling_factor: int = Field(description="The multiplier found in headers (1, 1000, 1000000).", default=1)
    confidence: float = Field(description="AI's confidence in this specific value (0.0 to 1.0)", default=1.0)
    notes: str | None = Field(description="Brief notes on any extraction difficulty.", default=None)

class LineItem(BaseModel):
    item: str = Field(description="The name of the header/account.", validation_alias=AliasChoices("item_name", "line_item", "item", "name"))
    group: str | None = Field(description="Group/Category", default=None)
    values: List[YearValue] = Field(description="List of values.", validation_alias=AliasChoices("data_points", "data", "values", "items"), default_factory=list)

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
        import re
        for key, val in data.items():
            if key == "item" or key == "item_name" or key == "group":
                continue
            
            # Extract year from key if possible
            match = re.search(r"(\d{4})", key)
            if match and (isinstance(val, (int, float)) or val is None):
                year = int(match.group(1))
                extracted_values.append({"reporting_year": year, "amount": val})
        
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
    statement_name: str | None = Field(description="Specific name from document.", default=None)
    items: List[LineItem] = Field(description="List of extracted row data.", validation_alias=AliasChoices("line_items", "data", "items"), default_factory=list)

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
        
        for item in items:
            if not isinstance(item, dict): continue
            dps = item.get("data_points", item.get("values", item.get("data", [])))
            for dp in dps:
                if not isinstance(dp, dict): continue
                if "reporting_year" not in dp and "year" not in dp: dp["reporting_year"] = period
                if "month_end" not in dp: dp["month_end"] = month_end
                if "is_cumulative" not in dp: dp["is_cumulative"] = is_cumulative
                if "scaling_factor" not in dp: dp["scaling_factor"] = scaling_factor
        
        return data

class FSDataPayload(BaseModel):
    institution_id: str | None = Field(description="The canonical ID of the institution", default=None)
    reporting_period: str | None = Field(description="The primary reporting period, e.g., 2024", default=None)
    source_document: str | None = Field(description="The name of the PDF file", default=None)
    statements: Union[List[Statement], Dict[str, Statement]] = Field(
        description="The extracted financial statements.",
        validation_alias="financial_statements"
    )

import google.generativeai as genai

# Load Gemini API Key
genai.configure(api_key=API_KEY)

def extract_financials_from_text(clipped_text: str, institution_id: str, reporting_period: str, filename: str, user_prompt: str) -> str:
    """
    Uses the Gemini SDK with native JSON Schema enforcement to extract financial data.
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

    prompt_text = (
        "TASK: Extract standardized financial data from the provided text snippet.\n"
        f"INSTITUTION: {institution_id}\n"
        f"PRIMARY REPORTING PERIOD: {reporting_period}\n"
        f"SOURCE FILE: {filename}\n"
        f"USER TARGET: {user_prompt}\n\n"
        "### EXTRACTION RULES ###\n"
        "1. RAW SCALING: Extract the RAWEST numerical value exactly as it appears in the table. Identify multipliers from headers (RM'000, Millions) and store as 'scaling_factor' metadata. DO NOT multiply the value yourself.\n"
        "2. TEMPORAL: Detect 'month_end' (1-12) and 'is_cumulative' (True for annual/balance sheet).\n"
        "3. PRECISION: Extract ONLY from the provided text. Do not hallucinate data points.\n"
        "4. STRUCTURE: Comply exactly with the target JSON schema.\n\n"
        "### TEXT SNIPPET ###\n"
        f"{clipped_text}"
    )

    # Use Gemini 1.5 Flash for performance/cost balance, or 1.5 Pro for complex layouts
    model_name = "gemini-1.5-flash"
    
    try:
        model = genai.GenerativeModel(model_name=model_name)
        logger.info(f"Targeting SDK: {model_name} with Structured Output")
        
        # Enforce structured output via Pydantic model
        response = model.generate_content(
            prompt_text,
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": FSDataPayload
            },
            request_options={"timeout": 120}
        )
        
        if response.text:
            logger.success(f"Successfully extracted data using {model_name}")
            return response.text
        else:
            logger.error(f"Empty response from {model_name}")
            return ""
            
    except Exception as e:
        logger.error(f"Gemini SDK Error: {e}")
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
