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

from p01_Data_Extraction.llm_factory import LLMFactory, MANUAL_SCHEMA, build_extraction_prompt, clean_json_output

def extract_financials_from_text(clipped_text: str, institution_id: str, reporting_period: str, filename: str, user_prompt: str) -> str:
    """
    Uses the LLMFactory to extract financial data with fallback logic.
    """
    if DUMMY_MODE:
        logger.warning("Running in DUMMY_MODE. Returning simulated JSON.")
        return json.dumps({
            "institution_id": institution_id,
            "reporting_period": reporting_period,
            "source_document": filename,
            "statements": [
                {
                    "statement_type": "Balance Sheet",
                    "items": [{
                        "item": "Total Assets", 
                        "values": [{"year": int(reporting_period), "value": 1000000, "month_end": 12, "is_cumulative": True, "scaling_factor": 1000, "confidence": 0.9}]
                    }]
                }
            ]
        })

    prompt_text = build_extraction_prompt(clipped_text, institution_id, reporting_period, filename, user_prompt, include_schema_text=False)

    # Load provider and model from environment
    provider = os.getenv("LLM_PROVIDER", "ollama")
    model = os.getenv("OLLAMA_MODEL", "llama3.2:latest")

    raw_json = LLMFactory.extract_with_fallback(
        prompt=prompt_text,
        schema=MANUAL_SCHEMA,
        initial_provider=provider,
        initial_model=model
    )

    return clean_json_output(raw_json) if raw_json else ""

def process_report(pdf_path: str, institution_id: str, reporting_period: str, user_prompt: str = "Balance Sheet and Income Statement"):
    filename = os.path.basename(pdf_path)
    logger.info(f"--- Processing: {institution_id} ({reporting_period}) ---")
    
    # Hardware-aware chunking: Scale down pages for heavy local models
    active_model = os.getenv("OLLAMA_MODEL", "llama3.2:latest")
    max_pages = 2 # Default for Llama 3.2 or Cloud
    
    if "qwen" in active_model.lower() and "7b" in active_model.lower():
        logger.warning(f"Heavy model detected ({active_model}). Restricting to 1-page payload to save VRAM.")
        max_pages = 1
    elif "llama" in active_model.lower() and "3b" in active_model.lower():
        max_pages = 3 # 3B has more room for context
        
    clipped_text = get_clipped_financial_text_dynamic(pdf_path, user_prompt, max_pages=max_pages)
    if not clipped_text: return None
        
    raw_json = extract_financials_from_text(clipped_text, institution_id, reporting_period, filename, user_prompt)
    if not raw_json: return None
        
    output_dir = "data/interim/extracted_metrics"
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        data_dict = json.loads(raw_json)
        payload = FSDataPayload.model_validate(data_dict)
        
        timestamp = int(time.time())
        output_path = os.path.join(output_dir, f"{institution_id}_{reporting_period}_extracted_{timestamp}.json")
        with open(output_path, "w") as f:
            json.dump(payload.model_dump(), f, indent=2)
            
        logger.success(f"Saved: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Validation Error: {e}")
        # Save raw failing JSON for analysis
        timestamp = int(time.time())
        fail_path = os.path.join(output_dir, f"{institution_id}_{reporting_period}_FAILED_{timestamp}.json")
        with open(fail_path, "w") as f:
            f.write(raw_json)
        return None
