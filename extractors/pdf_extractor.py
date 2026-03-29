import os
import json
import time
import requests
from typing import List, Dict, Any, Union
from pydantic import BaseModel, Field, model_validator, field_validator, AliasChoices
from loguru import logger
from dotenv import load_dotenv

from extractors.text_clipper import get_clipped_financial_text_dynamic

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

def extract_financials_from_text(clipped_text: str, institution_id: str, reporting_period: str, filename: str, user_prompt: str) -> str:
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
                        "item_name": "Total Assets", 
                        "data_points": [{"reporting_year": int(reporting_period), "amount": 1000000}]
                    }]
                }
            ]
        })

import google.generativeai as genai

def extract_financials_from_text(clipped_text: str, institution_id: str, reporting_period: str, filename: str, user_prompt: str) -> str:
    if DUMMY_MODE:
        return json.dumps({
            "institution_id": institution_id,
            "reporting_period": reporting_period,
            "source_document": filename,
            "financial_statements": [
                {
                    "statement_type": "Balance Sheet",
                    "line_items": [{
                        "item_name": "Total Assets", 
                        "data_points": [{"reporting_year": int(reporting_period), "amount": 1000000, "month_end": 12, "is_cumulative": True, "scaling_factor": 1000}]
                    }]
                }
            ]
        })

    prompt_text = (
        f"EXTRACT COMPLETE TABULAR FINANCIAL DATA for: '{user_prompt}'.\n"
        f"INSTITUTION: {institution_id}, PERIOD: {reporting_period}, FILE: {filename}\n\n"
        "### CRITICAL INSTRUCTIONS ###\n"
        "1. **Scaling**: Multiply raw numbers by the scale (e.g., if RM'000, multiply by 1000) and set 'scaling_factor'.\n"
        "2. **Temporal**: Set 'month_end' to the month number (e.g., 12) for the reporting date.\n"
        "3. **Cumulative**: Set 'is_cumulative' to True if it's year-to-date. Balance Sheet is ALWAYS True.\n"
        "4. **Format**: MANDATORY JSON structure below. Do NOT use different keys.\n\n"
        "### MANDATORY JSON TEMPLATE ###\n"
        "{\n"
        "  'financial_statements': [\n"
        "    {\n"
        "      'statement_type': 'Balance Sheet',\n"
        "      'month_end': 12,\n"
        "      'is_cumulative': true,\n"
        "      'scaling_factor': 1000,\n"
        "      'line_items': [\n"
        "        {\n"
        "          'item_name': 'Cash',\n"
        "          'data_points': [\n"
        "            {'reporting_year': 2024, 'amount': 1000000}\n"
        "          ]\n"
        "        }\n"
        "      ]\n"
        "    }\n"
        "  ]\n"
        "}\n"
    )

    # Use confirmed available models from list_models()
    model_names = ["gemini-2.0-flash", "gemini-flash-latest", "gemini-pro-latest"]
    
    for model_name in model_names:
        # Mandatory chill-down for Free Tier stability
        logger.info("Chilling for 65s to clear RPM/RPD quotas...")
        time.sleep(65)
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={API_KEY}"
        payload = {
            "contents": [{
                "parts": [{"text": prompt_text + "\n\n### DATA ###\n" + clipped_text}]
            }]
        }
        
        try:
            logger.info(f"Targeting REST v1beta: {model_name}")
            response = requests.post(url, json=payload, timeout=120)
            
            if response.status_code == 200:
                result = response.json()
                text = result['candidates'][0]['content']['parts'][0]['text']
                text = text.replace('```json', '').replace('```', '').strip()
                logger.success(f"Success with {model_name}")
                return text
            elif response.status_code == 429:
                logger.warning(f"Rate limit hit for {model_name}. Skipping to next.")
                continue
            else:
                logger.warning(f"Failed {model_name}: {response.status_code} - {response.text}")
                continue
        except Exception as e:
            logger.error(f"Request Error {model_name}: {e}")
            continue
            
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
