import os
import json
import time
import requests
from typing import Optional, Dict, Any, List
from loguru import logger
import google.generativeai as genai
from google.api_core import exceptions as g_exceptions
from dotenv import load_dotenv

load_dotenv()

import re

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
        "2. TEMPORAL & ENTITY: Detect 'month_end' (1-12) and 'is_cumulative' (True for annual/balance sheet). If month_end is not clearly stated, you may leave it as null. If the table presents multiple entity columns (e.g., 'Group' vs 'Bank'/'Company'), extract figures for BOTH and tag them appropriately in the 'entity_scope' field. If only one set of figures is present, assume it is 'Group'.\n"
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

class LLMProvider:
    def extract(self, prompt: str, schema: Dict[str, Any]) -> Optional[str]:
        raise NotImplementedError

class GeminiProvider(LLMProvider):
    def __init__(self, model_name: str = "gemini-2.0-flash"):
        self.model_name = model_name
        api_key = os.getenv("GEMINI_API_KEY")
        if api_key:
            genai.configure(api_key=api_key)
        else:
            logger.error("GEMINI_API_KEY not found in environment.")

    def extract(self, prompt: str, schema: Dict[str, Any]) -> Optional[str]:
        try:
            model = genai.GenerativeModel(model_name=self.model_name)
            generation_config = {
                "response_mime_type": "application/json",
                "response_schema": schema
            }
            response = model.generate_content(
                prompt,
                generation_config=generation_config,
                request_options={"timeout": 120}
            )
            if response.text:
                return response.text
        except g_exceptions.ResourceExhausted:
            logger.warning(f"Gemini 429 Resource Exhausted for {self.model_name}")
            return None
        except Exception as e:
            logger.error(f"Gemini error with {self.model_name}: {e}")
            return None
        return None

class OllamaProvider(LLMProvider):
    def __init__(self, model_name: str, base_url: str = "http://localhost:11434"):
        self.model_name = model_name
        self.base_url = base_url

    def extract(self, prompt: str, schema: Dict[str, Any]) -> Optional[str]:
        url = f"{self.base_url}/api/generate"
        
        # Cleaner prompt for Local Models to prevent "schema-bleeding"
        full_prompt = (
            f"SYSTEM: You are a financial data extractor. You return ONLY raw JSON matching a specific schema.\n"
            f"SCHEMA:\n{json.dumps(schema, indent=2)}\n\n"
            f"USER: {prompt}\n\n"
            f"INSTRUCTION: Based on the snippet above, populate the JSON schema. "
            f"Do not include the schema definition (like 'type', 'properties') in your response. "
            f"Return ONLY the populated JSON object. No conversational filler."
        )
        
        payload = {
            "model": self.model_name,
            "prompt": full_prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.0,
                "num_ctx": 4096,      # Optimized context for 6GB VRAM
                "num_predict": 2048   # Large enough for full financial output
            }
        }
        
        try:
            logger.info(f"Requesting Ollama: {self.model_name}...")
            response = requests.post(url, json=payload, timeout=300)
            response.raise_for_status()
            result = response.json()
            raw_response = result.get("response", "")
            
            # Post-processing to strip "schema-bleeding" from local models
            try:
                data = json.loads(raw_response)
                # If model returned {"type": "OBJECT", "properties": {...}, ...}
                if isinstance(data, dict) and "properties" in data and "type" in data:
                    logger.warning("Detected schema-bleeding in Ollama output. Flattening...")
                    # Merge properties with any top-level keys the model actually filled
                    flattened = data["properties"]
                    for k, v in data.items():
                        if k not in ["type", "properties", "required", "title"]:
                            flattened[k] = v
                    return json.dumps(flattened)
            except:
                pass

            return raw_response
        except Exception as e:
            logger.error(f"Ollama error with {self.model_name}: {e}")
            return None

class LLMFactory:
    @staticmethod
    def get_provider(provider_type: str, model_name: Optional[str] = None) -> LLMProvider:
        if provider_type.lower() == "gemini":
            return GeminiProvider(model_name or "gemini-2.0-flash")
        elif provider_type.lower() == "ollama":
            return OllamaProvider(model_name or "llama3.2:latest")
        else:
            raise ValueError(f"Unknown provider type: {provider_type}")

    @staticmethod
    def extract_with_fallback(prompt: str, schema: Dict[str, Any], initial_provider: str = "ollama", initial_model: str = "llama3.2:latest") -> Optional[str]:
        """
        Attempts extraction with the initial provider. 
        If it fails, it falls back to Gemini.
        """
        # 1. Primary Attempt
        provider = LLMFactory.get_provider(initial_provider, initial_model)
        logger.info(f"Attempting extraction via {initial_provider} ({initial_model})...")
        result = provider.extract(prompt, schema)
        
        if result:
            return result
            
        # 2. Fallback to Gemini if Primary fails
        if initial_provider != "gemini":
            logger.warning(f"Primary provider {initial_provider} failed. Falling back to Gemini Tier...")
            fallback_models = ["gemini-2.0-flash", "gemini-1.5-flash"]
            for model in fallback_models:
                logger.info(f"Attempting fallback via Gemini ({model})...")
                gemini = GeminiProvider(model)
                result = gemini.extract(prompt, schema)
                if result:
                    return result
        
        logger.error("All LLM providers failed.")
        return None
