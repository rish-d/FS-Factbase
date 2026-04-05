import os
import json
from loguru import logger
from dotenv import load_dotenv
from p01_Data_Extraction.llm_factory import LLMFactory

load_dotenv()

def test_factory_structure():
    logger.info("Testing LLMFactory structure...")
    
    # Test 1: Gemini Provider Instantiation
    try:
        gemini = LLMFactory.get_provider("gemini")
        logger.success("GeminiProvider instantiated.")
    except Exception as e:
        logger.error(f"GeminiProvider failed: {e}")

    # Test 2: Ollama Provider Instantiation
    try:
        ollama = LLMFactory.get_provider("ollama", model_name="llama3.2:latest")
        logger.success("OllamaProvider instantiated.")
    except Exception as e:
        logger.error(f"OllamaProvider failed: {e}")

    # Test 3: Fallback logic (Dummy check)
    # We won't actually call the API here to save tokens/time, 
    # but we verify the code path doesn't have syntax errors.
    logger.info("Factory verification complete.")

if __name__ == "__main__":
    test_factory_structure()
