import os
import json
import time
from google import genai
from google.genai import types
import google.generativeai as old_genai
from dotenv import load_dotenv
from loguru import logger

load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

def test_new_sdk(model_name):
    logger.info(f"Testing NEW SDK (google-genai) with model: {model_name}")
    try:
        client = genai.Client(api_key=API_KEY)
        response = client.models.generate_content(
            model=model_name,
            contents="Say 'OK'",
        )
        logger.success(f"NEW SDK + {model_name}: Success! Response: {response.text.strip()}")
        return True
    except Exception as e:
        logger.error(f"NEW SDK + {model_name}: Failed. Error: {e}")
        return False

def test_old_sdk(model_name):
    logger.info(f"Testing OLD SDK (google-generativeai) with model: {model_name}")
    try:
        old_genai.configure(api_key=API_KEY)
        model = old_genai.GenerativeModel(model_name)
        response = model.generate_content("Say 'OK'")
        logger.success(f"OLD SDK + {model_name}: Success! Response: {response.text.strip()}")
        return True
    except Exception as e:
        logger.error(f"OLD SDK + {model_name}: Failed. Error: {e}")
        return False

if __name__ == "__main__":
    models_to_test = [
        "gemini-2.0-flash",
        "gemini-1.5-flash",
        "gemini-1.5-flash-8b",
        "gemini-1.5-pro",
        "models/gemini-1.5-flash",
        "models/gemini-1.5-flash-8b",
    ]
    
    results = {}
    
    for model in models_to_test:
        results[f"new_{model}"] = test_new_sdk(model)
        time.sleep(1) # Avoid spamming
        results[f"old_{model}"] = test_old_sdk(model)
        time.sleep(1)

    logger.info("--- DIAGNOSTIC RESULTS SUMMARY ---")
    for k, v in results.items():
        status = "PASSED" if v else "FAILED"
        logger.info(f"{k}: {status}")
