from mapper import StandardizedMapper, run_mapping
from expand_dictionary import DictionaryExpander
from loguru import logger
import sys
import os

# Ensure we can import modules in this directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def run_pipeline():
    """
    Orchestrates the full FS Factbase mapping loop:
    1. Standard Mapping (Ingest new extraction JSONs)
    2. Dictionary Expansion (Auto-match unmapped terms to IFRS Taxonomy)
    3. Re-queue Processing (Move newly resolved staging items to Factbase)
    """
    logger.info("====================================================")
    logger.info("STARTING FS FACTBASE ETL & RESOLUTION LOOP")
    logger.info("====================================================")
    
    # STEP 1: Process any new extraction files in data/interim
    logger.info("PHASE 1: Standard File Mapping")
    try:
        run_mapping()
    except Exception as e:
        logger.error(f"Error in Phase 1 (Standard Mapping): {e}")

    # STEP 2: Waterfall Reconciliation (Lexical -> Semantic -> LLM)
    logger.info("PHASE 2: Waterfall Reconciliation Engine")
    try:
        from reconciliation_engine import ReconciliationEngine
        engine = ReconciliationEngine()
        engine.reconcile_unmapped()
    except Exception as e:
        logger.error(f"Error in Phase 2 (Waterfall Reconciliation): {e}")

    logger.info("====================================================")
    logger.success("ETL & RESOLUTION LOOP COMPLETED")
    logger.info("====================================================")

if __name__ == "__main__":
    run_pipeline()
