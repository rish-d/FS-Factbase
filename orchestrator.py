import os
import time
from loguru import logger
from extractors.pdf_extractor import process_report
from transformers.mapper import StandardizedMapper

def discover_reports(base_path="data/raw/reports"):
    """
    Finds all PDFs and yields (path, institution_id, period)
    Expected structure: data/raw/reports/{institution_id}/{year}_fs.pdf
    """
    reports = []
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".pdf"):
                path = os.path.join(root, file)
                # institutional_id is the parent folder
                inst_id = os.path.basename(root)
                # period is the first 4 chars of the filename
                period = file[:4] 
                reports.append((path, inst_id, period))
    return reports

def run_pipeline(user_prompt: str = "Balance Sheet and Income Statement"):
    reports = discover_reports()
    logger.info(f"Discovered {len(reports)} reports for processing. Target: {user_prompt}")
    
    mapper = StandardizedMapper()
    
    success_count = 0
    fail_count = 0
    
    for pdf_path, inst_id, period in reports:
        logger.info(f">>> Processing: {inst_id} | {period} <<<")
        
        try:
            # 1. Extract
            json_path = process_report(pdf_path, inst_id, period, user_prompt)
            
            if json_path and os.path.exists(json_path):
                # 2. Map
                mapper.process_file(json_path)
                success_count += 1
            else:
                logger.error(f"Extraction failed for {pdf_path}")
                fail_count += 1
                
            # Inter-request delay to respect API quotas (Free Tier)
            logger.info("Sleeping for 10s to respect API rate limits...")
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"Pipeline failed for {pdf_path}: {e}")
            fail_count += 1

    logger.success(f"Orchestration Complete. Success: {success_count}, Fail: {fail_count}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Orchestrate the extraction pipeline.")
    parser.add_argument("--prompt", default="Balance Sheet and Income Statement", help="Target financial context to extract")
    args = parser.parse_args()
    
    run_pipeline(args.prompt)
