import os
import time
import sys
from loguru import logger

# Ensure project root is in sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from p01_Data_Extraction.pdf_extractor import process_report
from p02_Database_and_Mapping.mapper import StandardizedMapper

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

def run_pipeline(user_prompt: str = "Balance Sheet and Income Statement", sample: bool = False):
    reports = discover_reports()
    
    if sample:
        logger.info("Sample mode ACTIVE: Limiting to 2 reports per institution.")
        sampled_reports = []
        inst_counts = {}
        for r in reports:
            path, inst_id, period = r
            count = inst_counts.get(inst_id, 0)
            if count < 2:
                sampled_reports.append(r)
                inst_counts[inst_id] = count + 1
        reports = sampled_reports

    logger.info(f"Discovered {len(reports)} reports for processing. Target: {user_prompt}")
    
    mapper = StandardizedMapper()
    
    success_count = 0
    fail_count = 0
    results_log = []
    
    for pdf_path, inst_id, period in reports:
        logger.info(f">>> Processing: {inst_id} | {period} <<<")
        
        try:
            # 1. Extract
            json_path = process_report(pdf_path, inst_id, period, user_prompt)
            
            if json_path and os.path.exists(json_path):
                # 2. Map
                mapper.process_file(json_path)
                success_count += 1
                results_log.append((inst_id, period, "SUCCESS"))
            else:
                logger.error(f"Extraction failed for {pdf_path}")
                fail_count += 1
                results_log.append((inst_id, period, "FAILED"))
                
            # Inter-request delay to respect API quotas (Free Tier)
            if success_count + fail_count < len(reports):
                logger.info("Sleeping for 10s to respect API rate limits...")
                time.sleep(10)
            
        except Exception as e:
            logger.error(f"Pipeline failed for {pdf_path}: {e}")
            fail_count += 1
            results_log.append((inst_id, period, "CRASHED"))

    print("\n" + "="*40)
    print("📋 EXTRACTION SUMMARY REPORT")
    print("="*40)
    print(f"{'Institution':<30} | {'Period':<8} | {'Status'}")
    print("-" * 50)
    for inst, per, status in results_log:
        print(f"{inst[:30]:<30} | {per:<8} | {status}")
    print("="*50)
    logger.success(f"Orchestration Complete. Success: {success_count}, Fail: {fail_count}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Orchestrate the extraction pipeline.")
    parser.add_argument("--prompt", default="Balance Sheet and Income Statement", help="Target financial context to extract")
    parser.add_argument("--sample", action="store_true", help="Run only 2 reports per bank for validation")
    args = parser.parse_args()
    
    run_pipeline(args.prompt, args.sample)
