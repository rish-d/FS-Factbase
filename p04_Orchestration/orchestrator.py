import os
import time
import sys
import json
from loguru import logger

# Ensure project root is in sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from p01_Data_Extraction.pdf_extractor import process_report, build_extraction_prompt, clean_json_output, FSDataPayload
from p01_Data_Extraction.text_clipper import get_clipped_financial_text_dynamic
from p02_Database_and_Mapping.mapper import StandardizedMapper
from p02_Database_and_Mapping.checkpoint_manager import CheckpointManager
from p04_Orchestration.status_manager import StatusManager

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

# CLAIMING LOGIC REMOVED TO SUPPORT FLUID MULTI-FOCUS EXTRACTION

def run_pipeline(user_prompt: str = "Balance Sheet and Income Statement", sample: bool = False, target_year: str = None, target_bank: str = None):
    reports = discover_reports()
    
    # Filter by Year and Bank if provided
    if target_year:
        reports = [r for r in reports if r[2] == target_year]
    if target_bank:
        reports = [r for r in reports if target_bank.lower() in r[1].lower()]
    
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
    checkpoint_manager = CheckpointManager()
    
    success_count = 0
    fail_count = 0
    results_log = []
    
    for pdf_path, inst_id, period in reports:
        # No 'claimed' check, execute fluid extraction on demand
            
        logger.info(f">>> Processing: {inst_id} | {period} <<<")
        
        try:
            # 1. Extract
            StatusManager.update_status(activity=f"Extracting: {inst_id} ({period})...")
            json_path = process_report(pdf_path, inst_id, period, user_prompt)
            
            if json_path and os.path.exists(json_path):
                # 2. Map
                StatusManager.update_status(activity=f"Mapping: {inst_id} ({period})...")
                mapper.process_file(json_path)
                
                # 3. Update Checkpoint for this specific task
                checkpoint_manager.set_checkpoint(inst_id, period, user_prompt, "COMPLETED")
                
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
    return success_count > 0 and fail_count == 0

def run_offline_prep(user_prompt: str = "Balance Sheet and Income Statement", sample: bool = False, target_year: str = None, target_bank: str = None):
    """
    Generates markdown prompt files for use with Gemini Web UI.
    """
    os.makedirs(os.path.join("data", "manual_runs", "prompts"), exist_ok=True)
    os.makedirs(os.path.join("data", "manual_runs", "responses"), exist_ok=True)
    
    reports = discover_reports()
    
    # Filter by Year and Bank if provided
    if target_year:
        reports = [r for r in reports if r[2] == target_year]
    if target_bank:
        reports = [r for r in reports if target_bank.lower() in r[1].lower()]
    
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
        
    logger.info(f"Discovered {len(reports)} reports for OFFLINE prep. Target: {user_prompt}")
    
    generated = 0
    for pdf_path, inst_id, period in reports:
        # No 'claimed' check, execute prep on demand
            
        logger.info(f"Prep: Generating prompt for {inst_id} {period}")
        filename = os.path.basename(pdf_path)
        
        clipped_text = get_clipped_financial_text_dynamic(pdf_path, user_prompt)
        if not clipped_text:
            logger.warning(f"No clipped text found for {pdf_path}")
            continue
            
        prompt = build_extraction_prompt(clipped_text, inst_id, period, filename, user_prompt, include_schema_text=True)
        
        prompt_path = os.path.join("data", "manual_runs", "prompts", f"{inst_id}_{period}_prompt.md")
        with open(prompt_path, "w", encoding="utf-8") as f:
            f.write(prompt)
        
        logger.success(f"Saved prompt: {prompt_path}")
        generated += 1
        
    logger.success(f"Offline Prep Complete. Generated {generated} prompts.")

def run_offline_ingest():
    """
    Scans the manual_runs/responses folder, validates the JSON, and ingests them via mapper.
    """
    responses_dir = os.path.join("data", "manual_runs", "responses")
    interim_dir = os.path.join("data", "interim", "extracted_metrics")
    
    os.makedirs(responses_dir, exist_ok=True)
    os.makedirs(interim_dir, exist_ok=True)
    
    mapper = StandardizedMapper()
    
    files = [f for f in os.listdir(responses_dir) if f.endswith((".md", ".txt", ".json")) and not f.endswith("_processed.md")]
    
    if not files:
        logger.info("No unparsed responses found in manual_runs/responses.")
        return
        
    for filename in files:
        filepath = os.path.join(responses_dir, filename)
        logger.info(f"Ingesting offline response: {filename}")
        
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                raw_text = f.read()
                
            clean_str = clean_json_output(raw_text)
            if not clean_str:
                logger.error(f"Failed to find JSON in {filename}")
                continue
                
            data_dict = json.loads(clean_str)
            payload = FSDataPayload.model_validate(data_dict)
            
            # Derive identifiers from the payload, not just filename
            inst_id = payload.institution_id or "UNKNOWN"
            period = payload.reporting_period or "0000"
            
            output_filename = f"{inst_id}_{period}_extracted.json"
            output_path = os.path.join(interim_dir, output_filename)
            
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(payload.model_dump(), f, indent=2)
                
            logger.success(f"Validated and saved: {output_path}")
            
            # Rename the response file so it's not processed again
            processed_filepath = os.path.join(responses_dir, f"{filename}.processed")
            os.rename(filepath, processed_filepath)
            
            # Map into DuckDB
            mapper.process_file(output_path)
            
        except Exception as e:
            logger.error(f"Validation Error processing {filename}: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Orchestrate the extraction pipeline.")
    parser.add_argument("--prompt", default=None, help="Target financial context to extract")
    parser.add_argument("--year", default=None, help="Target reporting year")
    parser.add_argument("--bank", default=None, help="Target institution keyword")
    parser.add_argument("--sample", action="store_true", help="Run only 2 reports per bank for validation")
    parser.add_argument("--offline-prep", action="store_true", help="Generate prompts for Web UI rather than calling API")
    parser.add_argument("--offline-ingest", action="store_true", help="Ingest parsed responses from Gemini Web UI")
    args = parser.parse_args()
    
    if args.offline_ingest:
        run_offline_ingest()
    else:
        # Interactive Mode if no prompt is explicitly provided
        user_prompt = args.prompt
        target_year = args.year
        target_bank = args.bank
        
        if not user_prompt:
            print("\n🤖 FS-Factbase Interactive CLI 🤖")
            print("==================================")
            u_input = input("Enter extraction focus/target (e.g. 'Customer deposits') [Default: Balance Sheet and Income Statement]: ").strip()
            user_prompt = u_input if u_input else "Balance Sheet and Income Statement"
            
            y_input = input("Enter target year (or press Enter for ALL): ").strip()
            target_year = y_input if y_input else None
            
            b_input = input("Enter target bank keyword (or press Enter for ALL): ").strip()
            target_bank = b_input if b_input else None
        
        if args.offline_prep:
            run_offline_prep(user_prompt, args.sample, target_year, target_bank)
        else:
            run_pipeline(user_prompt, args.sample, target_year, target_bank)
