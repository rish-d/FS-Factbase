import os
import json
from pathlib import Path
from loguru import logger
from p00_Shared_Utils.io_utils import get_root_dir
from p01_Data_Extraction.ingestor import sync_input_folder

def run_2021_2024_batch():
    """
    Orchestrates the extraction of IT Costs and Customer Deposits 
    for all synced bank reports from 2021 to 2024.
    """
    logger.info("🚀 Starting 2021-2024 Extraction Batch...")
    
    # 1. Get project root
    root_dir = get_root_dir()
    
    # 2. Sync folders 
    # Note: ingestor.py now handles root resolution internally
    new_files = sync_input_folder()
    logger.info(f"Sync complete. Found {len(new_files)} new files to process.")
    
    # 3. Define target metrics
    user_prompt = "Information Technology costs AND Deposits from customers"
    
    # 4. Define paths relative to root
    reports_base = root_dir / "data" / "raw" / "reports"
    interim_base = root_dir / "data" / "interim" / "extracted_metrics"
    
    if not reports_base.exists():
        logger.error(f"Reports base directory not found: {reports_base}")
        return

    interim_base.mkdir(parents=True, exist_ok=True)

    processed_count = 0
    failed_count = 0
    
    # Import process_report here to avoid circular imports if any
    from p01_Data_Extraction.pdf_extractor import process_report

    for institution in os.listdir(reports_base):
        inst_path = reports_base / institution
        if not inst_path.is_dir():
            continue
            
        for report_file in os.listdir(inst_path):
            if not report_file.endswith(".pdf"):
                continue
                
            # Extract year from filename (e.g., 2024_fs.pdf)
            year_match = report_file.split("_")[0]
            if not year_match.isdigit() or len(year_match) != 4:
                logger.warning(f"Skipping malformed report filename: {report_file}")
                continue
                
            year = year_match
            # Filter for 2021-2024
            if int(year) < 2021 or int(year) > 2024:
                continue
                
            pdf_path = inst_path / report_file
            
            # Check if already extracted
            output_path = interim_base / f"{institution}_{year}_extracted.json"
            if output_path.exists():
                logger.info(f"⏩ Skipping {institution} {year} - already extracted.")
                continue
            
            logger.info(f"📄 Processing {institution} ({year})...")
            try:
                # Convert Path to string for process_report if it expects strings
                result = process_report(str(pdf_path), institution, year, user_prompt)
                if result:
                    processed_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.error(f"❌ Critical error processing {institution} {year}: {e}")
                failed_count += 1
                
    logger.success(f"✅ Batch complete. Processed: {processed_count}, Failed: {failed_count}")

if __name__ == "__main__":
    run_2021_2024_batch()
