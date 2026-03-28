import os
from loguru import logger
from extractors.text_clipper import convert_pdf_to_text_pages, locate_target_financial_pages

if __name__ == "__main__":
    base_dir = "data/raw/reports"
    for bank_dir in sorted(os.listdir(base_dir)):
        report_dir = os.path.join(base_dir, bank_dir)
        if not os.path.isdir(report_dir): continue
        
        pdfs = sorted([f for f in os.listdir(report_dir) if f.endswith(".pdf")])
        
        for pdf in pdfs:
            path = os.path.join(report_dir, pdf)
            logger.info(f"Testing Clipper on: [{bank_dir}] {pdf}")
            
            pages_text = convert_pdf_to_text_pages(path)
            if pages_text:
                bs_pages, is_pages = locate_target_financial_pages(pages_text)
            logger.info(f"-> Balance Sheet Candidates: {bs_pages}")
            logger.info(f"-> Income Statement Candidates: {is_pages}")
            logger.info("*" * 40)
        else:
            logger.error(f"Failed to read {pdf}")
