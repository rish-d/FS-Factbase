import fitz
import os
from loguru import logger

def slice_financials(in_pdf_path: str, out_pdf_path: str) -> bool:
    try:
        doc = fitz.open(in_pdf_path)
    except Exception as e:
        logger.error(f"Failed to open {in_pdf_path}: {e}")
        return False
        
    start_page = -1
    end_page = -1
    
    start_anchors = ["statements of financial position", "statement of financial position"]
    end_anchors = ["independent auditors' report", "independent auditor's report", "auditors' report", "auditors’ report", "independent auditors’ report"]
    
    for i, page in enumerate(doc):
        text = page.get_text("text").lower().replace("\n", " ").replace("  ", " ")
        
        if start_page == -1:
            for anchor in start_anchors:
                if anchor in text:
                    start_page = i
                    logger.info(f"Start Anchor '{anchor}' found on Page {start_page + 1}")
                    break
                    
        elif end_page == -1 and i > start_page:
            for anchor in end_anchors:
                if anchor in text:
                    end_page = i
                    logger.info(f"End Anchor '{anchor}' found on Page {end_page + 1}")
                    break
        
        if start_page != -1 and end_page != -1:
            break
            
    if start_page == -1:
        logger.warning("Could not locate a Start Anchor. Slicing failed.")
        doc.close()
        return False
        
    if end_page == -1:
        logger.warning(f"Could not find End Anchor. Extracting next 150 pages as fallback.")
        end_page = min(start_page + 150, len(doc) - 1)
        
    os.makedirs(os.path.dirname(out_pdf_path), exist_ok=True)
    
    out_doc = fitz.open()
    out_doc.insert_pdf(doc, from_page=start_page, to_page=end_page)
    out_doc.save(out_pdf_path)
    
    logger.success(f"Successfully sliced {end_page - start_page + 1} pages to {out_pdf_path}")
    
    out_doc.close()
    doc.close()
    return True
