import fitz
from loguru import logger
from typing import Tuple

def convert_pdf_to_text_pages(pdf_path: str) -> dict[int, str]:
    """Extracts text from each page, returning a dict of {page_num: text}"""
    logger.info(f"Converting PDF {pdf_path} to paginated text...")
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        logger.error(f"Failed to open {pdf_path}: {e}")
        return {}
        
    pages_text = {}
    for i, page in enumerate(doc):
        # Extract text preserving rough layout/newlines to help LLM structure
        text = page.get_text("text") 
        pages_text[i+1] = text
        
    doc.close()
    return pages_text

def score_page_as_balance_sheet(text: str) -> int:
    text_lower = text.lower().replace("\n", " ")
    while "  " in text_lower: text_lower = text_lower.replace("  ", " ")
    score = 0
    # Core anchors
    if "statements of financial position" in text_lower or "statement of financial position" in text_lower or "balance sheet" in text_lower:
        score += 50
        
    # Structural keywords indicating a real table, not just ToC
    if "assets" in text_lower: score += 10
    if "liabilities" in text_lower: score += 10
    if "equity" in text_lower: score += 10
    if "cash and short-term funds" in text_lower: score += 20
    if "deposits from customers" in text_lower: score += 20
    
    # Financial table features
    if "note" in text_lower: score += 5
    if "rm'000" in text_lower or "rm 000" in text_lower or "in thousands" in text_lower: score += 15
    
    # Number density (crude but effective check for actual tables)
    numbers = sum(c.isdigit() for c in text)
    if numbers > 50:
        score += 20
        
    return score

def score_page_as_income_statement(text: str) -> int:
    text_lower = text.lower().replace("\n", " ")
    while "  " in text_lower: text_lower = text_lower.replace("  ", " ")
    score = 0
    # Core anchors
    if "statements of profit or loss" in text_lower or "statement of profit or loss" in text_lower or "income statements" in text_lower or "income statement" in text_lower:
        score += 50
        
    # Structural keywords
    if "interest income" in text_lower: score += 20
    if "interest expense" in text_lower: score += 20
    if "net interest income" in text_lower: score += 20
    if "profit before taxation" in text_lower or "profit before tax" in text_lower: score += 20
    if "taxation" in text_lower or "tax expense" in text_lower: score += 10
    
    if "note" in text_lower: score += 5
    if "rm'000" in text_lower or "rm 000" in text_lower or "in thousands" in text_lower: score += 15
    
    numbers = sum(c.isdigit() for c in text)
    if numbers > 50:
        score += 20
        
    return score

def locate_target_financial_pages(pages_text: dict[int, str]) -> Tuple[list[int], list[int]]:
    """Returns a list of high-scoring page numbers for Balance Sheet and Income Statement"""
    bs_scores = []
    is_scores = []
    
    for page_num, text in pages_text.items():
        bs_scores.append((page_num, score_page_as_balance_sheet(text)))
        is_scores.append((page_num, score_page_as_income_statement(text)))
        
    # Get highest scoring pages (must be >= 80 to be fundamentally structural)
    bs_candidates = sorted([p for p in bs_scores if p[1] >= 80], key=lambda x: x[1], reverse=True)
    is_candidates = sorted([p for p in is_scores if p[1] >= 80], key=lambda x: x[1], reverse=True)
    
    bs_pages = [p[0] for p in bs_candidates[:2]] # Take top 2 pages just in case it spans 2 pages
    is_pages = [p[0] for p in is_candidates[:2]]
    
    return bs_pages, is_pages
    
def get_clipped_financial_text(pdf_path: str) -> str:
    """Efficiently scans a PDF for BS/IS pages without loading all text into memory."""
    logger.info(f"Opening PDF for localized scanning: {pdf_path}")
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        logger.error(f"Failed to open {pdf_path}: {e}")
        return ""
        
    bs_scores = []
    is_scores = []
    
    # Efficiently score pages first
    for i, page in enumerate(doc):
        page_num = i + 1
        # Extract small chunks of text or specific area if needed, but here we just get raw text for scoring
        # Optimization: use small flags for faster extraction if supported
        try:
            text = page.get_text("text")
            bs_scores.append((page_num, score_page_as_balance_sheet(text)))
            is_scores.append((page_num, score_page_as_income_statement(text)))
        except Exception as e:
            logger.warning(f"Failed to extract text from page {page_num}: {e}")
            continue
            
    # Get highest scoring pages (threshold >= 80)
    bs_candidates = sorted([p for p in bs_scores if p[1] >= 80], key=lambda x: x[1], reverse=True)
    is_candidates = sorted([p for p in is_scores if p[1] >= 80], key=lambda x: x[1], reverse=True)
    
    # Take top 2 pages for each to handle multi-page tables
    target_pages = sorted(list(set([p[0] for p in bs_candidates[:2]] + [p[0] for p in is_candidates[:2]])))
    
    if not target_pages:
        logger.warning(f"No pages met the threshold in {pdf_path}.")
        doc.close()
        return ""
        
    logger.info(f"Target pages localized: {target_pages}")
    
    clipped_text = ""
    for page_num in target_pages:
        # Re-extract the text for targeted pages (redundant but safer if we had different extraction methods)
        page = doc[page_num - 1]
        clipped_text += f"\n\n--- TARGET FINANCIAL PAGE {page_num} ---\n\n"
        clipped_text += page.get_text("text")
        
    doc.close()
    return clipped_text
