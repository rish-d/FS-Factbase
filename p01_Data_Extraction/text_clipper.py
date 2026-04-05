import fitz
import re
from loguru import logger
from typing import Tuple, List

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
        text = page.get_text("text") 
        pages_text[i+1] = text
        
    doc.close()
    return pages_text

def clean_query_terms(user_prompt: str) -> List[str]:
    """Converts a natural language prompt into core target terms."""
    # Extremely simple stop-word removal and tokenization
    stop_words = {"look", "for", "information", "on", "extract", "the", "find", "me", "show", "of", "and"}
    words = re.findall(r'\b\w+\b', user_prompt.lower())
    terms = [w for w in words if w not in stop_words]
    return terms if terms else [user_prompt.lower()]

def score_page_dynamic(text: str, user_prompt: str) -> float:
    """Scores a page based on structural table density AND dynamic user intent."""
    text_lower = text.lower().replace("\n", " ")
    while "  " in text_lower: text_lower = text_lower.replace("  ", " ")
    
    score = 0.0
    
    # 1. Structural Table Density (Base Multiplier)
    # Does this page look like a financial table regardless of content?
    table_features_score = 1.0 # Base
    
    if "rm'000" in text_lower or "rm 000" in text_lower or "in thousands" in text_lower:
        table_features_score += 2.0
        
    if "note" in text_lower:
        table_features_score += 0.5
        
    numbers = sum(c.isdigit() for c in text)
    if numbers > 50:
        table_features_score += 2.0 # High density of numbers
    elif numbers < 10:
        table_features_score *= 0.1 # Penalty for pure text pages
        
    # 2. Dynamic Semantic Relevance
    target_terms = clean_query_terms(user_prompt)
    relevance_score = 0.0
    
    for term in target_terms:
        # Boost exact phrase matches strongly
        if user_prompt.lower() in text_lower:
            relevance_score += 5.0
            
        term_count = text_lower.count(term)
        if term_count > 0:
            # First occurrence gives highest confidence that the page is relevant
            relevance_score += 2.0 + (term_count * 0.2) 
            
    # Final Score: Only structurally dense pages with high relevance pass.
    # If relevance_score is 0, score is 0.
    final_score = table_features_score * relevance_score
    return final_score

def locate_target_financial_pages_dynamic(pages_text: dict[int, str], user_prompt: str) -> List[int]:
    """Returns a list of high-scoring page numbers matching the dynamic user query."""
    scores = []
    
    for page_num, text in pages_text.items():
        score = score_page_dynamic(text, user_prompt)
        scores.append((page_num, score))
        
    # Filter out anything with zero relevance
    valid_candidates = [p for p in scores if p[1] > 5.0]
    
    # Sort and take top 2 pages (assuming tables span 1-2 pages)
    candidates = sorted(valid_candidates, key=lambda x: x[1], reverse=True)
    top_pages = [p[0] for p in candidates[:2]]
    
    return top_pages
    
def get_clipped_financial_text_dynamic(pdf_path: str, user_prompt: str, max_pages: int = 2) -> str:
    """Efficiently scans a PDF for dynamically targeted pages based on a user prompt.
    Supports a 'max_pages' limit to prevent VRAM overflow in local models.
    """
    logger.info(f"Scanning PDF '{pdf_path}' dynamically (limit: {max_pages}) for: '{user_prompt}'")
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        logger.error(f"Failed to open {pdf_path}: {e}")
        return ""
        
    scores = []
    
    # Efficiently score pages first
    for i, page in enumerate(doc):
        page_num = i + 1
        try:
            text = page.get_text("text")
            score = score_page_dynamic(text, user_prompt)
            scores.append((page_num, score))
        except Exception as e:
            logger.warning(f"Failed to extract text from page {page_num}: {e}")
            continue
            
    # Filter out anything with zero relevance and take top N pages
    valid_candidates = [p for p in scores if p[1] > 5.0]
    candidates = sorted(valid_candidates, key=lambda x: x[1], reverse=True)
    
    # Take top N pages based on max_pages
    target_pages = sorted(list(set([p[0] for p in candidates[:max_pages]])))
    
    if not target_pages:
        logger.warning(f"No pages met the threshold in {pdf_path} for query: '{user_prompt}'.")
        doc.close()
        return ""
        
    logger.info(f"Dynamic target pages localized: {target_pages}")
    
    clipped_text = ""
    for page_num in target_pages:
        page = doc[page_num - 1]
        clipped_text += f"\n\n--- TARGET FINANCIAL PAGE {page_num} ---\n\n"
        clipped_text += page.get_text("text")
        
    doc.close()
    return clipped_text
