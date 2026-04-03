import re
from typing import Optional, Union
from loguru import logger

def parse_year(input_val: Union[str, int, float]) -> Optional[int]:
    """
    Robustly extract a 4-digit year from a string or number.
    Handles 'FY2024', '2024 (Restated)', '2024-12-31', etc.
    """
    if isinstance(input_val, (int, float)):
        # If it's a number, check if it's in a reasonable range (1900-2100)
        year = int(input_val)
        if 1900 <= year <= 2100:
            return year
        return None
    
    if not isinstance(input_val, str):
        return None
    
    # Use regex to find the first 4-digit number (19XX or 20XX)
    match = re.search(r"(19|20)\d{2}", input_val)
    if match:
        year = int(match.group(0))
        return year
    
    logger.warning(f"Could not parse year from: '{input_val}'")
    return None

def normalize_period(period_str: str) -> str:
    """
    Standardizes period strings like 'FY2024' or '2024' to '2024'.
    """
    year = parse_year(period_str)
    return str(year) if year else period_str

__all__ = ["parse_year", "normalize_period"]
