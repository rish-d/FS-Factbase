import json
import os
from pathlib import Path
from typing import Any, Union, Optional
from loguru import logger

def ensure_directory(path: Union[str, Path]) -> Path:
    """
    Ensure the parent directory of a file path (or the path itself if a directory) exists.
    """
    p = Path(path)
    # If the path has an extension, assume it's a file path and create parent directory
    if p.suffix:
        p.parent.mkdir(parents=True, exist_ok=True)
    else:
        p.mkdir(parents=True, exist_ok=True)
    return p

def save_json(data: Any, path: Union[str, Path], indent: int = 2) -> bool:
    """
    Saves a dictionary or list to a JSON file, creating parent directories if needed.
    """
    try:
        p = ensure_directory(path)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        logger.debug(f"JSON saved to: {p}")
        return True
    except Exception as e:
        logger.error(f"Failed to save JSON to {path}: {e}")
        return False

def load_json(path: Union[str, Path]) -> Optional[Any]:
    """
    Loads JSON from a file.
    """
    p = Path(path)
    if not p.exists():
        logger.warning(f"File not found: {p}")
        return None
    
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load JSON from {path}: {e}")
        return None

def write_text(content: str, path: Union[str, Path]) -> bool:
    """
    Saves text to a file, creating parent directories if needed.
    """
    try:
        p = ensure_directory(path)
        with open(p, "w", encoding="utf-8") as f:
            f.write(content)
        logger.debug(f"Text saved to: {p}")
        return True
    except Exception as e:
        logger.error(f"Failed to save text to {path}: {e}")
        return False

def get_root_dir(marker: str = "MASTER_PLAN.md") -> Path:
    """
    Find the project root directory by searching upwards for a marker file.
    """
    # Start from the current file's parent
    current_path = Path(__file__).resolve().parent
    
    # Check current path and all its parents
    if (current_path / marker).exists():
        return current_path
        
    for parent in current_path.parents:
        if (parent / marker).exists():
            return parent
    
    # Fallback: check CWD and its parents
    current_cwd = Path.cwd().resolve()
    if (current_cwd / marker).exists():
        return current_cwd
    
    for parent in current_cwd.parents:
        if (parent / marker).exists():
            return parent
            
    raise FileNotFoundError(f"Could not find root directory containing {marker}")

def validate_audit_trail(source_document: Union[str, Path], source_page_number: Any) -> bool:
    """
    Validate the audit trail metadata for Phase 4 compliance.
    source_document: Path to the PDF/document (relative to root or absolute).
    source_page_number: Must be a positive integer.
    """
    try:
        # 1. Validate page number
        if not isinstance(source_page_number, int) or source_page_number < 0:
            logger.error(f"Invalid source_page_number: {source_page_number}. Must be a non-negative integer.")
            return False
        
        # 2. Validate document path
        doc_path = Path(source_document)
        if not doc_path.is_absolute():
            try:
                root = get_root_dir()
                doc_path = root / doc_path
            except FileNotFoundError:
                # If root can't be found, we just check existence as is
                pass
            
        if not doc_path.exists():
            logger.error(f"Source document does not exist: {doc_path}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Audit trail validation failed: {e}")
        return False

__all__ = ["ensure_directory", "save_json", "load_json", "write_text", "get_root_dir", "validate_audit_trail"]
