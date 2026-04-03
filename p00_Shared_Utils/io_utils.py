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

__all__ = ["ensure_directory", "save_json", "load_json", "write_text"]
