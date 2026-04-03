import os
import re
import shutil
from pathlib import Path
from loguru import logger

def get_project_root():
    """Returns the absolute path to the project root."""
    return Path(__file__).resolve().parent.parent

def sync_input_folder(input_name="Bank Annual Reports input", target_rel_path="data/raw/reports"):
    """
    Scans the input folder for PDFs, extracts the year from filenames,
    and copies them to the structured data/raw/reports directory at the project root.
    """
    root_dir = get_project_root()
    input_base = root_dir / input_name
    target_base = root_dir / target_rel_path

    if not input_base.exists():
        logger.warning(f"Input base directory not found: {input_base}")
        return []

    synced_files = []
    year_pattern = re.compile(r'(\d{4})')

    for root, dirs, files in os.walk(input_base):
        for file in files:
            if file.lower().endswith(".pdf"):
                # Institutional ID is the immediate parent directory name
                institution_id = os.path.basename(root)
                
                # If the root is the input_base itself, we don't have an institution_id
                if institution_id == input_name:
                    logger.warning(f"File {file} found in root of input folder. Skipping as institution is unknown.")
                    continue
                
                # Standardize institution_id to uppercase
                institution_id = institution_id.upper()
                
                # Find the year in the filename
                match = year_pattern.search(file)
                if not match:
                    logger.warning(f"Could not find a 4-digit year in filename: {file}. Skipping.")
                    continue
                
                year = match.group(1)
                
                # Target path: project_root/data/raw/reports/{Institution}/{Year}_fs.pdf
                target_dir = target_base / institution_id
                target_filename = f"{year}_fs.pdf"
                target_path = target_dir / target_filename
                
                # Check if it already exists
                if target_path.exists():
                    continue
                
                # Copy the file
                target_dir.mkdir(parents=True, exist_ok=True)
                source_path = Path(root) / file
                try:
                    shutil.copy2(source_path, target_path)
                    logger.info(f"Synced: {file} -> {target_path}")
                    synced_files.append({
                        "institution": institution_id,
                        "year": year,
                        "original_name": file
                    })
                except Exception as e:
                    logger.error(f"Failed to copy {file}: {e}")
                    
    return synced_files

if __name__ == "__main__":
    results = sync_input_folder()
    print(f"Sync complete. New files: {len(results)}")
