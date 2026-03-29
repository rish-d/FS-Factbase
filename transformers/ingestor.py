import os
import re
import shutil
from loguru import logger

def sync_input_folder(input_base="Bank Annual Reports input", target_base="data/raw/reports"):
    """
    Scans the input folder for PDFs, extracts the year from filenames,
    and copies them to the structured data/raw/reports directory.
    """
    if not os.path.exists(input_base):
        logger.warning(f"Input base directory not found: {input_base}")
        return []

    synced_files = []
    
    # regex to find 4-digit years like 2021, 2022, etc.
    year_pattern = re.compile(r'(\d{4})')

    for root, dirs, files in os.walk(input_base):
        for file in files:
            if file.lower().endswith(".pdf"):
                # Institutional ID is the immediate parent directory name
                institution_id = os.path.basename(root)
                
                # If the root is the input_base itself, we don't have an institution_id
                if institution_id == input_base:
                    logger.warning(f"File {file} found in root of input folder. Skipping as institution is unknown.")
                    continue
                
                # Find the year in the filename
                match = year_pattern.search(file)
                if not match:
                    logger.warning(f"Could not find a 4-digit year in filename: {file}. Skipping.")
                    continue
                
                year = match.group(1)
                
                # Target path: data/raw/reports/{Institution}/{Year}_fs.pdf
                target_dir = os.path.join(target_base, institution_id)
                target_filename = f"{year}_fs.pdf"
                target_path = os.path.join(target_dir, target_filename)
                
                # Check if it already exists
                if os.path.exists(target_path):
                    # logger.debug(f"File already synced: {target_path}")
                    continue
                
                # Copy the file
                os.makedirs(target_dir, exist_ok=True)
                source_path = os.path.join(root, file)
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
