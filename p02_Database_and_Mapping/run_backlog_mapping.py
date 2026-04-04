import os
import sys
from loguru import logger

# Ensure project root is in sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from p02_Database_and_Mapping.mapper import StandardizedMapper

def main():
    logger.info("Starting Bulk Backlog Population...")
    mapper = StandardizedMapper()
    
    # This method scans Unmapped_Staging and re-maps against latest aliases
    mapper.process_unmapped_staging()
    
    logger.success("Bulk Backlog Population completed.")

if __name__ == "__main__":
    main()
