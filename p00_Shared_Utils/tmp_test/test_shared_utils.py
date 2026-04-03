import os
import sys
from pathlib import Path

# Add parent directory to sys.path to allow importing from current workspace
sys.path.append(str(Path(__file__).resolve().parent.parent))

from io_utils import get_root_dir, validate_audit_trail
from logging_utils import logger, setup_logger

def test_get_root_dir():
    print("\n--- Testing get_root_dir ---")
    try:
        root = get_root_dir()
        print(f"Found root: {root}")
        # Expecting d:\FS Factbase
        if "FS Factbase" in str(root):
            print("SUCCESS: Root directory found correctly.")
        else:
            print(f"FAILURE: Root directory '{root}' does not match expected pattern 'FS Factbase'.")
    except Exception as e:
        print(f"FAILURE: get_root_dir raised exception: {e}")

def test_validate_audit_trail():
    print("\n--- Testing validate_audit_trail ---")
    
    # 1. Valid file (relative to root)
    root = get_root_dir()
    rel_path = "MASTER_PLAN.md"
    is_valid = validate_audit_trail(rel_path, 1)
    print(f"Valid file (rel): {rel_path}, page: 1 -> Result: {is_valid}")
    
    # 2. Valid file (absolute)
    abs_path = root / "ARCHITECTURE.md"
    is_valid = validate_audit_trail(abs_path, 5)
    print(f"Valid file (abs): {abs_path}, page: 5 -> Result: {is_valid}")
    
    # 3. Invalid file
    invalid_path = "non_existent_report.pdf"
    is_valid = validate_audit_trail(invalid_path, 1)
    print(f"Invalid file: {invalid_path} -> Result: {is_valid}")
    
    # 4. Invalid page number (negative)
    is_valid = validate_audit_trail(rel_path, -1)
    print(f"Invalid page (-1): {rel_path} -> Result: {is_valid}")
    
    # 5. Invalid page number (string)
    is_valid = validate_audit_trail(rel_path, "1")
    print(f"Invalid page ('1'): {rel_path} -> Result: {is_valid}")

def test_logging():
    print("\n--- Testing logging format ---")
    test_log_file = "logs/test_audit.log"
    setup_logger(log_file=test_log_file)
    
    logger.info("This is an audit trail test message.")
    logger.warning("This is a warning for auditability.")
    logger.error("This is an error with structured data: bank='Maybank', year=2024")
    
    print(f"Logs should be visible in console and saved to {test_log_file}")
    
    if os.path.exists(test_log_file):
        with open(test_log_file, "r") as f:
            last_line = f.readlines()[-1]
            print(f"Last log line in file: {last_line.strip()}")
            if "." in last_line.split("|")[0]: # Check for millisecond precision
                print("SUCCESS: Millisecond precision found in log.")
            else:
                print("FAILURE: Millisecond precision NOT found in log.")

if __name__ == "__main__":
    test_get_root_dir()
    test_validate_audit_trail()
    test_logging()
