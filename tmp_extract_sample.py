import os
import sys

# Ensure d:\FS Factbase is in path
ROOT_DIR = r"d:\FS Factbase"
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from p01_Data_Extraction.pdf_extractor import process_report

pdf_path = os.path.join(ROOT_DIR, "data", "raw", "reports", "MALAYAN BANKING BERHAD", "2022_fs.pdf")
institution_id = "malayan_banking_berhad"
year = "2022"
prompt = "Information Technology costs AND Deposits from customers"

print(f"🚀 Running extraction for {institution_id} ({year})...")
result = process_report(pdf_path, institution_id, year, prompt)

if result:
    print(f"✅ Success! Extracted to {result}")
else:
    print("❌ Extraction failed.")
