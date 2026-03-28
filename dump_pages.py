import fitz

doc = fitz.open("data/raw/reports/cimb_group_holdings_berhad/2024_fs.pdf")
with open("data/interim/debug_pages.txt", "w", encoding="utf-8") as f:
    f.write("=== PAGE 42 ===\n")
    f.write(doc[42].get_text("text"))
    f.write("\n=== PAGE 43 ===\n")
    f.write(doc[43].get_text("text"))
    f.write("\n=== PAGE 44 ===\n")
    f.write(doc[44].get_text("text"))
    f.write("\n=== PAGE 45 ===\n")
    f.write(doc[45].get_text("text"))
