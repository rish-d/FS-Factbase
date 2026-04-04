import duckdb
from loguru import logger
import os
import sys
import csv

# Ensure project root is in sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from p02_Database_and_Mapping import db_config

def get_core_metrics(conn):
    """Returns a list of all available core metrics with IDs."""
    return conn.execute("SELECT metric_id, standardized_metric_name FROM Core_Metrics ORDER BY standardized_metric_name").fetchall()

def resolve_unmapped():
    db_path = db_config.get_db_path()
    if not os.path.exists(db_path):
        logger.error(f"Database not found at {db_path}")
        return

    conn = duckdb.connect(db_path)
    # Corrected path joining for custom_aliases.csv
    custom_aliases_path = os.path.join(db_config.ROOT_DIR, "data", "dictionary", "custom_aliases.csv")
    
    print("="*60)
    print("FS FACTBASE: INTERACTIVE METRIC RESOLVER")
    print("="*60)
    
    while True:
        # Get top unmapped terms
        try:
            unmapped = conn.execute("""
                SELECT raw_term, COUNT(*) as frequency, institution_id, GROUP_CONCAT(DISTINCT reporting_period) as periods
                FROM Unmapped_Staging
                GROUP BY raw_term, institution_id
                ORDER BY frequency DESC
                LIMIT 10
            """).fetchall()
        except Exception as e:
            logger.error(f"Error querying Unmapped_Staging: {e}")
            break

        if not unmapped:
            print("\nNo unmapped terms found! Everything is resolved.")
            break

        print(f"\n{'ID':<3} | {'Freq':<5} | {'Raw Term':<40} | {'Institution'}")
        print("-" * 80)
        for i, row in enumerate(unmapped):
            term, freq, inst, periods = row
            print(f"{i:<3} | {freq:<5} | {term[:40]:<40} | {inst[:20] if inst else 'GLOBAL'}")

        choice = input("\nSelect ID to resolve (or 'q' to quit, 's' to skip): ").strip().lower()
        if choice == 'q':
            break
        if choice == 's' or not choice:
            continue
            
        try:
            idx = int(choice)
            target_term, freq, target_inst, periods = unmapped[idx]
        except (ValueError, IndexError):
            print("[X] Invalid selection.")
            continue

        print(f"\nResolving: '{target_term}' ({target_inst if target_inst else 'GLOBAL'})")
        search = input("Search core metrics (part of name) or 'l' to list all: ").strip().lower()
        
        core_metrics = get_core_metrics(conn)
        if search == 'l':
             matches = core_metrics
        else:
             matches = [m for m in core_metrics if search in m[1].lower() or search in m[0].lower()]

        if not matches:
            print("[X] No matching core metrics found.")
            continue

        print(f"\n{'ID':<3} | {'Metric ID':<50} | {'Standard Name'}")
        print("-" * 100)
        for i, (m_id, m_name) in enumerate(matches[:30]): # Limit display
            print(f"{i:<3} | {m_id:<50} | {m_name}")
        
        m_choice = input("\nSelect core metric index (or 'c' to cancel): ").strip().lower()
        if m_choice == 'c': continue
        
        try:
            m_idx = int(m_choice)
            selected_metric_id, selected_metric_name = matches[m_idx]
        except (ValueError, IndexError):
            print("[X] Invalid selection.")
            continue

        confirm = input(f"Map '{target_term}' to '{selected_metric_id}'? (y/n): ").strip().lower()
        if confirm == 'y':
            # 1. Update Live DB
            try:
                # Use standard casing for comparisons later
                conn.execute("INSERT OR IGNORE INTO Metric_Aliases (raw_term, metric_id, institution_id) VALUES (?, ?, ?)", 
                             (target_term, selected_metric_id, target_inst))
                
                # 2. Append to CSV (Persistence)
                os.makedirs(os.path.dirname(custom_aliases_path), exist_ok=True)
                file_exists = os.path.isfile(custom_aliases_path)
                
                # Check for existing entry in CSV to avoid duplicates
                already_in_csv = False
                if file_exists:
                    with open(custom_aliases_path, mode='r', encoding='utf-8') as rf:
                        reader = csv.DictReader(rf)
                        for row in reader:
                            if row['raw_term'] == target_term and row['institution_id'] == str(target_inst):
                                already_in_csv = True
                                break
                
                if not already_in_csv:
                    with open(custom_aliases_path, mode='a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        if not file_exists:
                            writer.writerow(['raw_term', 'metric_id', 'institution_id'])
                        writer.writerow([target_term, selected_metric_id, target_inst])
                
                # 3. Clean up staging (Optional: move to Fact_Financials or wait for re-run)
                # For safety, delete from Unmapped_Staging so it doesn't show up in next loop
                if target_inst:
                    conn.execute("DELETE FROM Unmapped_Staging WHERE raw_term = ? AND institution_id = ?", (target_term, target_inst))
                else:
                    conn.execute("DELETE FROM Unmapped_Staging WHERE raw_term = ? AND institution_id IS NULL", (target_term,))
                
                print(f"[+] Successfully mapped and saved to {os.path.basename(custom_aliases_path)}")
                
            except Exception as e:
                print(f"[X] Error during persistence: {e}")

    conn.close()

if __name__ == "__main__":
    resolve_unmapped()
