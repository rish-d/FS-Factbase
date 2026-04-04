import duckdb
import argparse
import sys
import os

# Ensure project root is in sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

def query_facts(institution=None, period=None):
    db_path = "fs_factbase.duckdb"
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found.")
        return

    conn = duckdb.connect(db_path)
    
    query = """
    SELECT f.institution_id, f.reporting_period, f.metric_id, f.value, f.confidence_score
    FROM Fact_Financials f
    WHERE 1=1
    """
    params = []
    
    if institution:
        query += " AND f.institution_id LIKE ?"
        params.append(f"%{institution}%")
    if period:
        query += " AND f.reporting_period = ?"
        params.append(str(period))
        
    query += " ORDER BY f.institution_id, f.reporting_period, f.metric_id"
    
    try:
        results = conn.execute(query, params).fetchall()
        
        if not results:
            print("\nNo facts found matching your criteria.")
            return

        print("\n" + "="*80)
        print(f"{'INSTITUTION':<30} | {'PERIOD':<8} | {'METRIC':<20} | {'VALUE':<12} | {'CONF'}")
        print("-" * 80)
        for r in results:
            inst, per, metric, val, conf = r
            # Format value
            if isinstance(val, (int, float)):
                val_str = f"{val:,.2f}"
            else:
                val_str = str(val)
                
            print(f"{inst[:30]:<30} | {per:<8} | {metric[:20]:<20} | {val_str:<12} | {conf:.2f}")
        print("="*80)
        print(f"Total: {len(results)} facts.")
        
    except Exception as e:
        print(f"Query Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="View standardized facts from the DuckDB factbase.")
    parser.add_argument("--bank", help="Filter by bank name (partial match)")
    parser.add_argument("--year", help="Filter by reporting year")
    args = parser.parse_args()
    
    query_facts(args.bank, args.year)
