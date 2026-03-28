import duckdb
from loguru import logger

ALIASES = [
    # (metric_id, raw_term, institution_id)
    ("total_assets", "Cash and short-term funds", "cimb_group_holdings_berhad"),
    ("total_customer_deposits", "Deposits from customers", "cimb_group_holdings_berhad"),
    ("net_interest_income", "Net interest income", "cimb_group_holdings_berhad"),
]

def seed_aliases(db_path="fs_factbase.duckdb"):
    logger.info("Connecting to DuckDB to seed aliases...")
    conn = duckdb.connect(db_path)
    
    logger.info(f"Seeding {len(ALIASES)} metric aliases...")
    
    # Insert aliases
    # Note: alias_id is auto-incrementing
    conn.executemany(
        "INSERT INTO Metric_Aliases (metric_id, raw_term, institution_id) VALUES (?, ?, ?)",
        ALIASES
    )
    
    conn.close()
    logger.info("Alias seeding completed.")

if __name__ == "__main__":
    seed_aliases()
