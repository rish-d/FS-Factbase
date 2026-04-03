import os

# Calculate the path to the project root relative to this script's directory
# p02_Database_and_Mapping is at the root level of the project.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

def get_db_path():
    """Returns the absolute path to the DuckDB database file."""
    return os.path.join(BASE_DIR, "fs_factbase.duckdb")

if __name__ == "__main__":
    print(f"Base Directory: {BASE_DIR}")
    print(f"Root Directory: {ROOT_DIR}")
    print(f"DB Path: {get_db_path()}")
