import duckdb
import datetime
import os
import sys
from loguru import logger

# Ensure project root is in sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import p02_Database_and_Mapping.db_config as db_config

class CheckpointManager:
    def __init__(self, db_path=None):
        self.db_path = db_path or db_config.get_db_path()

    def get_checkpoint(self, institution_id, reporting_period, task_name):
        conn = duckdb.connect(self.db_path)
        try:
            row = conn.execute("""
                SELECT status FROM Pipeline_Checkpoints 
                WHERE institution_id = ? AND reporting_period = ? AND task_name = ?
            """, [institution_id, reporting_period, task_name]).fetchone()
            return row[0] if row else "PENDING"
        finally:
            conn.close()

    def set_checkpoint(self, institution_id, reporting_period, task_name, status):
        conn = duckdb.connect(self.db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO Pipeline_Checkpoints 
                (institution_id, reporting_period, task_name, status, last_run)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [institution_id, reporting_period, task_name, status])
            logger.info(f"Checkpoint set: {institution_id} | {reporting_period} | {task_name} -> {status}")
        finally:
            conn.close()

    def get_pending_targets(self, targets_list):
        """
        Returns a list of (institution_id, reporting_period, task_name) that are not COMPLETED.
        Optimized to use a single DB connection.
        """
        base_path = "data/raw/reports"
        reports = []
        if not os.path.exists(base_path):
            return []
            
        for root, dirs, files in os.walk(base_path):
            for file in files:
                if file.endswith(".pdf"):
                    inst_id = os.path.basename(root)
                    period = file[:4]
                    reports.append((inst_id, period))
        
        pending = []
        conn = duckdb.connect(self.db_path)
        try:
            # Pre-fetch all checkpoints to avoid nested connections
            rows = conn.execute("SELECT institution_id, reporting_period, task_name, status FROM Pipeline_Checkpoints").fetchall()
            completed_map = { (r[0], r[1], r[2]): r[3] for r in rows if r[3] == "COMPLETED" }

            for inst_id, period in reports:
                for task in targets_list:
                    if (inst_id, period, task) not in completed_map:
                        pending.append((inst_id, period, task))
            return pending
        finally:
            conn.close()

if __name__ == "__main__":
    cm = CheckpointManager()
    print(cm.get_pending_targets(["Balance Sheet and Income Statement"]))
