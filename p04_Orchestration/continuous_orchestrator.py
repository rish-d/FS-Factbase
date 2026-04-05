import os
import time
import json
import datetime
import sys
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Ensure project root and module directories are in sys.path
root_dir = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(root_dir)
sys.path.append(os.path.join(root_dir, "p01_Data_Extraction"))
sys.path.append(os.path.join(root_dir, "p02_Database_and_Mapping"))

from p04_Orchestration.orchestrator import run_pipeline, discover_reports
from p04_Orchestration.status_manager import StatusManager
from p02_Database_and_Mapping.checkpoint_manager import CheckpointManager
from p02_Database_and_Mapping.ai_batch_manager import AIBatchManager
from p02_Database_and_Mapping.mapper import StandardizedMapper

class ContinuousOrchestrator:
    def __init__(self, targets=None):
        self.targets = targets or ["Balance Sheet and Income Statement", "Customer Deposits", "Loans and Advances"]
        self.checkpoint_manager = CheckpointManager()
        self.batch_manager = AIBatchManager(confidence_threshold=0.90)
        self.mapper = StandardizedMapper()
        self.failure_streak = 0
        self.MAX_FAILURES = 3

    def run_cycle(self):
        status = StatusManager.get_status()
        if status.get("running_status") != "RUNNING":
            logger.info("Orchestrator is PAUSED. Sleeping...")
            time.sleep(5)
            self.failure_streak = 0 # Reset streak on manual pause/resume
            return

        # 1. Find the next pending target
        pending = self.checkpoint_manager.get_pending_targets(self.targets)
        if not pending:
            logger.info("No pending targets found. All reports processed. Waiting for new data...")
            StatusManager.update_status(current_target="DONE", activity=f"All targets exhausted at {datetime.datetime.now()}")
            time.sleep(30)
            return

        inst_id, period, task = pending[0]
        logger.info(f"🚀 Starting Target: {inst_id} | {period} | {task}")
        StatusManager.update_status(current_target=f"{inst_id} | {period} | {task}", 
                                    activity=f"Processing {inst_id} ({period}) for '{task}'")

        try:
            # 2. Execute Extraction and Mapping
            # Note: run_pipeline now returns True if at least one report succeeded and none failed.
            success = run_pipeline(user_prompt=task, target_year=period, target_bank=inst_id)
            
            if success:
                # 3. Update Checkpoint
                self.checkpoint_manager.set_checkpoint(inst_id, period, task, "COMPLETED")
                self.failure_streak = 0  # Reset on success
                
                # 4. Dictionary Expansion (Frequent Unmapped)
                logger.info("🤖 Triggering Dictionary Expansion...")
                StatusManager.update_status(activity="Triggering Dictionary Expansion...", is_expanding_dictionary=True)
                resolved_count = self.batch_manager.run_optimistic_batch(limit=5)
                
                StatusManager.update_status(
                    activity=f"SUCCESS: {inst_id} | {period}. Dictionary resolved {resolved_count} terms.",
                    is_expanding_dictionary=False,
                    last_expansion_count=resolved_count
                )
            else:
                logger.warning(f"Extraction returned failure status for {inst_id} | {period} | {task}")
                self.failure_streak += 1
                StatusManager.update_status(activity=f"FAILED (Attempt {self.failure_streak}/{self.MAX_FAILURES}): {inst_id} | {period}")
                
        except Exception as e:
            logger.error(f"Cycle failed for {inst_id} | {period}: {e}")
            self.failure_streak += 1
            StatusManager.update_status(activity=f"CRASH ({self.failure_streak}/{self.MAX_FAILURES}): {inst_id} | {period} - {str(e)}")

        # Auto-pause logic
        if self.failure_streak >= self.MAX_FAILURES:
            logger.critical("🚨 Too many consecutive failures. AUTO-PAUSING Orchestrator.")
            StatusManager.update_status(running_status="PAUSED", activity="AUTO-PAUSED due to excessive failures.")
            self.failure_streak = 0

    def start(self):
        logger.info("Continuous Orchestrator Started.")
        while True:
            self.run_cycle()
            # Small rest between cycles
            time.sleep(2)

if __name__ == "__main__":
    orchestrator = ContinuousOrchestrator()
    orchestrator.start()
