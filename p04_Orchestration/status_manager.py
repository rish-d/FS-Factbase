import os
import json
import datetime
from loguru import logger

STATUS_FILE = os.path.join("data", "loop_status.json")

class StatusManager:
    @staticmethod
    def get_status():
        try:
            if os.path.exists(STATUS_FILE):
                with open(STATUS_FILE, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return {"running_status": "PAUSED", "recent_activity": []}

    @staticmethod
    def update_status(activity=None, current_target=None, **kwargs):
        """
        Thread-safe (mostly, via atomic file writes) status update for the dashboard.
        """
        status = StatusManager.get_status()
        
        if current_target is not None:
            status["current_target"] = current_target
        
        # Merge other kwargs
        status.update(kwargs)
        
        if activity:
            # Prepend activity to log, keep only last 10
            recent = status.setdefault("recent_activity", [])
            recent.insert(0, f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {activity}")
            status["recent_activity"] = recent[:10]
            
        # Write back atomically
        tmp_file = STATUS_FILE + ".tmp"
        try:
            os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
            with open(tmp_file, "w") as f:
                json.dump(status, f, indent=2)
            os.replace(tmp_file, STATUS_FILE)
        except Exception as e:
            logger.error(f"Failed to update loop_status.json: {e}")

if __name__ == "__main__":
    StatusManager.update_status(activity="Status Manager Initialized")
