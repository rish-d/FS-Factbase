import http.server
import socketserver
import json
import os
import sys
import duckdb
from urllib.parse import urlparse, parse_qs

# Ensure project root is in sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import p02_Database_and_Mapping.db_config as db_config

PORT = 8080
DIRECTORY = os.path.join(os.path.dirname(__file__), "static")
STATUS_FILE = os.path.join("data", "loop_status.json")

class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/api/status":
            self.get_status()
        else:
            super().do_GET()

    def do_POST(self):
        url = urlparse(self.path)
        if url.path == "/api/control":
            self.post_control()
        else:
            self.send_error(404)

    def get_recent_logs(self, num_lines=50):
        log_path = os.path.join("p01_Data_Extraction", "logs", "app.log")
        if not os.path.exists(log_path):
            return ["Log file not found."]
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                content = f.readlines()
                return [line.strip() for line in content[-num_lines:]]
        except Exception as e:
            return [f"Error reading logs: {str(e)}"]

    def get_status(self):
        # 1. Load basic status
        status = {}
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, "r") as f:
                status = json.load(f)
        
        # 2. Augment with DB stats
        db_path = db_config.get_db_path()
        try:
            conn = duckdb.connect(db_path, read_only=True)
            facts_count = conn.execute("SELECT count(*) FROM Fact_Financials").fetchone()[0]
            unmapped_count = conn.execute("SELECT count(*) FROM Unmapped_Staging").fetchone()[0]
            metrics_count = conn.execute("SELECT count(*) FROM Core_Metrics").fetchone()[0]
            
            # Fetch last 5 mapped facts
            recent_facts = conn.execute("""
                SELECT institution_id, reporting_period, metric_id, value 
                FROM Fact_Financials 
                ORDER BY fact_id DESC LIMIT 5
            """).fetchall()
            
            status["db_stats"] = {
                "total_facts": facts_count,
                "unmapped_count": unmapped_count,
                "core_metrics": metrics_count,
                "recent_facts": [f"{r[0]} ({r[1]}): {r[2]}={r[3]}" for r in recent_facts]
            }
            conn.close()
        except Exception as e:
            status["db_stats"] = {"error": str(e)}

        # 3. Add recent system logs
        status["system_logs"] = self.get_recent_logs()

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(status).encode())

    def post_control(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        try:
            data = json.loads(post_data)
        except json.JSONDecodeError as e:
            print(f"FAILED to parse JSON: {post_data}")
            self.send_error(400, f"Invalid JSON: {str(e)}")
            return
        
        command = data.get("command") # "START" or "PAUSE"
        
        if command in ["START", "PAUSE"]:
            status = {}
            if os.path.exists(STATUS_FILE):
                with open(STATUS_FILE, "r") as f:
                    status = json.load(f)
            
            status["running_status"] = "RUNNING" if command == "START" else "PAUSE"
            
            with open(STATUS_FILE, "w") as f:
                json.dump(status, f, indent=2)
            
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok", "new_state": status["running_status"]}).encode())
        else:
            self.send_error(400, "Invalid Command")

def run_server():
    os.makedirs(DIRECTORY, exist_ok=True)
    with socketserver.TCPServer(("", PORT), DashboardHandler) as httpd:
        print(f"🚀 Dashboard serving at http://localhost:{PORT}")
        httpd.serve_forever()

if __name__ == "__main__":
    run_server()
