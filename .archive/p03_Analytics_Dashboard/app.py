from pathlib import Path
import sys

# Robust Pathing
BASE_DIR = Path(__file__).parent.parent.resolve()
APP_DIR = Path(__file__).parent.resolve()
DB_PATH = BASE_DIR / "fs_factbase.duckdb"
REPORTS_DIR = BASE_DIR / "data" / "raw" / "reports"

# Ensure imports work regardless of CWD
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# Satisfy internal imports for workspace packages
P02_DIR = BASE_DIR / "p02_Database_and_Mapping"
if str(P02_DIR) not in sys.path:
    sys.path.append(str(P02_DIR))

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import duckdb
import os
import threading
from pydantic import BaseModel
from typing import List, Optional, Dict

from p02_Database_and_Mapping.batch_resolver import BatchResolver
from p03_Analytics_Dashboard.sql_agent import FinancialSQLAgent
from p01_Data_Extraction.ingestor import sync_input_folder
from p02_Database_and_Mapping.cluster_analyzer import ClusterAnalyzer
from p04_Orchestration.orchestrator import run_pipeline
from p03_Analytics_Dashboard.comparison_engine import FinancialComparisonEngine

# Peer Group Models
class PeerGroupCreate(BaseModel):
    name: str
    institution_ids: List[str]

class ComparisonRequest(BaseModel):
    institution_id: str
    metrics: List[str]
    period: str = "2024"
    group_id: Optional[int] = None

app = FastAPI(title="FS Factbase Dashboard")

# Global status for the extraction pipeline
pipeline_status = {
    "is_running": False,
    "last_run": None,
    "last_status": "Idle",
    "synced_count": 0
}

def get_db_connection(read_only=True):
    return duckdb.connect(DB_PATH, read_only=read_only)

@app.get("/api/metrics")
async def get_metrics():
    try:
        conn = get_db_connection(read_only=True)
        rows = conn.execute("SELECT * FROM Core_Metrics").fetchall()
        conn.close()
        return [{"metric_id": r[0], "name": r[1], "standard": r[2], "type": r[3]} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/api/unmapped/{staging_id}")
async def update_unmapped(staging_id: int, data: dict):
    try:
        conn = get_db_connection(read_only=False)
        
        # Log the correction before updating
        original = conn.execute("SELECT raw_term, raw_value, institution_id, source_document, source_page_number FROM Unmapped_Staging WHERE staging_id = ?", [staging_id]).fetchone()
        
        if "value" in data:
            try:
                val = float(data["value"])
                conn.execute("UPDATE Unmapped_Staging SET raw_value = ?, confidence_score = 1.0, confidence_reason = 'Manually Corrected' WHERE staging_id = ?", [val, staging_id])
                
                if original:
                    conn.execute("""
                        INSERT INTO Extraction_Corrections (institution_id, raw_term, original_value, corrected_value, source_document, page_number, reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [original[2], original[0], original[1], val, original[3], original[4], data.get("reason", "Manual Correction")])
            except ValueError:
                raise HTTPException(status_code=400, detail="Value must be a number")
        
        if "term" in data:
            conn.execute("UPDATE Unmapped_Staging SET raw_term = ? WHERE staging_id = ?", [data["term"], staging_id])
            
        conn.close()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/unmapped/{staging_id}")
async def delete_unmapped(staging_id: int):
    try:
        conn = get_db_connection(read_only=False)
        conn.execute("DELETE FROM Unmapped_Staging WHERE staging_id = ?", [staging_id])
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/unmapped/{staging_id}/map")
async def map_unmapped(staging_id: int, data: dict):
    try:
        metric_id = data.get("metric_id")
        if not metric_id:
            raise HTTPException(status_code=400, detail="metric_id is required")

        conn = get_db_connection(read_only=False)
        
        # 1. Get the unmapped record
        row = conn.execute("SELECT * FROM Unmapped_Staging WHERE staging_id = ?", [staging_id]).fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="Staging record not found")
        
        # row: (staging_id, raw_term, raw_value, institution_id, reporting_period, source_document, source_page_number)
        raw_term, raw_value, inst_id, period, doc, page = row[1], row[2], row[3], row[4], row[5], row[6]

        # 2. Add to Metric_Aliases (Upsert)
        # Check if this alias already exists for this institution
        alias_exists = conn.execute(
            "SELECT 1 FROM Metric_Aliases WHERE metric_id = ? AND raw_term = ? AND institution_id = ?", 
            [metric_id, raw_term, inst_id]
        ).fetchone()
        
        if not alias_exists:
            conn.execute(
                "INSERT INTO Metric_Aliases (metric_id, raw_term, institution_id) VALUES (?, ?, ?)",
                [metric_id, raw_term, inst_id]
            )

        # 3. Add to Fact_Financials
        conn.execute("""
            INSERT INTO Fact_Financials (metric_id, institution_id, reporting_period, value, source_document, source_page_number)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [metric_id, inst_id, period, raw_value, doc, page])

        # 4. Remove from Unmapped_Staging
        conn.execute("DELETE FROM Unmapped_Staging WHERE staging_id = ?", [staging_id])
        
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/api/facts/{fact_id}")
async def update_fact(fact_id: int, data: dict):
    try:
        conn = get_db_connection(read_only=False)
        
        # Log correction
        original = conn.execute("SELECT metric_id, value, institution_id, source_document, source_page_number FROM Fact_Financials WHERE fact_id = ?", [fact_id]).fetchone()

        if "value" in data:
            try:
                val = float(data["value"])
                conn.execute("UPDATE Fact_Financials SET value = ?, confidence_score = 1.0, confidence_reason = 'Manually Corrected' WHERE fact_id = ?", [val, fact_id])
                
                if original:
                    # Get raw term from Metric_Aliases if possible to log it correctly
                    raw_term = conn.execute("SELECT raw_term FROM Metric_Aliases WHERE metric_id = ? AND institution_id = ?", [original[0], original[2]]).fetchone()
                    term_str = raw_term[0] if raw_term else original[0]
                    
                    conn.execute("""
                        INSERT INTO Extraction_Corrections (institution_id, raw_term, original_value, corrected_value, source_document, page_number, reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [original[2], term_str, original[1], val, original[3], original[4], data.get("reason", "Manual Correction")])
            except ValueError:
                raise HTTPException(status_code=400, detail="Value must be a number")
        
        if "metric_id" in data:
            conn.execute("UPDATE Fact_Financials SET metric_id = ? WHERE fact_id = ?", [data["metric_id"], fact_id])
            
        conn.close()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/facts/{fact_id}")
async def delete_fact(fact_id: int):
    try:
        conn = get_db_connection(read_only=False)
        conn.execute("DELETE FROM Fact_Financials WHERE fact_id = ?", [fact_id])
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/facts")
async def get_facts():
    try:
        conn = get_db_connection(read_only=True)
        rows = conn.execute("""
            SELECT f.*, m.standardized_metric_name 
            FROM Fact_Financials f
            JOIN Core_Metrics m ON f.metric_id = m.metric_id
        """).fetchall()
        conn.close()
        return [
            {
                "fact_id": r[0],
                "metric_id": r[1],
                "institution": r[2],
                "period": r[3],
                "value": r[4],
                "source_doc": r[5],
                "page": r[6],
                "confidence_score": r[7],
                "confidence_reason": r[8],
                "metric_name": r[9]
            } for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/unmapped")
async def get_unmapped():
    try:
        conn = get_db_connection(read_only=True)
        rows = conn.execute("SELECT * FROM Unmapped_Staging").fetchall()
        conn.close()
        return [
            {
                "staging_id": r[0],
                "term": r[1],
                "value": r[2],
                "institution": r[3],
                "period": r[4],
                "source_doc": r[5],
                "page": r[6],
                "confidence_score": r[7],
                "confidence_reason": r[8]
            } for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/lessons")
async def get_lessons():
    try:
        conn = get_db_connection(read_only=True)
        rows = conn.execute("SELECT * FROM Diagnostic_Lessons WHERE is_active = TRUE").fetchall()
        conn.close()
        return [
            {
                "lesson_id": r[0],
                "institution": r[1],
                "pattern": r[2],
                "advice": r[3],
                "created_at": r[4]
            } for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/diagnose/run")
async def run_diagnostics():
    try:
        # Trigger the batch script in a background process (simulation for now)
        # In a real app, use a task queue like Celery or just subprocess.Popen
        import subprocess
        import sys
        
        # Note: In windows, we use the venv python
        python_exe = sys.executable
        subprocess.Popen([python_exe, "-m", "p04_Orchestration.run_diagnostics"])
        
        return {"status": "started", "message": "Batch diagnostic learning process initiated."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class ResolutionRequest(BaseModel):
    target_metric_id: str
    aliases: List[str]
    create_new_metric: bool = False
    new_metric_details: Optional[dict] = None

@app.get("/api/clusters")
async def get_clusters():
    try:
        analyzer = ClusterAnalyzer(DB_PATH)
        return analyzer.get_clusters()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/resolve_cluster")
async def resolve_cluster(req: ResolutionRequest):
    try:
        resolver = BatchResolver(DB_PATH)
        success = resolver.resolve_cluster_to_metric(
            req.target_metric_id, 
            req.aliases, 
            req.create_new_metric, 
            req.new_metric_details
        )
        if success:
            return {"status": "success"}
        else:
            raise HTTPException(status_code=500, detail="Batch resolution transaction failed.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/chat")
async def chat(data: dict):
    try:
        question = data.get("question")
        if not question:
            raise HTTPException(status_code=400, detail="Missing question")
            
        agent = FinancialSQLAgent(DB_PATH)
        result = agent.process_query(question)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pipeline/status")
async def get_pipeline_status():
    return pipeline_status

@app.post("/api/pipeline/sync")
async def sync_pipeline():
    try:
        new_files = sync_input_folder()
        pipeline_status["synced_count"] = len(new_files)
        pipeline_status["last_status"] = f"Synced {len(new_files)} new files."
        return {"status": "success", "new_files": len(new_files)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def run_extraction_worker(prompt: str):
    global pipeline_status
    pipeline_status["is_running"] = True
    pipeline_status["last_status"] = "Processing extraction..."
    try:
        run_pipeline(user_prompt=prompt)
        pipeline_status["last_status"] = "Extraction Complete."
    except Exception as e:
        pipeline_status["last_status"] = f"Error: {str(e)}"
    finally:
        pipeline_status["is_running"] = False

@app.post("/api/pipeline/run")
async def run_extraction_pipeline(data: dict, background_tasks: BackgroundTasks):
    if pipeline_status["is_running"]:
        raise HTTPException(status_code=400, detail="Pipeline is already running.")
    
    prompt = data.get("prompt", "Balance Sheet and Income Statement")
    background_tasks.add_task(run_extraction_worker, prompt)
    
    return {"status": "started", "message": "Extraction process started in the background."}

@app.get("/api/benchmarking/rankings")
async def get_rankings(metric_id: str, period: str = "2024", sector: Optional[str] = None):
    try:
        engine = FinancialComparisonEngine(DB_PATH)
        return engine.get_rankings(metric_id, period, sector)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/benchmarking/compare")
async def compare_peer(req: ComparisonRequest):
    try:
        engine = FinancialComparisonEngine(DB_PATH)
        return engine.get_peer_comparison(req.institution_id, req.metrics, req.period, req.group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/peer-groups")
async def get_peer_groups():
    try:
        conn = get_db_connection(read_only=True)
        groups = conn.execute("SELECT * FROM Peer_Groups").fetchall()
        
        result = []
        for g in groups:
            members = conn.execute("SELECT institution_id FROM Peer_Group_Members WHERE group_id = ?", [g[0]]).fetchall()
            result.append({
                "id": g[0],
                "name": g[1],
                "members": [m[0] for m in members]
            })
        conn.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/peer-groups")
async def create_peer_group(req: PeerGroupCreate):
    try:
        conn = get_db_connection(read_only=False)
        conn.execute("INSERT INTO Peer_Groups (group_name) VALUES (?)", [req.name])
        group_id = conn.execute("SELECT currval('seq_group_id')").fetchone()[0]
        
        for inst_id in req.institution_ids:
            conn.execute("INSERT INTO Peer_Group_Members (group_id, institution_id) VALUES (?, ?)", [group_id, inst_id])
        
        conn.close()
        return {"status": "success", "id": group_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reports/batch-view")
async def get_batch_view(metric_name: str):
    """Returns a pivoted table for the 2021-2024 batch for a specific metric name."""
    try:
        conn = get_db_connection(read_only=True)
        
        # 1. Look up metric_id
        metric = conn.execute("SELECT metric_id FROM Core_Metrics WHERE standardized_metric_name = ?", [metric_name]).fetchone()
        if not metric:
            conn.close()
            raise HTTPException(status_code=404, detail=f"Metric '{metric_name}' not found in dictionary.")
        
        metric_id = metric[0]
        
        # 2. Get data for 2021-2024
        query = """
            SELECT institution_id, reporting_period, value
            FROM Fact_Financials
            WHERE metric_id = ? AND reporting_period IN ('2021', '2022', '2023', '2024')
        """
        rows = conn.execute(query, [metric_id]).fetchall()
        
        # 3. Pivot data: { institution_id: { year: value } }
        pivot = {}
        for inst_id, year, val in rows:
            if inst_id not in pivot:
                pivot[inst_id] = {"2021": None, "2022": None, "2023": None, "2024": None}
            pivot[inst_id][year] = val
        
        # 4. Format for table: [{ institution: name, "2021": val, ... }]
        result = []
        for inst_id, years in pivot.items():
            row = {"institution": inst_id.replace("_", " ").title()}
            row.update(years)
            result.append(row)
            
        conn.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Static Files & Report Serving
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")

# Serve raw PDF reports
if REPORTS_DIR.exists():
    app.mount("/reports", StaticFiles(directory=str(REPORTS_DIR)), name="reports")

@app.get("/")
async def read_index():
    index_path = APP_DIR / "static" / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="index.html not found in static directory")
    return FileResponse(str(index_path))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
