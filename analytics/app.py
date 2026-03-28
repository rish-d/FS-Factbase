from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import duckdb
import os

app = FastAPI(title="FS Factbase Dashboard")

# Database Path
DB_PATH = os.path.join(os.getcwd(), "fs_factbase.duckdb")

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
        subprocess.Popen([python_exe, "scripts/run_diagnostics.py"])
        
        return {"status": "started", "message": "Batch diagnostic learning process initiated."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Static Files
app.mount("/static", StaticFiles(directory="analytics/static"), name="static")

@app.get("/")
async def read_index():
    return FileResponse("analytics/static/index.html")
