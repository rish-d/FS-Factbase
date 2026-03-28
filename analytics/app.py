from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import duckdb
import os

app = FastAPI(title="FS Factbase Dashboard")

# Database Path
DB_PATH = os.path.join(os.getcwd(), "fs_factbase.duckdb")

def get_db_connection():
    return duckdb.connect(DB_PATH, read_only=True)

@app.get("/api/metrics")
async def get_metrics():
    try:
        conn = get_db_connection()
        rows = conn.execute("SELECT * FROM Core_Metrics").fetchall()
        conn.close()
        return [{"metric_id": r[0], "name": r[1], "standard": r[2], "type": r[3]} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/facts")
async def get_facts():
    try:
        conn = get_db_connection()
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
                "metric_name": r[7]
            } for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/unmapped")
async def get_unmapped():
    try:
        conn = get_db_connection()
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
                "page": r[6]
            } for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Static Files
app.mount("/static", StaticFiles(directory="analytics/static"), name="static")

@app.get("/")
async def read_index():
    return FileResponse("analytics/static/index.html")
