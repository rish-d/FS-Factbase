import duckdb
from loguru import logger
import json

class FinancialComparisonEngine:
    def __init__(self, db_path="fs_factbase.duckdb"):
        self.db_path = db_path

    def get_connection(self):
        return duckdb.connect(self.db_path, read_only=True)

    def get_rankings(self, metric_id, period="2024", sector=None):
        """Returns a ranked list of institutions for a specific metric and period."""
        conn = self.get_connection()
        query = """
            SELECT 
                f.institution_id, 
                i.name as institution_name,
                f.value, 
                f.currency_code,
                f.is_published
            FROM Fact_Financials f
            JOIN Institutions i ON f.institution_id = i.institution_id
            WHERE f.metric_id = ? 
              AND f.reporting_period = ? 
              AND f.is_published = TRUE
        """
        params = [metric_id, str(period)]
        
        if sector:
            query += " AND i.sector = ?"
            params.append(sector)
            
        query += " ORDER BY f.value DESC"
        
        try:
            results = conn.execute(query, params).fetchall()
            return [
                {"id": r[0], "name": r[1], "value": r[2], "currency": r[3]} 
                for r in results
            ]
        finally:
            conn.close()

    def get_peer_comparison(self, institution_id, metrics, period="2024", group_id=None):
        """Compares one institution against its peers (either sector-wide or a specific group)."""
        conn = self.get_connection()
        
        # 1. Define the peer set
        if group_id:
            peer_query = "SELECT institution_id FROM Peer_Group_Members WHERE group_id = ?"
            peer_ids = [r[0] for r in conn.execute(peer_query, [group_id]).fetchall()]
        else:
            # Default: Same sector
            sector = conn.execute("SELECT sector FROM Institutions WHERE institution_id = ?", [institution_id]).fetchone()
            if not sector: return None
            peer_query = "SELECT institution_id FROM Institutions WHERE sector = ?"
            peer_ids = [r[0] for r in conn.execute(peer_query, [sector[0]]).fetchall()]

        if not peer_ids: return None

        # 2. Get values for each metric for all peers
        comparison_data = {}
        for metric in metrics:
            query = """
                SELECT institution_id, value 
                FROM Fact_Financials 
                WHERE metric_id = ? AND reporting_period = ? AND is_published = TRUE
                AND institution_id IN ({})
            """.format(",".join(["'{}'".format(pid) for pid in peer_ids]))
            
            rows = conn.execute(query, [metric, str(period)]).fetchall()
            vals = {r[0]: r[1] for r in rows}
            
            if not vals: continue
            
            avg_val = sum(vals.values()) / len(vals)
            max_val = max(vals.values())
            
            comparison_data[metric] = {
                "target_value": vals.get(institution_id),
                "peer_average": avg_val,
                "peer_max": max_val,
                "peer_count": len(vals)
            }
            
        conn.close()
        return comparison_data

    def get_time_series_matrix(self, institution_ids, metric_id, start_year=2021, end_year=2025):
        """Returns a matrix of multi-institution data over time."""
        conn = self.get_connection()
        
        years = [str(y) for y in range(start_year, end_year + 1)]
        ids_placeholder = ",".join(["'{}'".format(pid) for pid in institution_ids])
        
        query = """
            SELECT institution_id, reporting_period, value
            FROM Fact_Financials
            WHERE metric_id = ? 
              AND institution_id IN ({})
              AND is_published = TRUE
            ORDER BY reporting_period ASC
        """.format(ids_placeholder)
        
        rows = conn.execute(query, [metric_id]).fetchall()
        
        # Structure as {institution_id: {year: value}}
        matrix = {pid: {y: None for y in years} for pid in institution_ids}
        for r in rows:
            if r[1] in matrix.get(r[0], {}):
                matrix[r[0]][r[1]] = r[2]
                
        conn.close()
        return {"years": years, "data": matrix}
