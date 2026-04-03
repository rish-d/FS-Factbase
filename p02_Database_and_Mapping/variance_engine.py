import duckdb
import os
from loguru import logger
import db_config

class VarianceEngine:
    def __init__(self, db_path=None):
        if db_path is None:
            self.db_path = db_config.get_db_path()
        else:
            self.db_path = db_path
        self.threshold = 0.01  # 1% Variance Threshold

    def run_audit(self):
        logger.info(f"Starting Variance Audit on {self.db_path}...")
        conn = duckdb.connect(self.db_path)

        # 1. Get Hierarchy
        hierarchy = conn.execute("SELECT parent_metric_id, child_metric_id, weight FROM Metric_Hierarchy").fetchall()
        parents = sorted(list(set([h[0] for h in hierarchy])))

        if not parents:
            logger.warning("No hierarchical relationships found in Metric_Hierarchy table.")
            conn.close()
            return

        audit_results = []

        # 2. Audit each Parent per Institution/Period
        for parent_id in parents:
            children = [h for h in hierarchy if h[0] == parent_id]
            
            # Simple SQL to compare parent vs sum of children
            # We use a JOIN to find cases where both parent and at least one child exist
            query = f"""
                WITH ParentValue AS (
                    SELECT institution_id, reporting_period, value as parent_val
                    FROM Fact_Financials
                    WHERE metric_id = '{parent_id}'
                ),
                ChildrenSum AS (
                    SELECT f.institution_id, f.reporting_period, SUM(f.value * h.weight) as children_sum
                    FROM Fact_Financials f
                    JOIN Metric_Hierarchy h ON f.metric_id = h.child_metric_id
                    WHERE h.parent_metric_id = '{parent_id}'
                    GROUP BY f.institution_id, f.reporting_period
                )
                SELECT p.institution_id, p.reporting_period, p.parent_val, c.children_sum
                FROM ParentValue p
                JOIN ChildrenSum c ON p.institution_id = c.institution_id AND p.reporting_period = c.reporting_period
            """
            
            results = conn.execute(query).fetchall()
            
            for inst, period, p_val, c_sum in results:
                variance = p_val - c_sum
                var_pct = abs(variance / p_val) if p_val != 0 else 0
                
                status = "PASS" if var_pct <= self.threshold else "FAIL"
                
                audit_results.append({
                    "parent": parent_id,
                    "institution": inst,
                    "period": period,
                    "reported": p_val,
                    "calculated": c_sum,
                    "variance": variance,
                    "status": status
                })

        conn.close()
        self.report_results(audit_results)

    def report_results(self, results):
        if not results:
            logger.info("Audit completed: No comparable parent-child data found.")
            return

        passes = [r for r in results if r['status'] == "PASS"]
        fails = [r for r in results if r['status'] == "FAIL"]

        logger.info(f"Audit Summary: {len(results)} checks performed. {len(passes)} Passed, {len(fails)} Failed.")

        if fails:
            logger.warning("!!! AUDIT FAILURES DETECTED !!!")
            for f in fails:
                logger.error(f"FAIL: {f['institution']} ({f['period']}) - {f['parent']}: Reported {f['reported']}, Calced {f['calculated']} (Var: {f['variance']})")
        else:
            logger.success("All additive audits passed successfully.")

if __name__ == "__main__":
    engine = VarianceEngine()
    engine.run_audit()
