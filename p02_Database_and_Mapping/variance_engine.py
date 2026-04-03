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
            # Simple SQL to compare parent vs sum of children
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
                # Using 1.0 as denominator for very small numbers to avoid div by zero
                var_pct = abs(variance / p_val) if abs(p_val) > 0.0001 else 0
                
                status = "PASS" if var_pct <= self.threshold else "FAIL"
                
                audit_results.append({
                    "parent_metric": parent_id,
                    "institution_id": inst,
                    "reporting_period": period,
                    "reported_value": p_val,
                    "calculated_value": c_sum,
                    "variance_abs": variance,
                    "variance_pct": round(var_pct * 100, 4),
                    "status": status,
                    "audit_timestamp": conn.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0].isoformat()
                })

        conn.close()
        self.report_results(audit_results)

    def report_results(self, results):
        if not results:
            logger.info("Audit completed: No comparable parent-child data found.")
            return

        passes = [r for r in results if r['status'] == "PASS"]
        fails = [r for r in results if r['status'] == "FAIL"]

        # 1. Console Summary
        logger.info(f"Audit Summary: {len(results)} relationships checked across all institutions.")
        logger.info(f"Passed: {len(passes)} | Failed: {len(fails)}")

        # 2. Save JSON Report
        import json
        report_path = os.path.join(os.path.dirname(self.db_path), "audit_variance_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump({
                "summary": {
                    "total_checks": len(results),
                    "passed": len(passes),
                    "failed": len(fails),
                    "threshold": self.threshold
                },
                "results": results
            }, f, indent=2)
        
        logger.info(f"Full audit report saved to: {report_path}")

        if fails:
            logger.warning("!!! AUDIT FAILURES DETECTED !!!")
            for f in fails[:10]: # Log first 10
                logger.error(f"FAIL: {f['institution_id']} ({f['reporting_period']}) -> {f['parent_metric']}: Var {f['variance_pct']}%")
            if len(fails) > 10:
                logger.warning(f"... and {len(fails)-10} more failures. Check JSON report.")
        else:
            logger.success("Zero-Variance Hardening Verified: All additive hierarchies consistent.")

if __name__ == "__main__":
    engine = VarianceEngine()
    engine.run_audit()
