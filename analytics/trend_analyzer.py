import duckdb
from loguru import logger

class TrendAnalyzer:
    def __init__(self, db_path="fs_factbase.duckdb"):
        self.db_path = db_path

    def get_historical_context(self, institution_id, metric_id, current_period):
        """
        Retrieves the value for the same metric/institution from the previous period.
        Assumes periods are numeric strings like '2024'.
        """
        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            try:
                prev_period = str(int(current_period) - 1)
            except ValueError:
                return None

            query = """
                SELECT value FROM Fact_Financials 
                WHERE institution_id = ? AND metric_id = ? AND reporting_period = ?
            """
            result = conn.execute(query, [institution_id, metric_id, prev_period]).fetchone()
            conn.close()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Trend Analysis Error: {e}")
            return None

    def analyze_value(self, institution_id, metric_id, current_period, current_value):
        """
        Analyzes a value against its historical counterpart.
        Returns (confidence_score, reason)
        """
        prev_value = self.get_historical_context(institution_id, metric_id, current_period)
        
        if prev_value is None:
            return 0.8, "No historical data for comparison"

        if prev_value == 0:
            return 0.9, "Previous value was zero, trend indeterminate"

        # Calculate YoY change
        yoy_change = abs((current_value - prev_value) / prev_value)
        
        if yoy_change > 0.3: # Flag if > 30% change
            return 0.4, f"Statistical Anomaly: {yoy_change:.1%} variance from {current_period} vs {int(current_period)-1}"
        
        if yoy_change > 0.15: # Warning if > 15% change
            return 0.7, f"Significant Trend: {yoy_change:.1%} growth"
            
        return 1.0, f"Stable Trend: {yoy_change:.1%} variance"

if __name__ == "__main__":
    # Test
    analyzer = TrendAnalyzer()
    score, reason = analyzer.analyze_value("cimb_group_holdings_berhad", "total_assets", "2025", 60000000.0)
    print(f"Score: {score}, Reason: {reason}")
