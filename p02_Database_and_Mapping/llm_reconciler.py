import os
import json
import duckdb
from loguru import logger
import google.generativeai as genai
import db_config

class LLMReconciler:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            logger.error("Neither GOOGLE_API_KEY nor GEMINI_API_KEY found in environment.")
            raise ValueError("API key required for LLMReconciler")
        
        genai.configure(api_key=self.api_key)
        # We use a smaller model for efficiency unless complexity warrants more
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        self.db_path = db_config.get_db_path()

    def reconcile_batch(self, terms_with_context):
        """
        Reconciles a batch of unmapped terms.
        terms_with_context: List of dicts [{"raw_term": "...", "statement_type": "..."}]
        """
        if not terms_with_context:
            return []

        # Group by statement type to provide relevant IFRS context
        statements = set(t['statement_type'] for t in terms_with_context if t.get('statement_type'))
        
        # Load IFRS context for these statements
        ifrs_dictionary = self._load_ifrs_dictionary(statements)
        
        prompt = self._build_prompt(terms_with_context, ifrs_dictionary)
        
        try:
            response = self.model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    response_mime_type="application/json",
                    temperature=0.1
                )
            )
            
            results = json.loads(response.text)
            # results should be a list of {"raw_term": "...", "ifrs_concept_id": "...", "confidence": 0.X}
            return results
        except Exception as e:
            logger.error(f"LLM Reconciliation failed: {e}")
            return []

    def _load_ifrs_dictionary(self, statement_types):
        """Fetches relevant IFRS concepts from DB to act as LLM context."""
        conn = duckdb.connect(self.db_path)
        
        # Map statement_type to IFRS roles (simplified)
        role_map = {
            "balance sheet": "210000",
            "income statement": "310000",
            "cash flow": "610000",
            "equity": "510000"
        }
        
        roles = []
        for st in statement_types:
            for key, role in role_map.items():
                if st and key in st.lower():
                    roles.append(role)
        
        if not roles:
            roles = ["210000", "310000"] # Default to core
            
        role_str = "', '".join(roles)
        query = f"SELECT metric_id, standardized_metric_name FROM Core_Metrics WHERE statement_role IN ('{role_str}', 'universal')"
        
        rows = conn.execute(query).fetchall()
        conn.close()
        
        # Format for prompt: "ID: Label"
        return [f"{r[0]}: {r[1]}" for r in rows]

    def _build_prompt(self, terms, dictionary):
        dict_txt = "\n".join(dictionary[:200]) # Limit context to top 200 relevant items
        terms_txt = "\n".join([f"- {t['raw_term']} (Context: {t.get('statement_type', 'Unknown')})" for t in terms])
        
        return f"""
You are an expert IFRS accounting reconciler. Your task is to map raw financial terms extracted from bank annual reports to the most appropriate official IFRS concept ID.

IFRS DICTIONARY (CANDIDATES):
{dict_txt}

RAW TERMS TO MAP:
{terms_txt}

INSTRUCTIONS:
1. For each term, identify the single best matching IFRS concept ID.
2. If the term is too bespoke or ambiguous to map clearly, return "UNMAPPED" for that ID.
3. Provide a confidence score from 0.0 to 1.0.
4. Return the result in a JSON list format.

OUTPUT FORMAT:
[
  {{"raw_term": "term from list", "ifrs_concept_id": "ID from dictionary or UNMAPPED", "confidence": 0.95}}
]
"""

if __name__ == "__main__":
    reconciler = LLMReconciler()
    test_batch = [{"raw_term": "Cash at Bank", "statement_type": "Balance Sheet"}]
    print(reconciler.reconcile_batch(test_batch))
