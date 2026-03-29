import os
import re
import time
import json
import duckdb
from loguru import logger
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Tuple

from google import genai
from google.genai import types

class SqlResponse(BaseModel):
    sql_query: str = Field(description="A perfectly valid, read-only DuckDB SQL query string.")

class FinancialSQLAgent:
    def __init__(self, db_path="fs_factbase.duckdb"):
        self.db_path = db_path
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            logger.warning("No API key found. SQL Agent will fail.")
        
        self.client = genai.Client(api_key=self.api_key, http_options={'api_version': 'v1'})
        # Start with the fastest/cheapest model, fallback if needed
        self.models = ['gemini-1.5-flash-8b', 'gemini-1.5-flash', 'gemini-pro']
        
        # Pre-fetch schema
        self.schema_context = self._get_db_schema()

    def _get_db_schema(self) -> str:
        """Fetches the schema of key tables to inform the LLM."""
        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            schema_str = "Database Schema:\n\n"
            
            tables = ["Fact_Financials", "Core_Metrics"]
            for table in tables:
                schema_str += f"Table: {table}\nColumns: "
                cols = conn.execute(f"PRAGMA show('{table}')").fetchall()
                cols_info = [f"{c[0]} ({c[1]})" for c in cols]
                schema_str += ", ".join(cols_info) + "\n\n"
                
            # Specifically for Core_Metrics, grab some standard metric names to help the LLM
            names = conn.execute("SELECT DISTINCT standardized_metric_name FROM Core_Metrics LIMIT 50").fetchall()
            names_list = [n[0] for n in names]
            schema_str += f"Sample standardized metric names available to query:\n{', '.join(names_list)}\n"
            
            conn.close()
            return schema_str
        except Exception as e:
            logger.error(f"Failed to fetch schema: {e}")
            return "Failed to load schema."

    def _call_llm(self, prompt: str, system_instruction: str, expected_schema: Any = None) -> str:
        """Helper to call Gemini with retries and fallbacks."""
        for model_slug in self.models:
            for attempt in range(2): # 2 attempts per model (for 429 quota primarily)
                try:
                    config_args = {"system_instruction": system_instruction}
                    if expected_schema:
                        config_args["response_mime_type"] = "application/json"
                        config_args["response_schema"] = expected_schema
                        
                    response = self.client.models.generate_content(
                        model=model_slug,
                        contents=prompt,
                        config=types.GenerateContentConfig(**config_args)
                    )
                    
                    if response and response.text:
                        return response.text
                except Exception as e:
                    if "429" in str(e):
                        logger.warning(f"Quota error with {model_slug}. Sleeping 5s before retry...")
                        time.sleep(5)
                    else:
                        logger.warning(f"Model {model_slug} failed: {e}")
                        break # Break inner loop to try next model
        return ""

    def generate_sql(self, user_question: str, error_feedback: str = None) -> str:
        """Generates a DuckDB SQL query from a natural language question."""
        sys_prompt = (
            "You are a master DuckDB SQL Database Administrator. You write highly accurate, read-only analytical queries. "
            "You always join Fact_Financials with Core_Metrics on metric_id to get the 'standardized_metric_name' when needed. "
            "Remember that institution_id is usually snake_case (e.g., 'cimb_group_holdings_berhad')."
        )
        
        prompt = f"{self.schema_context}\n\nUser Question: {user_question}"
        
        if error_feedback:
            prompt += f"\n\nYour previous SQL query failed with this error: {error_feedback}\n"
            prompt += "Please provide a corrected SQL query that resolves the error."

        logger.info(f"Generating SQL for: {user_question} (Retry: {bool(error_feedback)})")
        
        # We ask for structured JSON to strictly get the SQL string
        res = self._call_llm(prompt, sys_prompt, expected_schema=SqlResponse)
        if res:
            try:
                data = json.loads(res)
                return data.get('sql_query', '').strip()
            except Exception as e:
                logger.error(f"Failed to parse LLM JSON: {e}")
                # Fallback purely regex if json parsing fails
                match = re.search(r'```sql\n(.*?)\n```', res, re.DOTALL)
                if match: return match.group(1).strip()
                return res.strip()
        return ""

    def execute_sql(self, sql: str) -> Tuple[List[Dict[str, Any]], str]:
        """Executes a SQL query and returns results and any error message."""
        if not sql:
            return [], "No SQL query generated."
            
        # Hard block against destructive queries
        upper_sql = sql.upper()
        if any(bad_word in upper_sql for bad_word in ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE"]):
            return [], "Unsafe SQL operation blocked."

        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            # Fetch results as a list of dictionaries for ease of use
            result_df = conn.execute(sql).df()
            results = result_df.to_dict(orient="records")
            conn.close()
            return results, None
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            return [], str(e)

    def generate_summary(self, user_question: str, sql: str, results: List[Dict[str, Any]]) -> str:
        """Generates a natural language summary of the query results."""
        sys_prompt = "You are a helpful Financial AI Assistant. Keep your answers concise, accurate, and conversational."
        
        # Truncate results if massive
        if len(results) > 20:
            preview = results[:20]
            context_results = f"{json.dumps(preview, indent=2)}\n...(Truncated {len(results)-20} more rows)..."
        else:
            context_results = json.dumps(results, indent=2)

        prompt = f"""
        User Question: {user_question}
        
        I executed the following SQL query to find the answer:
        {sql}
        
        Here are the data results:
        {context_results}
        
        Please provide a concise, friendly natural language answer to the user's question based strictly on this data. 
        If the data is empty or irrelevant, politely inform the user that the information couldn't be found.
        """
        
        summary = self._call_llm(prompt, sys_prompt)
        return summary or "I couldn't generate a summary of the results."

    def process_query(self, user_question: str) -> Dict[str, Any]:
        """Main pipeline: Question -> SQL -> Correct(if needed) -> Execute -> Summarize."""
        # Step 1: Generate SQL
        sql = self.generate_sql(user_question)
        
        if not sql:
            return {"sql": "", "data": [], "answer": "I'm sorry, I was unable to generate a valid SQL query for your request."}

        # Step 2: Execute
        results, error = self.execute_sql(sql)
        
        # Step 3: Self-Correction (1 Retry)
        if error:
            logger.warning("Applying single self-correction retry due to SQL error.")
            sql = self.generate_sql(user_question, error_feedback=error)
            results, error = self.execute_sql(sql)
            
            if error:
                return {
                    "sql": sql, 
                    "data": [], 
                    "answer": f"I hit a database error I couldn't automatically resolve: {error}"
                }

        # Step 4: Summarize
        answer = self.generate_summary(user_question, sql, results)
        
        return {
            "sql": sql,
            "data": results,
            "answer": answer
        }

if __name__ == "__main__":
    agent = FinancialSQLAgent()
    res = agent.process_query("What were CIMB's total assets in 2024?")
    print(json.dumps(res, indent=2))
