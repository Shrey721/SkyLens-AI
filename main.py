import os
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from groq import Groq
from datetime import datetime, timedelta, timezone
from db import supabase

app = FastAPI()

class QueryRequest(BaseModel):
    question: str

# Initialize Groq Client
groq_api_key = os.getenv("GROQ_API_KEY")
if not groq_api_key:
    print("Warning: GROQ_API_KEY not found in environment.")

groq_client = Groq(api_key=groq_api_key) if groq_api_key else None

SCHEMA_PROMPT = """
You are a PostgreSQL expert. Your task is to convert a natural language question into a PostgreSQL SELECT query based on the following schema:

flights(id, callsign, origin_country, longitude, latitude, velocity, heading, timestamp)
weather(id, airport, condition, temperature, windspeed, timestamp)
airports(id, name, city, country, iata, icao, latitude, longitude)

Rules:
1. Return ONLY the SQL query.
2. NO markdown formatting, NO explanations, NO code blocks. Just the raw SQL string.
3. Only write SELECT queries.
"""

@app.get("/db-test")
async def test_db():
    try:
        response = supabase.table("flights").select("*").execute()
        return response.data
    except Exception as e:
        return {"error": str(e)}

@app.post("/query")
async def query_endpoint(req: QueryRequest):
    if not groq_client:
        return {"question": req.question, "generated_sql": "", "rows": [], "answer": "GROQ_API_KEY is missing."}

    # 1. Generate SQL from Groq
    try:
        prompt = f"{SCHEMA_PROMPT}\nQuestion: {req.question}"
        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}]
        )
        sql_query = response.choices[0].message.content.strip()
        
        # Strip potential markdown code blocks if the LLM didn't listen
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        sql_query = sql_query.strip()

    except Exception as e:
        print(f"Groq SQL generation failed: {e}")
        # Rule-based fallback
        q_lower = req.question.lower()
        if "weather" in q_lower:
            sql_query = "SELECT * FROM weather ORDER BY timestamp DESC LIMIT 5"
        elif "how many" in q_lower or "count" in q_lower:
            sql_query = "SELECT COUNT(*) FROM flights"
        else:
            sql_query = "SELECT * FROM flights LIMIT 5"

    # 2. Validate SQL
    upper_query = sql_query.upper()
    if not upper_query.startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed.")
    
    forbidden_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "EXECUTE", "CREATE"]
    for kw in forbidden_keywords:
        import re
        if re.search(r'\b' + kw + r'\b', upper_query):
            raise HTTPException(status_code=400, detail="Forbidden SQL keyword detected.")

    # 3. Execute SQL
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return {
            "question": req.question,
            "generated_sql": sql_query,
            "rows": [],
            "answer": "DATABASE_URL is not set in the environment."
        }

    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception as e:
        return {
            "question": req.question,
            "generated_sql": sql_query,
            "rows": [],
            "answer": f"Database execution failed: {str(e)}"
        }

    # 4. Generate Answer using Gemini
    try:
        summary_prompt = (
            f"Question: {req.question}\n"
            f"SQL Query Executed: {sql_query}\n"
            f"Result Rows: {rows}\n"
            "Provide a short plain English summary answering the question based on the result rows."
        )
        ans_response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": summary_prompt}]
        )
        answer = ans_response.choices[0].message.content.strip()
    except Exception as e:
        if rows and isinstance(rows, list) and len(rows) == 1 and len(rows[0]) == 1:
            val = list(rows[0].values())[0]
            answer = f"There are {val} matching flights."
        elif rows:
            answer = f"Found {len(rows)} results. Data: {rows}"
        else:
            answer = "No matching flights found."

    return {
        "question": req.question,
        "generated_sql": sql_query,
        "rows": rows,
        "answer": answer
    }

@app.get("/summary/{airport}")
async def get_airport_summary(airport: str):
    airport = airport.upper()
    
    # 1. Query latest weather
    try:
        weather_res = supabase.table("weather").select("*").eq("airport", airport).order("timestamp", desc=True).limit(1).execute()
        weather_data = weather_res.data[0] if weather_res.data else None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch weather: {str(e)}")

    if not weather_data:
        raise HTTPException(status_code=404, detail=f"No weather data found for airport {airport}")
        
    condition = weather_data.get("condition", "Unknown")
    temperature = weather_data.get("temperature", "Unknown")
    weather_str = f"Condition: {condition}, Temp: {temperature}"

    # 2. Count grounded/low-speed flights in the latest hour
    one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    try:
        flights_res = supabase.table("flights").select("id").lte("velocity", 1).gte("timestamp", one_hour_ago).execute()
        grounded_flights = len(flights_res.data)
    except Exception as e:
        print(f"Failed to count grounded flights: {e}")
        grounded_flights = 0

    # 3. Use Gemini to generate a summary
    fallback_summary = f"The weather at {airport} is {condition} with a temperature of {temperature}°C, and there are {grounded_flights} grounded flights recorded in the last hour."
    summary = fallback_summary
    
    if groq_client:
        try:
            prompt = (
                f"Airport: {airport}\\n"
                f"Weather: {condition}, {temperature} degrees\\n"
                f"Grounded flights in the last hour: {grounded_flights}\\n"
                "Write exactly one plain-English sentence summarizing this situation for a dashboard."
            )
            response = groq_client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[{"role": "user", "content": prompt}]
            )
            summary = response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Groq failed to generate summary: {e}")
            summary = fallback_summary

    # 4. Return formatted JSON
    return {
        "airport": airport,
        "weather": weather_str,
        "grounded_flights": grounded_flights,
        "summary": summary
    }

@app.get("/flights-map")
async def get_flights_map():
    try:
        res = supabase.table("flights").select("callsign, origin_country, latitude, longitude, velocity, heading").order("timestamp", desc=True).limit(500).execute()
        
        valid_flights = []
        for flight in res.data:
            if flight.get("latitude") is not None and flight.get("longitude") is not None:
                valid_flights.append(flight)
                
        return valid_flights
    except Exception as e:
        print(f"Error fetching map data: {e}")
        return []

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all for now
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)