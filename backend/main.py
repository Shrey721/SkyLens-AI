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
        return {"question": req.question, "generated_sql": "", "rows": [], "answer": "GROQ_API_KEY is missing.", "metrics": {"total_flights": 0, "avg_velocity": 0, "active_countries": 0, "risk_level": "Unknown"}}

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
            "answer": "DATABASE_URL is not set in the environment.",
            "metrics": {"total_flights": 0, "avg_velocity": 0, "active_countries": 0, "risk_level": "Unknown"}
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
            "answer": f"Database execution failed: {str(e)}",
            "metrics": {"total_flights": 0, "avg_velocity": 0, "active_countries": 0, "risk_level": "Unknown"}
        }

    # 4. Generate Answer using Groq (JSON)
    import json
    try:
        summary_prompt = (
            f"Question: {req.question}\n"
            f"SQL Query Executed: {sql_query}\n"
            f"Result Rows: {rows}\n"
            "Based on the results, provide a structured JSON response. Do NOT use markdown code blocks, just return raw JSON. "
            "The JSON must have exactly these 4 string keys: "
            "'summary' (short answer), "
            "'key_insight' (one interesting observation), "
            "'risk_level' (Low, Medium, or High), "
            "'recommendation' (one actionable advice)."
        )
        ans_response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": summary_prompt}]
        )
        answer = ans_response.choices[0].message.content.strip()
        # Clean up any markdown blocks around JSON
        if answer.startswith("```json"): answer = answer[7:]
        if answer.startswith("```"): answer = answer[3:]
        if answer.endswith("```"): answer = answer[:-3]
        answer = answer.strip()
    except Exception as e:
        if rows and isinstance(rows, list) and len(rows) == 1 and len(rows[0]) == 1:
            val = list(rows[0].values())[0]
            answer = json.dumps({"summary": f"There are {val} matching records.", "key_insight": "Specific count retrieved.", "risk_level": "Low", "recommendation": "Monitor trends."})
        elif rows:
            answer = json.dumps({"summary": f"Found {len(rows)} results.", "key_insight": "Multiple records found.", "risk_level": "Medium", "recommendation": "Analyze the detailed rows for more insights."})
        else:
            answer = json.dumps({"summary": "No matching records found.", "key_insight": "The query yielded empty results.", "risk_level": "Low", "recommendation": "Try broadening your search criteria."})

    # 5. Compute Metrics
    total_flights = 0
    avg_velocity = 0
    active_countries = 0
    risk_level = "Low"
    
    if rows and isinstance(rows, list):
        if len(rows) == 1 and len(rows[0]) == 1 and str(list(rows[0].values())[0]).isdigit():
            total_flights = int(list(rows[0].values())[0])
        else:
            total_flights = len(rows)
            velocities = [r.get("velocity") for r in rows if isinstance(r, dict) and r.get("velocity") is not None]
            if velocities:
                avg_velocity = int(sum(velocities) / len(velocities))
                low_v = sum(1 for v in velocities if v < 50)
                if low_v > len(velocities) * 0.5:
                    risk_level = "High"
                elif low_v > 0:
                    risk_level = "Medium"
            
            countries = set(r.get("origin_country") for r in rows if isinstance(r, dict) and r.get("origin_country"))
            active_countries = len(countries)

    metrics = {
        "total_flights": total_flights,
        "avg_velocity": avg_velocity,
        "active_countries": active_countries,
        "risk_level": risk_level
    }

    return {
        "question": req.question,
        "generated_sql": sql_query,
        "rows": rows,
        "answer": answer,
        "metrics": metrics
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

from typing import Optional

@app.get("/flights-map")
async def get_flights_map(
    country: Optional[str] = None,
    min_velocity: Optional[float] = None,
    max_velocity: Optional[float] = None,
    time_range: Optional[int] = None
):
    try:
        query = supabase.table("flights").select("id, callsign, origin_country, latitude, longitude, velocity, heading").order("timestamp", desc=True)
        
        if time_range:
            time_threshold = (datetime.now(timezone.utc) - timedelta(hours=time_range)).isoformat()
            query = query.gte("timestamp", time_threshold)
            
        res = query.limit(1500).execute()
        
        valid_flights = []
        for flight in res.data:
            if flight.get("latitude") is not None and flight.get("longitude") is not None:
                # Local filtering
                if country:
                    origin = flight.get("origin_country") or ""
                    if origin.lower() != country.lower():
                        continue
                v = flight.get("velocity") or 0
                if min_velocity is not None and v < min_velocity:
                    continue
                if max_velocity is not None and v > max_velocity:
                    continue
                    
                valid_flights.append(flight)
                
        return valid_flights[:500]
    except Exception as e:
        print(f"Error fetching map data: {e}")
        return []

@app.get("/kpi")
async def get_kpi():
    try:
        res = supabase.table("flights").select("velocity, origin_country").execute()
        flights = res.data
        total_flights = len(flights)
        
        total_velocity = sum(f.get("velocity", 0) or 0 for f in flights)
        avg_velocity = int(total_velocity / total_flights) if total_flights > 0 else 0
        
        countries = set(f.get("origin_country") for f in flights if f.get("origin_country"))
        active_countries = len(countries)
        
        weather_res = supabase.table("weather").select("condition").order("timestamp", desc=True).limit(50).execute()
        severe_conditions = ["Rain", "Snow", "Storm", "Thunderstorm", "Fog", "Heavy Rain", "Freezing Drizzle"]
        risk_level = "Low"
        for w in weather_res.data:
            if w.get("condition") in severe_conditions:
                risk_level = "High"
                break
                
        return {
            "total_flights": total_flights,
            "avg_velocity": avg_velocity,
            "active_countries": active_countries,
            "risk_level": risk_level
        }
    except Exception as e:
        return {"total_flights": 0, "avg_velocity": 0, "active_countries": 0, "risk_level": "Unknown"}

@app.get("/alerts")
async def get_alerts():
    alerts = []
    try:
        one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        low_speed_res = supabase.table("flights").select("id").lte("velocity", 5).gte("timestamp", one_hour_ago).execute()
        if len(low_speed_res.data) > 20:
            alerts.append(f"High Ground Congestion: {len(low_speed_res.data)} aircraft detected with very low velocity in the last hour.")
            
        weather_res = supabase.table("weather").select("airport, condition").order("timestamp", desc=True).limit(10).execute()
        severe_conditions = ["Rain", "Snow", "Storm", "Thunderstorm", "Heavy Rain"]
        severe_airports = set(w.get("airport") for w in weather_res.data if w.get("condition") in severe_conditions)
                
        if severe_airports:
            alerts.append(f"Severe Weather Disruptions at: {', '.join(severe_airports)}")
            
    except Exception as e:
        print(f"Alerts Error: {e}")
        
    return alerts

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all for now
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)