import os
import requests
import csv
from datetime import datetime, timezone
from db import supabase

def fetch_flights():
    """
    Fetches flight data from OpenSky as primary.
    Falls back to Aviationstack if OpenSky fails.
    """
    print("Fetching flight data...")
    try:
        # Primary: OpenSky
        url = "https://opensky-network.org/api/states/all"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        flights = []
        # Parse OpenSky data - limiting to 100 to avoid huge payload for now
        for state in data.get("states", [])[:100]:
            timestamp = state[3] or state[4]
            iso_time = datetime.fromtimestamp(timestamp, timezone.utc).isoformat() if timestamp else datetime.now(timezone.utc).isoformat()
            
            flight = {
                "callsign": str(state[1]).strip() if state[1] else "UNKNOWN",
                "origin_country": state[2],
                "longitude": state[5],
                "latitude": state[6],
                "velocity": state[9],
                "heading": state[10],
                "timestamp": iso_time
            }
            # Only insert if coordinates exist
            if flight["longitude"] is not None and flight["latitude"] is not None:
                flights.append(flight)
                
        print(f"Source used: OpenSky (fetched {len(flights)} records)")
        return flights
    except Exception as e:
        print(f"OpenSky API failed: {e}. Falling back to Aviationstack...")
        
        # Fallback: Aviationstack
        api_key = os.getenv("AVIATIONSTACK_API_KEY")
        if not api_key:
            print("Error: AVIATIONSTACK_API_KEY is not set in environment variables.")
            return []
            
        try:
            url = f"http://api.aviationstack.com/v1/flights?access_key={api_key}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            flights = []
            for flight_data in data.get("data", []):
                live = flight_data.get("live")
                if live and not live.get("is_ground"):
                    flight = {
                        "callsign": flight_data.get("flight", {}).get("iata") or "UNKNOWN",
                        "origin_country": "UNKNOWN", # Aviationstack doesn't provide origin country explicitly at top level
                        "longitude": live.get("longitude"),
                        "latitude": live.get("latitude"),
                        "velocity": live.get("speed_horizontal"),
                        "heading": live.get("direction"),
                        "timestamp": live.get("updated") or datetime.now(timezone.utc).isoformat()
                    }
                    if flight["longitude"] is not None and flight["latitude"] is not None:
                        flights.append(flight)
            
            print(f"Source used: Aviationstack (fetched {len(flights)} records)")
            return flights
        except Exception as fallback_e:
            print(f"Error fetching from Aviationstack fallback: {fallback_e}")
            return []

def fetch_weather(flights_data):
    """
    Fetches weather data from Open-Meteo for unique airport locations based on flight origin countries.
    """
    print("Fetching weather data based on flight origins...")
    
    # 1. Extract unique origin countries from flights
    unique_countries = set()
    for f in flights_data:
        country = f.get("origin_country")
        if country and country != "UNKNOWN":
            unique_countries.add(country)
            
    # 2. Map countries to an airport coordinate using the OpenFlights dataset
    country_to_airport = {}
    try:
        url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        lines = response.text.strip().split("\n")
        reader = csv.reader(lines)
        for row in reader:
            if len(row) > 7:
                try:
                    c = row[3]
                    if c not in country_to_airport:
                        # Use IATA if available, else use the airport name
                        iata = row[4] if row[4] and row[4] != "\\N" else row[1]
                        country_to_airport[c] = {"code": iata, "lat": float(row[6]), "lon": float(row[7])}
                except ValueError:
                    pass
    except Exception as e:
        print(f"Failed to fetch airport mappings for weather: {e}")
        
    # 3. Select airports for the unique countries
    target_airports = []
    for c in unique_countries:
        if c in country_to_airport:
            target_airports.append(country_to_airport[c])
            
    # Limit to 20-30 airports to avoid API rate limits and performance issues
    target_airports = target_airports[:25]
    print(f"Mapped flights to {len(target_airports)} unique airport locations.")
    
    weather_data = []
    for airport in target_airports:
        code = airport["code"]
        try:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={airport['lat']}&longitude={airport['lon']}&current=temperature_2m,wind_speed_10m,weather_code"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            current = data.get("current", {})
            weather = {
                "airport": code,
                "temperature": current.get("temperature_2m"),
                "windspeed": current.get("wind_speed_10m"),
                "condition": str(current.get("weather_code")),
                "timestamp": current.get("time") or datetime.now(timezone.utc).isoformat()
            }
            weather_data.append(weather)
        except Exception as e:
            print(f"Error fetching weather for {code}: {e}")
            
    print(f"Fetched weather for {len(weather_data)} airports.")
    return weather_data

def fetch_airports():
    """
    Optional secondary source: Load OpenFlights airports dataset.
    """
    print("Fetching optional airports dataset...")
    try:
        url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Parse CSV
        lines = response.text.strip().split("\n")
        reader = csv.reader(lines)
        airports = []
        for row in reader:
            if len(row) > 7:
                try:
                    airports.append({
                        "name": row[1],
                        "city": row[2],
                        "country": row[3],
                        "iata": row[4],
                        "icao": row[5],
                        "latitude": float(row[6]) if row[6] else None,
                        "longitude": float(row[7]) if row[7] else None
                    })
                except ValueError:
                    pass # Skip rows with invalid float conversions
        
        # Limit to a smaller subset for demonstration
        print(f"Fetched {len(airports)} airports from secondary dataset. Limiting insertion to 50 records.")
        return airports[:50]
    except Exception as e:
        print(f"Error fetching airports dataset: {e}")
        return []

def insert_data(table_name, data):
    """
    Inserts data into Supabase and prints success/errors.
    """
    if not data:
        print(f"No data to insert for {table_name}.")
        return

    try:
        response = supabase.table(table_name).insert(data).execute()
        # Ensure we catch potential issues gracefully.
        print(f"Successfully inserted {len(response.data)} records into '{table_name}'.")
    except Exception as e:
        print(f"Error inserting into '{table_name}': {str(e)}")

if __name__ == "__main__":
    print("Starting data ingestion process...")
    
    flights = fetch_flights()
    insert_data("flights", flights)
    
    weather = fetch_weather(flights)
    insert_data("weather", weather)
    
    airports = fetch_airports()
    if airports:
        insert_data("airports", airports)
        
    print("Data ingestion process complete.")
