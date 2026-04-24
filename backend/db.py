import os
from dotenv import load_dotenv
from supabase import create_client, Client
from pathlib import Path

# Load environment variables
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

url: str | None = os.getenv("SUPABASE_URL")
key: str | None = os.getenv("SUPABASE_ANON_KEY")

if not url or not key:
    raise ValueError("Supabase URL or Key not found in environment variables")

supabase: Client = create_client(url, key)
