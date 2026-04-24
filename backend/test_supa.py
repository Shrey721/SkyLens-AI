import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

print(os.getenv("SUPABASE_URL"))
print(os.getenv("SUPABASE_ANON_KEY")[:30])
print(len(os.getenv("SUPABASE_ANON_KEY")))