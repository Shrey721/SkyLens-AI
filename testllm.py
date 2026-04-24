import os
from dotenv import load_dotenv
from pathlib import Path
from google import genai
from openai import OpenAI

# load env
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

# debug print
key = os.getenv("LLM_API_KEY")
print("KEY:", key)

# stop if key not found
if not key:
    raise ValueError("API key not found")

# init client
client = OpenAI.Client(api_key=key)

# test call
response = client.models.generate_content(
    model="gpt-4o-mini",
    contents="what is capital of india"
)

print("RESPONSE:", response.text)