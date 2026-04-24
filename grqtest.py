import os
from pathlib import Path
from dotenv import load_dotenv
from groq import Groq

env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

key = os.getenv("GROQ_API_KEY")
print("KEY FOUND:", key is not None)

client = Groq(api_key=key)

response = client.chat.completions.create(
    model="llama-3.1-8b-instant",
    messages=[
        {"role": "user", "content": "Say hello in one sentence"}
    ],
)

print(response.choices[0].message.content)