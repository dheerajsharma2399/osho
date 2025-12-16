from fastapi import FastAPI
import json

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Welcome to the Osho Discourse API"}

@app.get("/api/discourses/hindi")
def get_hindi_discourses():
    with open("hindi.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    return data
