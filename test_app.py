"""최소 FastAPI 앱 — asyncpg 없이 동작 확인용"""
from fastapi import FastAPI
import os

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok", "port": os.environ.get("PORT", "not set")}

@app.get("/health")
def health():
    return {"status": "ok"}
