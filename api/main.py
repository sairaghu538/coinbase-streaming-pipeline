"""
Crypto Dashboard API - FastAPI Application

Run with:
    uvicorn api.main:app --reload --port 8000

Then visit:
    http://localhost:8000/docs   (Swagger UI)
    http://localhost:8000/api/products
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import router

app = FastAPI(
    title="Crypto Dashboard API",
    description="REST API serving Gold layer data from the Coinbase Streaming Pipeline",
    version="1.0.0",
)

# Allow React dev server to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",   # Vite dev server
        "http://localhost:3000",   # Alternate port
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")


@app.get("/")
def root():
    return {
        "message": "Crypto Dashboard API",
        "docs": "/docs",
        "endpoints": [
            "/api/products",
            "/api/ohlc?product_id=BTC-USD&interval=1m",
            "/api/kpis",
            "/api/top-movers",
            "/api/status",
        ],
    }
