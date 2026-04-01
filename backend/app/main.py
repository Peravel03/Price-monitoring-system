from fastapi import FastAPI
from sqlalchemy.orm import Session

from .database import engine, Base, SessionLocal
from . import models
from .ingest import run_ingestion_pipeline  # <-- Updated import!

app = FastAPI()

# Create tables
Base.metadata.create_all(bind=engine)

@app.get("/")
def read_root():
    return {"message": "Price Monitoring System Running"}

@app.post("/ingest/")
async def ingest():  # <-- Note the 'async' keyword here
    await run_ingestion_pipeline()  # <-- Calling the new async function
    return {"message": "Async data ingestion pipeline executed successfully"}

@app.get("/test-products/")
def test_products():
    db: Session = SessionLocal()
    products = db.query(models.Product).all()
    db.close() # Added a close() here just to be safe!
    return {"count" : len(products)}