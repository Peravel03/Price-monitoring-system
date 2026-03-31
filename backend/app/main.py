from fastapi import FastAPI

from .database import engine, Base
from . import models
from .ingest import ingest_data
from .database import SessionLocal
from sqlalchemy.orm import Session

app = FastAPI()


# Create tables
Base.metadata.create_all(bind=engine)

@app.get("/")
def read_root():
    return {"message": "Price Monitoring System Running"}



@app.post("/ingest/")
def ingest():
    ingest_data() #update path
    return {"message": "Data ingested"}

@app.get("/test-products/")
def test_products():
    db: Session = SessionLocal()
    products = db.query(models.Product).all()
    return {"count" : len(products)}