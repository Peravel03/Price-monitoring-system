from fastapi import FastAPI
from .database import engine, Base
from . import models
from .ingest import ingest_data

app = FastAPI()


# Create tables
Base.metadata.create_all(bind=engine)

@app.get("/")
def read_root():
    return {"message": "Price Monitoring System Running"}



@app.post("/ingest/")
def ingest():
    ingest_data("data/sample_data.json") #update path
    return {"message": "Data ingested"}