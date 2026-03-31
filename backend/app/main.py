from fastapi import FastAPI
from .database import engine, Base
from . import models

app = FastAPI()

# Create tables
Base.metadata.create_all(bind=engine)

@app.get("/")
def read_root():
    return {"message": "Price Monitoring System Running"}