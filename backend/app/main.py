from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "price-monitoring-sys first server is running!"}