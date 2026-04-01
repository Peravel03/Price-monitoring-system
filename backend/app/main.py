from fastapi import FastAPI, Depends, HTTPException, Security, status, Query
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func
from typing import List, Optional

from .database import engine, Base, SessionLocal
from . import models, schemas
from .ingest import run_ingestion_pipeline

#imports for front end
from fastapi import FastAPI, Depends, HTTPException, Security, status, Query
from fastapi.middleware.cors import CORSMiddleware
app = FastAPI(title="Entrupy Price Monitoring API")

# --- NEW CORS SETUP ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins for local development
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers (like our X-API-Key)
)
# ----------------------

# Create tables
Base.metadata.create_all(bind=engine)


# --- AUTHENTICATION & USAGE TRACKING ---
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

# Mock database for valid keys and tracking usage
VALID_API_KEYS = {
    "entrupy-intern-2026": {"user": "grader", "usage_count": 0}
}

def get_api_key(api_key: str = Security(api_key_header)):
    if api_key in VALID_API_KEYS:
        # Track usage per request
        VALID_API_KEYS[api_key]["usage_count"] += 1
        print(f"User {VALID_API_KEYS[api_key]['user']} made a request. Total: {VALID_API_KEYS[api_key]['usage_count']}")
        return api_key
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API Key. Please provide 'X-API-Key' in headers.",
    )

# --- DATABASE DEPENDENCY ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- ENDPOINTS ---

@app.get("/")
def read_root():
    return {"message": "Price Monitoring System Running. Check /docs for API documentation."}

@app.post("/ingest/", tags=["Admin"])
async def ingest():
    """Trigger a manual data refresh from marketplaces."""
    await run_ingestion_pipeline()
    return {"message": "Async data ingestion pipeline executed successfully"}

@app.get("/products/", response_model=List[schemas.ProductOut], tags=["Consumer"])
def browse_products(
    category: Optional[str] = Query(None, description="Filter by product category"),
    min_price: Optional[float] = Query(None, ge=0, description="Minimum current price"),
    max_price: Optional[float] = Query(None, ge=0, description="Maximum current price"),
    db: Session = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """Browse and filter products."""
    query = db.query(models.Product)
    
    # Apply category filter
    if category:
        query = query.filter(models.Product.category.ilike(f"%{category}%"))
        
    # Apply price filters (requires joining the listings table)
    if min_price is not None or max_price is not None:
        query = query.join(models.Listing)
        if min_price is not None:
            query = query.filter(models.Listing.current_price >= min_price)
        if max_price is not None:
            query = query.filter(models.Listing.current_price <= max_price)

    # joinedload prevents the "N+1 query" problem by fetching related data in one big query
    query = query.options(
        joinedload(models.Product.listings).joinedload(models.Listing.price_history)
    )
    
    return query.all()

@app.get("/products/{product_id}", response_model=schemas.ProductOut, tags=["Consumer"])
def get_single_product(
    product_id: int, 
    db: Session = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """View a single product's details and price history."""
    product = db.query(models.Product).options(
        joinedload(models.Product.listings).joinedload(models.Listing.price_history)
    ).filter(models.Product.id == product_id).first()
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
        
    return product

@app.get("/analytics/", response_model=schemas.AggregateAnalyticsOut, tags=["Consumer"])
def get_analytics(
    db: Session = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """See aggregate analytics (totals by source, averages by category)."""
    
    total_products = db.query(models.Product).count()

    # Calculate total listings grouped by source
    source_stats_query = db.query(
        models.Source.name, func.count(models.Listing.id)
    ).join(models.Listing).group_by(models.Source.name).all()
    
    source_stats = [{"source_name": row[0], "total_listings": row[1]} for row in source_stats_query]

    # Calculate average price grouped by category
    category_stats_query = db.query(
        models.Product.category, func.avg(models.Listing.current_price)
    ).join(models.Listing).group_by(models.Product.category).all()
    
    category_stats = [
        {"category": row[0], "average_price": round(row[1], 2) if row[1] else 0.0} 
        for row in category_stats_query
    ]

    return {
        "total_products": total_products,
        "listings_by_source": source_stats,
        "averages_by_category": category_stats
    }