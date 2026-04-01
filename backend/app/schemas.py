from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# --- OUTPUT SCHEMAS (What the user sees) ---

class PriceHistoryOut(BaseModel):
    price: float
    timestamp: datetime

    class Config:
        from_attributes = True  # Allows Pydantic to read standard SQLAlchemy models

class ListingOut(BaseModel):
    id: int
    external_id: str
    url: str
    current_price: float
    currency: str
    last_seen: datetime
    # We will include the price history directly in the listing
    price_history: List[PriceHistoryOut] = []

    class Config:
        from_attributes = True

class ProductOut(BaseModel):
    id: int
    name: str
    brand: Optional[str]
    category: Optional[str]
    listings: List[ListingOut] = []

    class Config:
        from_attributes = True

# --- ANALYTICS SCHEMAS ---

class SourceStats(BaseModel):
    source_name: str
    total_listings: int

class CategoryStats(BaseModel):
    category: str
    average_price: float

class AggregateAnalyticsOut(BaseModel):
    total_products: int
    listings_by_source: List[SourceStats]
    averages_by_category: List[CategoryStats]