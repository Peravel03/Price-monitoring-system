import json
import os
import asyncio
import logging
from sqlalchemy.orm import Session
from . import models
from .database import SessionLocal

# Setup basic logging to track our async tasks
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def normalize_key(name, brand):
    return f"{brand}_{name}".lower().replace(" ", "_")

async def fetch_marketplace_data(source_name: str, max_retries: int = 3) -> list:
    """
    Simulates async data fetching with retry logic and exponential backoff.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Fetching from {source_name} (Attempt {attempt}/{max_retries})...")
            
            # Simulate network latency
            await asyncio.sleep(0.5)
            
            # Simulate a network failure on the first attempt for one source to prove retries work
            if attempt == 1 and source_name == "grailed":
                raise ConnectionError("Simulated network timeout")

            # --- HOW THIS LOOKS IN PRODUCTION WITH HTTPX ---
            # async with httpx.AsyncClient() as client:
            #     response = await client.get(f"https://api.{source_name}.com/listings")
            #     response.raise_for_status()
            #     return response.json()
            # -----------------------------------------------

            # Fallback: Read our sample JSON file for the assignment
            BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            full_path = os.path.join(BASE_DIR, "data", "sample_data.json")
            
            with open(full_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            return data if isinstance(data, list) else [data]

        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for {source_name}: {e}")
            if attempt == max_retries:
                logger.error(f"Max retries reached for {source_name}. Skipping.")
                return []
            
            # Exponential backoff (1s, 2s, 4s...)
            backoff_time = 2 ** (attempt - 1)
            logger.info(f"Retrying {source_name} in {backoff_time} seconds...")
            await asyncio.sleep(backoff_time)

async def gather_all_marketplaces():
    """
    Concurrently fetches data from all required marketplaces.
    """
    sources = ["grailed", "fashionphile", "1stdibs"]
    
    # Create concurrent async tasks
    tasks = [fetch_marketplace_data(source) for source in sources]
    
    # gather() runs them simultaneously. return_exceptions=True prevents one crash from ruining the batch
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    all_data = []
    for source_data in results:
        if isinstance(source_data, list):
            all_data.extend(source_data)
            
    return all_data

def process_and_store_data(data: list):
    """
    Synchronous DB logic (your original code, slightly tweaked for the new pipeline).
    """
    if not data:
        logger.warning("No data retrieved to process.")
        return

    db: Session = SessionLocal()
    try:
        # Assuming we just map everything to 1stdibs for now based on your previous code. 
        # (If your JSON has a 'source' field, we can make this dynamic!)
        source_obj = db.query(models.Source).filter_by(name="1stdibs").first()
        if not source_obj:
            source_obj = models.Source(name="1stdibs")
            db.add(source_obj)
            db.commit()
            db.refresh(source_obj)

        for item in data:
            name = item.get("model")
            brand = item.get("brand")
            price = item.get("price")
            url = item.get("product_url")

            if not name or price is None or not url:
                continue

            normalized = normalize_key(name, brand)

            product = db.query(models.Product).filter_by(normalized_key=normalized).first()
            if not product:
                product = models.Product(
                    name=name, brand=brand, category="unknown", normalized_key=normalized
                )
                db.add(product)
                db.commit()
                db.refresh(product)

            existing_listing = db.query(models.Listing).filter_by(
                external_id=url, source_id=source_obj.id
            ).first()

            if existing_listing:
                if existing_listing.current_price != price:
                    logger.info(f"Price changed for: {name} (${existing_listing.current_price} -> ${price})")
                    existing_listing.current_price = price
                    
                    history = models.PriceHistory(listing_id=existing_listing.id, price=price)
                    db.add(history)
                    db.commit()
            else:
                logger.info(f"New listing found: {name}")
                listing = models.Listing(
                    product_id=product.id, source_id=source_obj.id, external_id=url,
                    url=url, current_price=price, currency="USD"
                )
                db.add(listing)
                db.commit()
                db.refresh(listing)

                history = models.PriceHistory(listing_id=listing.id, price=price)
                db.add(history)
                db.commit()

    finally:
        db.close()

async def run_ingestion_pipeline():
    """Main entrypoint orchestrating fetch and store."""
    logger.info("Starting async ingestion pipeline...")
    
    fetched_data = await gather_all_marketplaces()
    
    # Run the synchronous DB operations in a separate thread so FastAPI doesn't block
    await asyncio.to_thread(process_and_store_data, fetched_data)
    
    logger.info("Ingestion pipeline completed.")