import json
import os
import asyncio
import logging
from sqlalchemy.orm import Session
from . import models
from .database import SessionLocal

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def normalize_key(name, brand):
    return f"{brand}_{name}".lower().replace(" ", "_")

async def fetch_marketplace_data(source_name: str, max_retries: int = 3) -> list:
    """Simulates async data fetching with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Fetching from {source_name} (Attempt {attempt}/{max_retries})...")
            await asyncio.sleep(0.5)
            
            if attempt == 1 and source_name == "grailed":
                raise ConnectionError("Simulated network timeout")

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
            
            backoff_time = 2 ** (attempt - 1)
            logger.info(f"Retrying {source_name} in {backoff_time} seconds...")
            await asyncio.sleep(backoff_time)

async def gather_all_marketplaces():
    """Concurrently fetches data from all required marketplaces."""
    sources = ["grailed", "fashionphile", "1stdibs"]
    tasks = [fetch_marketplace_data(source) for source in sources]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    all_data = []
    for source_data in results:
        if isinstance(source_data, list):
            all_data.extend(source_data)
            
    return all_data

def process_and_store_data(data: list):
    """Synchronous DB logic to update prices and create Notification Events."""
    if not data:
        return

    db: Session = SessionLocal()
    try:
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
                # --- PRICE CHANGE DETECTION & NOTIFICATION LOGIC ---
                if existing_listing.current_price != price:
                    old_price = existing_listing.current_price
                    logger.info(f"Price changed for: {name} (${old_price} -> ${price})")
                    
                    # 1. Update current price
                    existing_listing.current_price = price
                    
                    # 2. Add to price history
                    history = models.PriceHistory(listing_id=existing_listing.id, price=price)
                    db.add(history)
                    
                    # 3. Create a pending Notification Event
                    notification = models.NotificationEvent(
                        listing_id=existing_listing.id,
                        old_price=old_price,
                        new_price=price,
                        status="pending"
                    )
                    db.add(notification)
                    
                    db.commit()
                # ---------------------------------------------------
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

async def process_notifications():
    """
    Background worker that finds pending notifications and 'sends' them.
    This simulates a webhook delivery system.
    """
    db: Session = SessionLocal()
    try:
        pending_events = db.query(models.NotificationEvent).filter_by(status="pending").all()
        
        if not pending_events:
            logger.info("No pending notifications to process.")
            return

        logger.info(f"Processing {len(pending_events)} pending notifications...")
        
        for event in pending_events:
            event.status = "processing"
            db.commit()
            
            try:
                # Simulate network delay for sending a webhook
                await asyncio.sleep(0.5)
                
                # The actual "Notification"
                logger.info(f"[WEBHOOK FIRED] Alert! Listing {event.listing_id} changed from ${event.old_price} to ${event.new_price}")
                
                event.status = "sent"
            except Exception as e:
                logger.error(f"Failed to send webhook for event {event.id}: {e}")
                event.status = "failed"
                
            db.commit()
    finally:
        db.close()

async def run_ingestion_pipeline():
    """Main entrypoint orchestrating fetch, store, and notify."""
    logger.info("Starting async ingestion pipeline...")
    
    # 1. Fetch data concurrently
    fetched_data = await gather_all_marketplaces()
    
    # 2. Store data and create pending events (Synchronous DB writes in a thread)
    await asyncio.to_thread(process_and_store_data, fetched_data)
    
    # 3. Process webhooks in the background without blocking the main return
    asyncio.create_task(process_notifications())
    
    logger.info("Ingestion pipeline completed.")