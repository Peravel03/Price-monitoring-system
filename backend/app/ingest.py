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
    """Safely normalizes the product key, handling missing brands."""
    safe_brand = str(brand) if brand else "unknown"
    safe_name = str(name) if name else "unknown"
    return f"{safe_brand}_{safe_name}".lower().replace(" ", "_")

def guess_category(name: str) -> str:
    """A simple auto-categorizer based on item names to improve analytics."""
    if not name:
        return "Other"
    
    name_lower = name.lower()
    
    
    if any(word in name_lower for word in ["jacket", "shirt", "denim", "sweater", "tee", "jeans", "hoodie", "pants", "cardigan"]):
        return "Apparel"
        
    elif any(word in name_lower for word in ["necklace", "ring", "bracelet", "earring", "charm", "pendant"]):
        return "Jewelry"
        
    elif any(word in name_lower for word in ["belt", "sunglasses", "hat", "cap", "scarf"]):
        return "Accessories"
        
    elif any(word in name_lower for word in ["bag", "tote", "wallet", "purse", "backpack"]):
        return "Bags"
        
    elif any(word in name_lower for word in ["shoe", "sneaker", "boot", "sandal"]):
        return "Footwear"
        
    elif any(word in name_lower for word in ["watch", "timepiece"]):
        return "Watches"
    
    return "Other" 

async def fetch_marketplace_data(source_name: str, max_retries: int = 3) -> dict:
    """Fetches data from all local JSON files matching the source_name."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Fetching from {source_name} (Attempt {attempt}/{max_retries})...")
            await asyncio.sleep(0.5) # Simulate latency
            
            BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            data_dir = os.path.join(BASE_DIR, "data")
            
            all_source_items = []
            
            # Loop through all files in the data directory to find matches
            if os.path.exists(data_dir):
                for filename in os.listdir(data_dir):
                    if filename.startswith(source_name) and filename.endswith(".json"):
                        full_path = os.path.join(data_dir, filename)
                        
                        with open(full_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            
                        # Add items to our master list for this source
                        if isinstance(data, list):
                            all_source_items.extend(data)
                        else:
                            all_source_items.append(data)
                            
            return {"source": source_name, "items": all_source_items}

        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for {source_name}: {e}")
            if attempt == max_retries:
                logger.error(f"Max retries reached for {source_name}. Skipping.")
                return {"source": source_name, "items": []}
            
            backoff_time = 2 ** (attempt - 1)
            logger.info(f"Retrying {source_name} in {backoff_time} seconds...")
            await asyncio.sleep(backoff_time)

async def gather_all_marketplaces():
    """Concurrently fetches data from all required marketplaces."""
    sources = ["grailed", "fashionphile", "1stdibs"]
    tasks = [fetch_marketplace_data(source) for source in sources]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out any fatal async crashes, keeping only successful dicts
    return [r for r in results if isinstance(r, dict)]

def process_and_store_data(results: list, db: Session = None):
    """Synchronous DB logic to update prices, assign sources, and trigger notifications."""
    if not results:
        return

    db = db or SessionLocal()
    try:
        for result in results:
            source_name = result["source"]
            items = result["items"]
            
            if not items:
                continue

            # 1. Get or Create the Source Dynamically
            source_obj = db.query(models.Source).filter_by(name=source_name).first()
            if not source_obj:
                source_obj = models.Source(name=source_name)
                db.add(source_obj)
                db.commit()
                db.refresh(source_obj)

            # 2. Process all items for this specific source
            for item in items:
                # Handle varying JSON key structures across marketplaces
                name = item.get("model") or item.get("name")
                brand = item.get("brand")
                price = item.get("price")
                url = item.get("product_url") or item.get("url")

                if not name or price is None or not url:
                    continue
                
                # Ensure price is read as a float
                try:
                    price = float(price)
                except ValueError:
                    continue 

                normalized = normalize_key(name, brand)
                category = guess_category(name)

                # Check/Create Base Product
                product = db.query(models.Product).filter_by(normalized_key=normalized).first()
                if not product:
                    product = models.Product(
                        name=name, brand=brand, category=category, normalized_key=normalized
                    )
                    db.add(product)
                    db.commit()
                    db.refresh(product)

                # Check for existing listing on THIS specific marketplace
                existing_listing = db.query(models.Listing).filter_by(
                    external_id=url, source_id=source_obj.id
                ).first()

                if existing_listing:
                    # --- PRICE CHANGE DETECTION & NOTIFICATION LOGIC ---
                    if existing_listing.current_price != price:
                        old_price = existing_listing.current_price
                        logger.info(f"Price changed for: {name} (${old_price} -> ${price})")
                        
                        existing_listing.current_price = price
                        
                        history = models.PriceHistory(listing_id=existing_listing.id, price=price)
                        db.add(history)
                        
                        notification = models.NotificationEvent(
                            listing_id=existing_listing.id,
                            old_price=old_price,
                            new_price=price,
                            status="pending"
                        )
                        db.add(notification)
                        db.commit()
                else:
                    logger.info(f"New listing found on {source_name}: {name}")
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