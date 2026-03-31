import json
from sqlalchemy.orm import Session
from . import models
from .database import SessionLocal


def normalize_key(name, brand):
    return f"{brand}_{name}".lower().replace(" ", "_")


def ingest_data(file_path: str):
    db: Session = SessionLocal()

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for item in data:

        name = item.get("model")
        brand = item.get("brand")
        price = item.get("price")
        url = item.get("product_url")

        if not name or not price:
            continue

        normalized = normalize_key(name, brand)

        # Check if product exists
        product = db.query(models.Product).filter_by(normalized_key=normalized).first()

        if not product:
            product = models.Product(
                name=name,
                brand=brand,
                category="unknown",
                normalized_key=normalized
            )
            db.add(product)
            db.commit()
            db.refresh(product)

        # Create source (hardcode for now)
        source = db.query(models.Source).filter_by(name="1stdibs").first()
        if not source:
            source = models.Source(name="1stdibs")
            db.add(source)
            db.commit()
            db.refresh(source)

        # Create listing
        listing = models.Listing(
            product_id=product.id,
            source_id=source.id,
            external_id=url,
            url=url,
            current_price=price,
            currency="USD"
        )
        db.add(listing)
        db.commit()
        db.refresh(listing)

        # Add price history
        history = models.PriceHistory(
            listing_id=listing.id,
            price=price
        )
        db.add(history)
        db.commit()

    db.close()