from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from .database import Base

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    brand = Column(String)
    category = Column(String)
    normalized_key = Column(String, unique=True)

    listings = relationship("Listing", back_populates="product")


class Source(Base):
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)

    listings = relationship("Listing", back_populates="source")


class Listing(Base):
    __tablename__ = "listings"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"), index=True)
    source_id = Column(Integer, ForeignKey("sources.id"), index=True)

    external_id = Column(String)
    url = Column(String)
    current_price = Column(Float)
    currency = Column(String)
    last_seen = Column(DateTime, default=datetime.utcnow)

    product = relationship("Product", back_populates="listings")
    source = relationship("Source", back_populates="listings")
    price_history = relationship("PriceHistory", back_populates="listing")
    
    # Links to the new notification events table
    notifications = relationship("NotificationEvent", back_populates="listing")


class PriceHistory(Base):
    __tablename__ = "price_history"

    id = Column(Integer, primary_key=True, index=True)
    listing_id = Column(Integer, ForeignKey("listings.id"), index=True)

    price = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)

    listing = relationship("Listing", back_populates="price_history")


class NotificationEvent(Base):
    __tablename__ = "notification_events"

    id = Column(Integer, primary_key=True, index=True)
    listing_id = Column(Integer, ForeignKey("listings.id"), index=True)
    
    old_price = Column(Float)
    new_price = Column(Float)
    
    # Status can be: 'pending', 'processing', 'sent', 'failed'
    status = Column(String, default="pending", index=True) 
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    listing = relationship("Listing", back_populates="notifications")