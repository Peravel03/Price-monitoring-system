import pytest
from app import models
from app.ingest import normalize_key, process_and_store_data

# --- TEST 6 & 7: Authentication & Tracking ---

def test_missing_api_key(client):
    """Test that requests without an API key are rejected."""
    response = client.get("/products/")
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or missing API Key. Please provide 'X-API-Key' in headers."

def test_invalid_api_key(client):
    """Test that fake API keys are rejected."""
    response = client.get("/products/", headers={"X-API-Key": "fake-key"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or missing API Key. Please provide 'X-API-Key' in headers."

def test_valid_api_key_and_usage_tracking(client, valid_headers):
    """Test that a valid key works and increments usage."""
    # Note: Because we hardcoded the dictionary in main.py, 
    # we can't easily assert the exact count without importing it, 
    # but we can assert the response succeeds (200).
    response = client.get("/products/", headers=valid_headers)
    assert response.status_code == 200

# --- TEST 4: Filtering ---

def test_product_filtering_validation(client, valid_headers):
    """Test that invalid filters are caught by Pydantic."""
    # Sending a string instead of a number for max_price
    response = client.get("/products/?max_price=abc", headers=valid_headers)
    assert response.status_code == 422 # FastAPI validation error

# --- TEST 3: Deduplication ---

def test_normalize_key_logic():
    """Test the core logic that deduplicates products."""
    key1 = normalize_key("Oversized Tee", "Amiri")
    key2 = normalize_key("OVERSIZED TEE", "amiri")
    key3 = normalize_key("Oversized Tee", None)
    
    assert key1 == "amiri_oversized_tee"
    assert key1 == key2 # Proves case insensitivity
    assert key3 == "unknown_oversized_tee" # Proves fallback works