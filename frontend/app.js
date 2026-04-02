const API_BASE = "http://127.0.0.1:8000";
const API_KEY = "entrupy-intern-2026"; // The mock key we defined in the backend

const HEADERS = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
};

// --- INIT ---
document.addEventListener("DOMContentLoaded", () => {
    fetchAnalytics();
    fetchProducts();
});

// --- API FETCHING ---
async function fetchAnalytics() {
    try {
        const response = await fetch(`${API_BASE}/analytics/`, { headers: HEADERS });
        const data = await response.json();
        renderAnalytics(data);
    } catch (error) {
        console.error("Error fetching analytics:", error);
    }
}

async function fetchProducts(queryParams = "") {
    try {
        const response = await fetch(`${API_BASE}/products/${queryParams}`, { headers: HEADERS });
        const data = await response.json();
        renderProducts(data);
    } catch (error) {
        console.error("Error fetching products:", error);
    }
}

async function triggerIngestion() {
    const btn = document.getElementById("refresh-btn");
    btn.innerText = "Ingesting...";
    btn.disabled = true;

    try {
        await fetch(`${API_BASE}/ingest/`, { method: "POST", headers: HEADERS });
        alert("Data refreshed successfully!");
        fetchAnalytics();
        fetchProducts();
    } catch (error) {
        alert("Error refreshing data.");
    } finally {
        btn.innerText = "Trigger Data Refresh";
        btn.disabled = false;
    }
}

// --- RENDERING ---
function renderAnalytics(data) {
    document.getElementById("total-products").innerText = data.total_products;
    
    const sourceList = document.getElementById("source-stats");
    sourceList.innerHTML = data.listings_by_source.map(s => 
        `<li><span>${s.source_name}</span> <strong>${s.total_listings}</strong></li>`
    ).join('');

    const catList = document.getElementById("category-stats");
    catList.innerHTML = data.averages_by_category.map(c => 
        `<li><span>${c.category}</span> <strong>$${c.average_price}</strong></li>`
    ).join('');
}

function renderProducts(products) {
    const grid = document.getElementById("product-grid");
    grid.innerHTML = "";
    
    // Check if the user wants to see ONLY dropped prices
    const showDroppedOnly = document.getElementById("filter-dropped").checked;
    let displayedCount = 0;

    products.forEach(product => {
        // 1. Find the listing with the lowest current price
        let bestListing = product.listings.reduce((prev, curr) => 
            prev.current_price < curr.current_price ? prev : curr
        );
        
        let lowestPrice = bestListing.current_price;
        let numListings = product.listings.length;
        
        // 2. Check if this specific listing has dropped in price
        let hasDropped = false;
        let originalPrice = lowestPrice;

        if (bestListing.price_history && bestListing.price_history.length > 1) {
            originalPrice = bestListing.price_history[0].price;
            if (lowestPrice < originalPrice) {
                hasDropped = true;
            }
        }

        // --- NEW LOGIC: Skip rendering if filter is ON but price hasn't dropped ---
        if (showDroppedOnly && !hasDropped) {
            return; // This skips to the next product in the loop
        }

        displayedCount++;

        // 3. Build the HTML dynamically
        const card = document.createElement("div");
        card.className = "product-card";
        card.onclick = () => openModal(product);
        
        let priceSectionHTML = "";
        if (hasDropped) {
            priceSectionHTML = `
                <div class="price-container">
                    <span class="old-price">$${originalPrice}</span>
                    <span class="current-price drop">$${lowestPrice} ↓</span>
                    <span class="badge-drop">Price Dropped!</span>
                </div>
            `;
        } else {
            priceSectionHTML = `
                <div class="price-container">
                    <span class="current-price normal">Starting at $${lowestPrice}</span>
                </div>
            `;
        }

        card.innerHTML = `
            <h3>${product.name}</h3>
            <p><strong>Brand:</strong> ${product.brand || 'N/A'}</p>
            <p><strong>Category:</strong> ${product.category}</p>
            <hr>
            <p>Available across <strong>${numListings}</strong> listings</p>
            ${priceSectionHTML}
        `;
        grid.appendChild(card);
    });

    // Handle the case where no products match the drop filter
    if (displayedCount === 0) {
        grid.innerHTML = "<p>No products found matching those filters.</p>";
    }
}

// --- FILTERS ---
function applyFilters() {
    const category = document.getElementById("filter-category").value;
    const min = document.getElementById("filter-min").value;
    const max = document.getElementById("filter-max").value;

    const params = new URLSearchParams();
    if (category) params.append("category", category);
    if (min) params.append("min_price", min);
    if (max) params.append("max_price", max);

    fetchProducts(`?${params.toString()}`);
}

// --- MODAL & PRICE HISTORY ---
function openModal(product) {
    document.getElementById("modal-title").innerText = product.name;
    document.getElementById("modal-brand").innerText = `Brand: ${product.brand || 'N/A'} | Category: ${product.category}`;
    
    const listingsContainer = document.getElementById("modal-listings");
    listingsContainer.innerHTML = product.listings.map(listing => {
        
        // Build price history table rows
        const historyRows = listing.price_history.map(h => {
            const date = new Date(h.timestamp).toLocaleString();
            return `<tr><td>${date}</td><td>$${h.price}</td></tr>`;
        }).join('');

        return `
            <div class="listing-box">
                <h4><a href="${listing.url}" target="_blank">View on Marketplace (External ID: ${listing.external_id})</a></h4>
                <p>Current Price: <strong>$${listing.current_price}</strong></p>
                <p>Last Seen: ${new Date(listing.last_seen).toLocaleString()}</p>
                
                <h5>Price History</h5>
                <table class="history-table">
                    <thead><tr><th>Date</th><th>Price</th></tr></thead>
                    <tbody>${historyRows}</tbody>
                </table>
            </div>
        `;
    }).join('');

    document.getElementById("product-modal").style.display = "block";
}

function closeModal() {
    document.getElementById("product-modal").style.display = "none";
}

// Close modal if clicking outside of it
window.onclick = function(event) {
    const modal = document.getElementById("product-modal");
    if (event.target == modal) {
        modal.style.display = "none";
    }
}