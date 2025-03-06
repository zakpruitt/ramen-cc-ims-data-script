import asyncio
import csv
import json
import os
import time
import requests
from tcgdexsdk import TCGdex

# Output and API settings
OUTPUT_FILENAME = "./data/combined_export.csv"
PRODUCT_API_URL = "https://www.pokedata.io/api/products"
SERIES_CACHE_FILE = "./data/series_cache.json"

# Ensure the data directory exists for cache
os.makedirs(os.path.dirname(SERIES_CACHE_FILE), exist_ok=True)

##############################
# Product Data Functions
##############################

def fetch_products(api_url):
    """Fetch product data from the PokéData API."""
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def transform_product(product):
    """
    Transform a product JSON object into the desired CSV format.
    
    Mapping rules:
      - handleId: product id
      - name: product name
      - productImageUrl: product img_url
      - collection: language (Pascal cased) ; series (Pascal cased with ' era' appended, if provided) ; constant "Sealed Product"
      - sku: product id
      - price: market_value
      - inventory: 0
      - discountMode: "amount"
      - visible: "true"
      - ribbon: blank
      - weight: 0
    """
    language_pascal = product.get("language", "").capitalize()
    series_value = product.get("series")
    series_pascal = series_value.title() if series_value else ""
    
    if series_pascal:
        collection = f"{language_pascal};{series_pascal} era;Sealed Product"
    else:
        collection = f"{language_pascal};Sealed Product"
    
    return {
        "handleId": product["id"],
        "name": product["name"],
        "productImageUrl": product["img_url"],
        "collection": collection,
        "sku": product["id"],
        "price": product["market_value"],
        "inventory": 0,
        "discountMode": "amount",
        "visible": "true",
        "ribbon": "",
        "weight": 0,
    }

##############################
# Card Data Functions
##############################

def transform_card(card, series_mapping) -> dict:
    """
    Transform a card brief from TCGdex into the desired CSV format.
    
    - handleId: card.id
    - name: card.name + " #" + card.localId (e.g. "Froslass #174")
    - productImageUrl: card.image modified to request a high-quality image.
    - collection: "English;{series_name} Era; Raw Single"
         * Uses the full set code (first part of card.id) to look up the series from the cache.
    - sku: card.id
    - price: 0
    - inventory: 0
    - discountMode: "amount"
    - visible: "true"
    - ribbon: blank
    - weight: 0
    """
    # Use the full set code (everything before the dash) as the key.
    full_set_code = card.id.split('-')[0].lower()  # e.g., "swsh8" from "swsh8-150"
    series_name = series_mapping.get(full_set_code, "Unknown")
    
    # Use a different URL style for high-quality images (adjust as needed).
    image_url = f"{card.image}/high.webp"
    
    return {
        "handleId": card.id,
        "name": f"{card.name} #{card.localId}",
        "productImageUrl": image_url,
        "collection": f"English;{series_name} Era;Raw Single",
        "sku": card.id,
        "price": 0,
        "inventory": 0,
        "discountMode": "amount",
        "visible": "true",
        "ribbon": "",
        "weight": 0,
    }

async def load_series_cache(tcgdex: TCGdex) -> dict:
    """
    Loads the series mapping from the cache file.
    If the cache doesn't exist, fetch sets from the API, build a mapping from full set code to series name, and save it.
    """
    if os.path.exists(SERIES_CACHE_FILE):
        print(f"Loading series cache from {SERIES_CACHE_FILE}...")
        with open(SERIES_CACHE_FILE, "r", encoding="utf-8") as f:
            series_mapping = json.load(f)
        print(f"Loaded series cache with {len(series_mapping)} entries.")
    else:
        print("Series cache not found. Fetching sets from API to build cache...")
        sets = await tcgdex.set.list()
        series_mapping = {}
        for s in sets:
            # Assume each set object has a property 'series' holding the series name.
            set_code = s.id.lower()  # e.g., "swsh8" or "sv08.5"
            series_name = getattr(s, "series", None)
            if series_name is None:
                series_name = "Unknown"
            series_mapping[set_code] = series_name
        with open(SERIES_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(series_mapping, f, indent=2)
        print(f"Series cache built and saved to {SERIES_CACHE_FILE}.")
    return series_mapping

async def fetch_card_data() -> list:
    """Fetches card briefs from TCGdex and transforms them using the series cache."""
    tcgdex = TCGdex("en")
    print("Started fetching TCG card briefs...")
    start_briefs = time.perf_counter()
    card_briefs = await tcgdex.card.list()
    elapsed_briefs = time.perf_counter() - start_briefs
    print(f"Fetched {len(card_briefs)} TCG card briefs in {elapsed_briefs:.2f} seconds.")
    
    series_mapping = await load_series_cache(tcgdex)
    print("Transforming TCG card briefs...")
    transformed_cards = [transform_card(card, series_mapping) for card in card_briefs]
    print(f"Transformed {len(transformed_cards)} TCG cards.")
    return transformed_cards

##############################
# Main Combined Export
##############################

async def main():
    combined_rows = []
    
    # Process product data (synchronous part)
    try:
        print("Fetching product data from PokéData API...")
        start_prod = time.perf_counter()
        raw_products = fetch_products(PRODUCT_API_URL)
        prod_rows = [transform_product(prod) for prod in raw_products]
        elapsed_prod = time.perf_counter() - start_prod
        print(f"Fetched and transformed {len(prod_rows)} products in {elapsed_prod:.2f} seconds.")
        combined_rows.extend(prod_rows)
    except Exception as e:
        print(f"Error occurred fetching products: {e}")
    
    # Process TCG card data (asynchronous part)
    try:
        card_rows = await fetch_card_data()
        combined_rows.extend(card_rows)
    except Exception as e:
        print(f"Error occurred fetching TCG card data: {e}")
    
    # Write combined data to CSV
    print("Writing combined data to CSV...")
    start_csv = time.perf_counter()
    fieldnames = [
        "handleId", "name", "productImageUrl", "collection",
        "sku", "price", "inventory", "discountMode", "visible",
        "ribbon", "weight"
    ]
    with open(OUTPUT_FILENAME, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in combined_rows:
            writer.writerow(row)
    elapsed_csv = time.perf_counter() - start_csv
    print(f"Exported {len(combined_rows)} total rows to CSV in {elapsed_csv:.2f} seconds.")
    print(f"Combined CSV file '{OUTPUT_FILENAME}' created successfully.")

if __name__ == "__main__":
    asyncio.run(main())
