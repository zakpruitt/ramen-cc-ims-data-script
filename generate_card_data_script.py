import asyncio
import csv
import time
import json
import os
from tcgdexsdk import TCGdex

OUTPUT_FILENAME = "./data/tcg_cards.csv"
CACHE_FILE = "./data/series_cache.json"

def transform_card(card, series_mapping) -> dict:
    """
    Maps a card brief to the CSV format using series information from the cache.
    
    - handleId: card.id
    - name: card.name + " #" + card.localId (e.g. "Froslass #174")
    - productImageUrl: card.image appended with quality parameters.
    - collection: "English;{series_name} Era; Raw Single"
         * Uses the full set code (first part of card.id) to look up the series in the cache.
    - sku: card.id
    - price: 0 (default)
    - inventory: 0
    - discountMode: "amount"
    - visible: "true"
    - ribbon: (blank)
    - weight: 0
    """
    # Use the full set code (everything before the dash) as the key.
    full_set_code = card.id.split('-')[0].lower()  # e.g., "swsh8" for "swsh8-150"
    series_name = series_mapping.get(full_set_code, "Unknown")
    
    # Append quality parameter to the image URL.
    image_url = f"{card.image}/high.webp"
    
    return {
        "handleId": card.id,
        "name": f"{card.name} #{card.localId}",
        "productImageUrl": image_url,
        "collection": f"English;{series_name} Era; Raw Single",
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
    Loads the series mapping from CACHE_FILE.
    If the cache doesn't exist, fetch sets from the API, build a mapping
    from full set code to series name, and save it.
    """
    if os.path.exists(CACHE_FILE):
        print(f"Loading series cache from {CACHE_FILE}...")
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            series_mapping = json.load(f)
        print(f"Loaded series cache with {len(series_mapping)} entries.")
    else:
        print("Series cache not found. Fetching sets from API to build cache...")
        sets = await tcgdex.set.list()
        series_mapping = {}
        for s in sets:
            # Assume each set object has a property 'series' holding the correct series name.
            set_code = s.id.lower()  # e.g., "swsh8" or "sv08.5"
            series_name = getattr(s, "series", None)
            if series_name is None:
                series_name = "Unknown"
            series_mapping[set_code] = series_name
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(series_mapping, f, indent=2)
        print(f"Series cache built and saved to {CACHE_FILE}.")
    return series_mapping

async def main():
    tcgdex = TCGdex("en")  # Initialize the SDK in English

    # Fetch card briefs.
    print("Started fetching card briefs...")
    start_briefs = time.perf_counter()
    card_briefs = await tcgdex.card.list()
    elapsed_briefs = time.perf_counter() - start_briefs
    print(f"Fetched {len(card_briefs)} card briefs in {elapsed_briefs:.2f} seconds.")

    # Load the series mapping (cache) from file or API.
    series_mapping = await load_series_cache(tcgdex)
    
    # Transform each card brief using the series mapping.
    print("Transforming card briefs into CSV rows using series mapping...")
    csv_rows = [transform_card(card, series_mapping) for card in card_briefs]
    print(f"Transformed {len(csv_rows)} cards.")

    # Write the CSV file.
    print("Transferring cards to CSV...")
    start_csv = time.perf_counter()
    fieldnames = [
        "handleId", "name", "productImageUrl", "collection",
        "sku", "price", "inventory", "discountMode", "visible",
        "ribbon", "weight"
    ]
    with open(OUTPUT_FILENAME, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)
    elapsed_csv = time.perf_counter() - start_csv
    print(f"Transferred {len(csv_rows)} cards to CSV in {elapsed_csv:.2f} seconds.")
    print(f"CSV file '{OUTPUT_FILENAME}' created successfully.")

if __name__ == "__main__":
    asyncio.run(main())
