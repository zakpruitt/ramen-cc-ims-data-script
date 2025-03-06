import asyncio
import json
import os
import time
from tcgdexsdk import TCGdex

CACHE_FILE = "./data/series_cache.json"

async def build_series_cache() -> dict:
    """
    Fetches every series via the TCGdex API and, for each series,
    does an individual .get() to obtain detailed information including its related sets.
    Then builds a mapping from each set id to the series name.
    """
    tcgdex = TCGdex("en")
    
    print("Fetching list of series briefs...")
    start_list = time.perf_counter()
    series_briefs = await tcgdex.serie.list()
    elapsed_list = time.perf_counter() - start_list
    print(f"Fetched {len(series_briefs)} series briefs in {elapsed_list:.2f} seconds.")
    
    series_cache = {}
    # Process each series individually.
    for brief in series_briefs:
        print(f"Fetching details for series '{brief.id}'...")
        start_detail = time.perf_counter()
        detailed_series = await tcgdex.serie.get(brief.id)
        elapsed_detail = time.perf_counter() - start_detail
        print(f"Fetched details for series '{brief.id}' in {elapsed_detail:.2f} seconds.")
        
        # For each set in the detailed series, map the set id to the series name.
        if hasattr(detailed_series, "sets") and detailed_series.sets:
            for set_resume in detailed_series.sets:
                set_code = set_resume.id.lower()  # e.g. "swsh8", "bw3", etc.
                series_cache[set_code] = detailed_series.name
                # Optionally, print mapping info:
                print(f"Mapping set '{set_resume.id}' -> '{detailed_series.name}'")
        else:
            print(f"Warning: Series '{brief.id}' has no related sets.")
    
    return series_cache

async def main():
    # If the cache file exists, load it; otherwise, build it.
    if os.path.exists(CACHE_FILE):
        print(f"Loading series cache from {CACHE_FILE}...")
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        print(f"Loaded cache with {len(cache)} mappings.")
    else:
        print("No existing series cache found. Building new cache...")
        cache = await build_series_cache()
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
        print(f"Series cache saved to {CACHE_FILE}.")
    
    print("Final series cache mapping:")
    print(json.dumps(cache, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
