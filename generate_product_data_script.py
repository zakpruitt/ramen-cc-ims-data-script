import requests
import csv

API_URL = "https://www.pokedata.io/api/products"
OUTPUT_FILENAME = "wix_products.csv"

def fetch_products(api_url):
    """Fetch product data from the API."""
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def transform_product(product):
    """
    Transform the API product to the desired Wix CSV format.
    
    Mapping rules:
      - handleId: same as product id
      - name: product name
      - productImageUrl: product img_url
      - collection: 
          • language (converted to Pascal case)
          • series (converted to Pascal case with ' era' appended, if available)
          • constant "Sealed Product"
      - sku: same as product id
      - price: market_value
      - inventory: set to 0
      - discountMode: set to "amount"
      - visible: set to "true"
      - ribbon: leave blank
      - weight: set to 0
    """
    # Convert language to Pascal case, if available
    language_pascal = product.get("language", "").capitalize()
    
    # Check if series is provided and convert to Pascal case if so
    series_value = product.get("series")
    series_pascal = series_value.title() if series_value else ""
    
    # Build the collection field conditionally
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

def write_csv(products, filename):
    """Write the list of products to a CSV file."""
    fieldnames = [
        "handleId", "name", "productImageUrl", "collection",
        "sku", "price", "inventory", "discountMode", "visible",
        "ribbon", "weight"
    ]
    with open(filename, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for product in products:
            writer.writerow(product)

def main():
    try:
        raw_products = fetch_products(API_URL)
        transformed_products = [transform_product(prod) for prod in raw_products]
        write_csv(transformed_products, OUTPUT_FILENAME)
        print(f"CSV file '{OUTPUT_FILENAME}' created successfully with {len(transformed_products)} products.")
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
