import sys
import math
import requests
import pandas as pd
from os import environ
from time import sleep
from dotenv import load_dotenv
from opencage.geocoder import OpenCageGeocode
from requests.exceptions import RequestException

# --- Constants ---
# Grouping constants at the top makes the script easier to configure.
GRAPHQL_API_URL = "https://gateway.avito.ma/graphql"
MAX_RESULTS_PER_PAGE = 1000  # The API's maximum page size

GRAPHQL_API_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
}

GRAPHQL_API_DATA_QUERY = """
query getListingAds($query: ListingAdsSearchQuery!) {
  getListingAds(query: $query) {
    ads {
      details {
        ... on PublishedAd {
          adId
          listId
          category { id name parent { id name } }
          type { key name }
          title
          description
          price { withCurrency withoutCurrency }
          discount
          params {
            primary { ... on TextAdParam { id name textValue } ... on NumericAdParam { id name numericValue } ... on BooleanAdParam { id name booleanValue } }
            secondary { ... on TextAdParam { id name textValue } ... on NumericAdParam { id name numericValue } ... on BooleanAdParam { id name booleanValue } }
          }
          sellerType
          location { city { id name } area { id name } }
          listTime
          isEcommerce
          isImmoneuf
        }
      }
    }
  }
}
"""

GRAPHQL_API_COUNT_QUERY = """
query getListingAds($query: ListingAdsSearchQuery!) {
  getListingAds(query: $query) {
    count {
      total
    }
  }
}
"""

# --- Helper Functions ---
def get_graphql_api_variables(page_offset: int = 1, page_size: int = 1) -> dict:
    """Creates the variables dictionary for the GraphQL query."""
    return {
        "query": {
            "filters": {
                "ad": {
                    "categoryId": 1200, "type": "SELL", "hasPrice": True,
                    "hasImage": False, "price": {"greaterThanOrEqual": 0}
                }
            },
            "page": {"number": page_offset, "size": page_size}
        }
    }

def fetch_graphql_data(query: str, variables: dict) -> dict | None:
    """
    Sends a request to the GraphQL API with robust error handling.
    """
    try:
        response = requests.post(
            GRAPHQL_API_URL, headers=GRAPHQL_API_HEADERS, json={"query": query, "variables": variables}
        )
        response.raise_for_status()  # Checks for HTTP errors like 404 or 500
        data = response.json()
        if "errors" in data:
            print(f"GraphQL API Error: {data['errors']}", file=sys.stderr)
            return None
        return data
    except RequestException as e:
        print(f"A network error occurred: {e}", file=sys.stderr)
        return None

# --- Main Logic ---
def main():
    """Main function to orchestrate the data fetching, processing, and geocoding."""
    # Step 1: Fetch total ad count to determine pagination
    print("Fetching total number of ads...")
    count_vars = get_graphql_api_variables()
    count_response = fetch_graphql_data(GRAPHQL_API_COUNT_QUERY, count_vars)
    
    if not count_response or not count_response.get("data"):
        print("Failed to fetch ad count. Exiting.", file=sys.stderr)
        sys.exit(1)

    results_count = int(count_response["data"]["getListingAds"]["count"]["total"])
    if results_count == 0:
        print("No ads found for the specified criteria.")
        return

    pages_count = math.ceil(results_count / MAX_RESULTS_PER_PAGE)
    print(f"Found {results_count} ads across {pages_count} pages.")

    # Step 2: Fetch all ad data using pagination
    raw_data = []
    for page in range(1, pages_count + 1):
        print(f"Fetching page {page}/{pages_count}...")
        page_vars = get_graphql_api_variables(page, MAX_RESULTS_PER_PAGE)
        page_response = fetch_graphql_data(GRAPHQL_API_DATA_QUERY, page_vars)
        
        if page_response and page_response.get("data"):
            ads_list = page_response["data"]["getListingAds"]["ads"]
            # NEW FIXED CODE
            for ad_item in ads_list:
                # First, check if the item is a dictionary
                if isinstance(ad_item, dict):
                    details = ad_item.get("details")
                    # This is the key change: the append is now INSIDE the 'if details:' block.
                    # It will only run if 'details' is not None.
                    if details:
                        raw_data.append(details)
                else:
                    # If it's a string or something else, print a warning and skip it
                    print(f"\nWarning: Skipping unexpected item in API response: {ad_item}")
        else:
            print(f"Warning: Failed to fetch data for page {page}.", file=sys.stderr)
    
    # Step 3: Clean and flatten the raw data into a structured format
    print("\nProcessing and cleaning data...")
    clean_data = []
    for ad in raw_data:
        # Using .get() with default empty dicts {} prevents errors if a key is missing
        clean_row = {
            "adId": ad.get("adId"), "listId": ad.get("listId"), "listTime": ad.get("listTime"),
            "title": ad.get("title"), "description": ad.get("description"),
            "priceStr": ad.get("price", {}).get("withCurrency"), "price": ad.get("price", {}).get("withoutCurrency"),
            "categoryId": ad.get("category", {}).get("id"), "categoryName": ad.get("category", {}).get("name"),
            "parentCategoryId": ad.get("category", {}).get("parent", {}).get("id"),
            "parentCategoryName": ad.get("category", {}).get("parent", {}).get("name"),
            "locationCityName": ad.get("location", {}).get("city", {}).get("name"),
            "locationAreaName": ad.get("location", {}).get("area", {}).get("name"),
            "sellerType": ad.get("sellerType"), "isEcommerce": ad.get("isEcommerce"),
        }
        
        # This loop efficiently processes both primary and secondary parameters
        for param_type in ["primary", "secondary"]:
            params = ad.get("params", {}).get(param_type)
            if params:
                for param in params:
                    if param and param.get("id"):
                        value = param.get("textValue") or param.get("numericValue") or param.get("booleanValue")
                        clean_row[param["id"]] = value

        clean_data.append(clean_row)

    df = pd.DataFrame(clean_data)
    print(f"Successfully created a DataFrame with {len(df)} rows.")

    # Step 4: Geocode unique locations to add latitude and longitude
    print("\nStarting geocoding process...")
    load_dotenv()
    OPENCAGE_API_KEY = environ.get("OPENCAGE_API_KEY")

    if not OPENCAGE_API_KEY:
        print("OpenCage API key not found. Skipping geocoding.", file=sys.stderr)
        df.to_csv("avito_ads_unmapped.csv", index=False)
        print("Data saved to avito_ads_unmapped.csv")
        return

    geocoder = OpenCageGeocode(OPENCAGE_API_KEY)
    
    # Create a DataFrame of unique addresses to avoid redundant API calls
    addresses = df[["locationCityName", "locationAreaName"]].copy().drop_duplicates().dropna(subset=["locationCityName"])
    
    geocoded_data = []
    total_addresses = len(addresses)
    print(f"Found {total_addresses} unique locations to geocode.")

    for row in addresses.itertuples(index=False):
        query = f"{row.locationAreaName}, {row.locationCityName}, Morocco" if pd.notna(row.locationAreaName) else f"{row.locationCityName}, Morocco"
        try:
            results = geocoder.geocode(query, language='en')
            if results:
                top_result = results[0]
                geocoded_data.append({
                    "locationCityName": row.locationCityName, "locationAreaName": row.locationAreaName,
                    "latitude": top_result["geometry"]["lat"], "longitude": top_result["geometry"]["lng"],
                })
        except Exception as e:
            print(f"\nError geocoding '{query}': {e}", file=sys.stderr)
        
        sys.stdout.write(f"\rCompleted geocoding {len(geocoded_data)}/{total_addresses} locations...")
        sys.stdout.flush()
        sleep(1.1)  # Respect the API rate limit of 1 request/second

    print("\nGeocoding complete.")

    if geocoded_data:
        coordinates_df = pd.DataFrame(geocoded_data)
        df = df.merge(coordinates_df, on=["locationCityName", "locationAreaName"], how="left")

    # Step 5: Save the final DataFrame to a CSV file
    df.to_csv("avito_ads_geocoded.csv", index=False)
    print("\nFinal data saved to avito_ads_geocoded.csv")

if __name__ == "__main__":
    main()