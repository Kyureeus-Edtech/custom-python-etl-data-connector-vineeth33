import os
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
API_URL = "https://www.dshield.org/ipsascii.html?limit=100"
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "dshield_db"
COLLECTION_NAME = "top_attackers"

def extract_data_from_api():
    """
    Extracts raw text data of top attackers from the DShield API.
    """
    print("Extracting data from DShield API...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise an exception for bad status codes

        lines = response.text.splitlines()
        data_lines = [line for line in lines if not line.strip().startswith('#')][1:]

        print(f"Successfully extracted {len(data_lines)} data records.")
        return data_lines

    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return None

def transform_data(raw_data_lines: list):
    """
    Transforms the raw text lines into structured documents for MongoDB.
    This version handles inconsistent numbers of columns from the API.
    """
    if not raw_data_lines:
        print("No data to transform.")
        return []
        
    print("Transforming data...")
    transformed_records = []
    
    for line in raw_data_lines:
        try:
            # --- MODIFICATION 1: Split by any whitespace, not just tabs ---
            parts = line.split() 
            
            clean_record = None

            # --- MODIFICATION 2: Check the number of columns before parsing ---
            if len(parts) >= 9:
                # This is the expected full format
                clean_record = {
                    "ip_address": parts[5],
                    "reports": int(parts[2]),
                    "targets": int(parts[3]),
                    "attacks": int(parts[4]),
                    "first_seen": datetime.strptime(f"{parts[0]} {parts[1]}", "%Y-%m-%d %H:%M:%S"),
                    "last_seen": datetime.strptime(f"{parts[0]} {parts[1]}", "%Y-%m-%d %H:%M:%S"),
                    "location": {
                        "country_code": parts[7],
                        "country_name": parts[6],
                        "city": ' '.join(parts[8:]) # Join remaining parts for city name
                    },
                    "ingestion_timestamp": datetime.utcnow()
                }
            elif len(parts) == 5:
                # This handles the shorter, malformed format we observed
                clean_record = {
                    "ip_address": parts[0],
                    "reports": int(parts[1]),
                    "targets": int(parts[2]),
                    "attacks": 0, # Attacks count is missing in this format
                    "first_seen": datetime.strptime(parts[3], "%Y-%m-%d"),
                    "last_seen": datetime.strptime(parts[4], "%Y-%m-%d"),
                    "location": { # Provide default values for missing data
                        "country_code": "N/A",
                        "country_name": "N/A",
                        "city": "N/A"
                    },
                    "ingestion_timestamp": datetime.utcnow()
                }

            if clean_record:
                transformed_records.append(clean_record)
            else:
                print(f"Skipping line with unexpected format: '{line}'")

        except (IndexError, ValueError) as e:
            print(f"Skipping malformed line: '{line}'. Error: {e}")
            continue

    print(f"Transformation complete for {len(transformed_records)} records.")
    return transformed_records

def load_data_to_mongodb(data: list):
    """
    Loads the transformed data into a MongoDB collection.
    """
    if not data:
        print("No data to load.")
        return

    print("Loading data into MongoDB...")
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        collection.delete_many({})
        result = collection.insert_many(data)
        
        print(f"Successfully loaded {len(result.inserted_ids)} documents into '{COLLECTION_NAME}'.")
        client.close()
    except Exception as e:
        print(f"Error during MongoDB load: {e}")

def main():
    """ Main function to run the ETL pipeline. """
    print("--- Starting DShield ETL Pipeline ---")
    raw_lines = extract_data_from_api()
    
    if raw_lines:
        transformed_records = transform_data(raw_lines)
        load_data_to_mongodb(transformed_records)
    
    print("--- ETL Pipeline Finished ---")

if __name__ == "__main__":
    main()