import os
import requests
import html
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# API endpoint for 15 Computer Science (ID: 18), multiple-choice questions
API_URL = "https://opentdb.com/api.php?amount=15&category=18&type=multiple"
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "ssn_assignment_db"
COLLECTION_NAME = "trivia_questions_raw"

def extract_data_from_api():
    """
    Extracts question data from the Open Trivia Database API.
    Handles potential request errors.
    """
    print("Extracting data from the API...")
    try:
        response = requests.get(API_URL)
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status() 
        data = response.json()
        
        # The API returns a response_code. 0 means success.
        if data.get("response_code") != 0:
            print("API returned an error. No results found.")
            return []
            
        print(f"Successfully extracted {len(data['results'])} questions.")
        return data["results"]
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return None

def transform_data(raw_data: list):
    """
    Transforms the raw API data into a clean format for MongoDB.
    - Decodes HTML entities.
    - Combines correct and incorrect answers.
    - Adds an ingestion timestamp.
    """
    if not raw_data:
        print("No data to transform.")
        return []
        
    print("Transforming data...")
    transformed_records = []
    
    for record in raw_data:
        # 1. Decode HTML entities from all relevant text fields
        question_text = html.unescape(record["question"])
        correct_answer = html.unescape(record["correct_answer"])
        incorrect_answers = [html.unescape(ans) for ans in record["incorrect_answers"]]
        
        # 2. Combine all answers into a single list for easier querying
        all_answers = incorrect_answers + [correct_answer]
        
        # 3. Create the clean document for MongoDB
        clean_record = {
            "category": record["category"],
            "difficulty": record["difficulty"],
            "question": question_text,
            "correct_answer": correct_answer,
            "all_answers": all_answers,
            "ingestion_timestamp": datetime.now(timezone.utc) # Use timezone-aware UTC time
        }
        transformed_records.append(clean_record)
        
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
        # Establish connection to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Insert the data
        # Using delete_many() + insert_many() makes the script idempotent
        # (running it multiple times won't create duplicate data)
        collection.delete_many({}) # Clear old data from the collection
        result = collection.insert_many(data)
        
        print(f"Successfully loaded {len(result.inserted_ids)} documents into '{COLLECTION_NAME}'.")
        
        # Close the connection
        client.close()
    except Exception as e:
        print(f"Error during MongoDB load: {e}")

def main():
    """
    Main function to run the ETL pipeline.
    """
    print("--- Starting ETL Pipeline ---")
    raw_questions = extract_data_from_api()
    
    if raw_questions:
        transformed_questions = transform_data(raw_questions)
        load_data_to_mongodb(transformed_questions)
    
    print("--- ETL Pipeline Finished ---")

if __name__ == "__main__":
    main()