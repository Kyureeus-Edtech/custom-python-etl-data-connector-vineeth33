# Custom ETL Connector: DShield Top Attackers

This project is a custom ETL (Extract, Transform, Load) data connector built in Python. It fetches a list of the top 100 attacking IP addresses from the [DShield API](https://www.dshield.org/api.html), parses the plain text data, and loads it into a MongoDB collection.

This demonstrates parsing non-JSON data and was created for the SSN College of Engineering Software Architecture assignment.

## ETL Process

* **Extract**: Fetches a raw, tab-separated text file from the DShield `ipsascii.html` endpoint.
* **Transform**:
    * Skips all comment lines and the header row.
    * Parses each tab-delimited line into a structured Python dictionary.
    * Converts string values to appropriate data types (e.g., `integer` for attack counts, `datetime` for timestamps).
    * Adds an `ingestion_timestamp` to each record.
* **Load**: Clears the target collection and inserts the new data into a MongoDB collection named `top_attackers`.

## Project Setup

### 1. Configure Environment
This API does not require an authentication key. You only need to configure your MongoDB connection.

Create a file named `.env` in the root directory and add your MongoDB connection string:

```env
MONGO_URI="mongodb://localhost:27017/"
```

### 2. Install & Run
Follow these steps to run the pipeline:

```bash
# Set up a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the ETL script
python etl_connector.py
```

## Example Output in MongoDB

After running the script, you will find documents in your MongoDB database (`dshield_db` -> `top_attackers`) with the following structure:

```json
{
  "_id": {
    "$oid": "670a7b5d8f6e1d2c3b4a9f1e"
  },
  "ip_address": "223.165.2.149",
  "reports": 487532,
  "targets": 5586,
  "attacks": 1618256,
  "first_seen": {
    "$date": "2025-08-14T00:00:00.000Z"
  },
  "last_seen": {
    "$date": "2025-08-14T08:00:00.000Z"
  },
  "location": {
    "country_code": "IN",
    "country_name": "India",
    "city": "Thiruporur"
  },
  "ingestion_timestamp": {
    "$date": "2025-08-14T14:30:00.123Z"
  }
}
```