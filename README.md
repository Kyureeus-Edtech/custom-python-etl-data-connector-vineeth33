# Custom ETL Connector: Open Trivia Database

This project is a custom ETL (Extract, Transform, Load) data connector built in Python. It fetches computer science trivia questions from the [Open Trivia Database API](https://opentdb.com/), transforms the data into a clean format, and loads it into a MongoDB collection.

This was created for the SSN College of Engineering Software Architecture assignment.

## ETL Process

- **Extract**: Fetches 15 multiple-choice questions from the "Science: Computers" category.
- **Transform**:
  - Decodes HTML entities (e.g., `&quot;` becomes `"`).
  - Combines the correct and incorrect answers into a single `all_answers` array.
  - Adds a `ingestion_timestamp` field to each document.
- **Load**: Clears the target collection and inserts the new data into a MongoDB collection named `trivia_questions_raw`.

## Project Structure

```
/
├── etl_connector.py      # The main Python script for the ETL pipeline
├── .env                  # Stores the MongoDB connection URI (not committed)
├── requirements.txt      # Lists all Python dependencies
└── README.md             # This documentation file
```

## How to Run

1.  **Clone the Repository**:

    ```bash
    git clone <your-repo-url>
    cd <your-repo-folder>
    ```

2.  **Set Up a Virtual Environment** (Recommended):

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install Dependencies**:

    ```bash
    pip install -r requirements.txt
    ```

4.  **Create `.env` File**:
    Create a file named `.env` in the root directory and add your MongoDB connection string:

    ```env
    MONGO_URI="mongodb://localhost:27017/"
    ```

5.  **Run the Script**:
    ```bash
    python etl_connector.py
    ```

## Example Output in MongoDB

After running the script, you will find documents in your MongoDB database (`ssn_assignment_db` -> `trivia_questions_raw`) with the following structure:

```json
{
  "_id": {
    "$oid": "670a5d4c9f1e8a3b7c9e5b2a"
  },
  "category": "Science: Computers",
  "difficulty": "hard",
  "question": "What was the name of the security vulnerability found in Bash in 2014?",
  "correct_answer": "Shellshock",
  "all_answers": ["Heartbleed", "Bashbug", "Stagefright", "Shellshock"],
  "ingestion_timestamp": {
    "$date": "2025-08-14T06:30:04.123Z"
  }
}
```
