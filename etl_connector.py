import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

class SpamhausETL:
    def __init__(self):
        self.base_url = "https://www.spamhaus.org"
        self.endpoint = "/drop/drop.txt"
        self.mongo_uri = os.getenv("MONGO_URI")
        self.db_name = os.getenv("DB_NAME")
        self.collection_name = os.getenv("COLLECTION_NAME")
        
    def extract(self):
        """Fetch raw DROP list data from Spamhaus"""
        try:
            response = requests.get(f"{self.base_url}{self.endpoint}", timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Extraction failed: {e}")
            return None
    
    def transform(self, raw_data):
        """Convert raw text into structured documents"""
        if not raw_data:
            return None
            
        records = []
        for line in raw_data.split('\n'):
            if not line or line.startswith(';'):
                continue  # Skip comments/empty lines
                
            parts = line.split(';')
            if len(parts) >= 2:
                records.append({
                    "ip_range": parts[0].strip(),
                    "description": parts[1].strip(),
                    "source": "Spamhaus DROP",
                    "ingested_at": datetime.utcnow()
                })
        return records
    
    def load(self, transformed_data):
        """Insert data into MongoDB"""
        if not transformed_data:
            return False
            
        try:
            client = MongoClient(self.mongo_uri)
            collection = client[self.db_name][self.collection_name]
            result = collection.insert_many(transformed_data)
            print(f"[SUCCESS] Loaded {len(result.inserted_ids)} records")
            return True
        except Exception as e:
            print(f"[ERROR] MongoDB insertion failed: {e}")
            return False
    
    def run_pipeline(self):
        """Execute full ETL workflow"""
        print("=== Starting Spamhaus ETL ===")
        
        # Extract
        print("Extracting data...")
        raw_data = self.extract()
        if not raw_data:
            return False
        
        # Transform
        print("Transforming data...")
        clean_data = self.transform(raw_data)
        if not clean_data:
            return False
        
        # Load
        print("Loading to MongoDB...")
        return self.load(clean_data)

if __name__ == "__main__":
    pipeline = SpamhausETL()
    success = pipeline.run_pipeline()
    print("=== Pipeline completed successfully ===" if success else "=== Pipeline failed ===")