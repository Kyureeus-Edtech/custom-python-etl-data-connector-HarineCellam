#!/usr/bin/env python3
"""
CERT.at Threat Feeds ETL Connector
Using actual CERT.at RSS/Atom feeds
"""

import os
import datetime as dt
import logging
import sys
import requests
import xml.etree.ElementTree as ET
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "certat_feeds")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "certat_raw")

# CERT.at RSS Feed Endpoints (These are real, publicly accessible)
BASE_URL = "https://www.cert.at"
ENDPOINTS = {
    "warnings": f"{BASE_URL}/cert-at.de.warnings.rss_2.0.xml",
    "blog": f"{BASE_URL}/cert-at.de.blog.rss_2.0.xml",
    "daily_reports": f"{BASE_URL}/cert-at.de.daily.rss_2.0.xml",
    "current_news": f"{BASE_URL}/cert-at.de.current.rss_2.0.xml",
    "specials": f"{BASE_URL}/cert-at.de.specials.rss_2.0.xml"
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_rss_feed(feed_name, url):
    """
    Extract data from CERT.at RSS feed
    """
    logger.info(f"Extracting feed '{feed_name}' from {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse RSS XML
        root = ET.fromstring(response.content)
        
        # Extract items from RSS feed
        items = []
        for item in root.findall('.//item'):
            entry = {}
            entry['title'] = item.find('title').text if item.find('title') is not None else None
            entry['link'] = item.find('link').text if item.find('link') is not None else None
            entry['description'] = item.find('description').text if item.find('description') is not None else None
            entry['pubDate'] = item.find('pubDate').text if item.find('pubDate') is not None else None
            entry['guid'] = item.find('guid').text if item.find('guid') is not None else None
            items.append(entry)
        
        logger.info(f"Successfully fetched {len(items)} records from {feed_name}")
        return items
        
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {feed_name}: {str(e)}")
        return []
    except ET.ParseError as e:
        logger.error(f"Failed to parse XML for {feed_name}: {str(e)}")
        return []


def transform(feed_name, records):
    """
    Transform: Add metadata and timestamps
    """
    logger.info(f"Transforming {len(records)} records from {feed_name}")
    
    for record in records:
        # Add feed source
        record["feed_name"] = feed_name
        record["feed_category"] = "certat_threat_intelligence"
        
        # Add ingestion timestamp
        record["ingested_at"] = dt.datetime.utcnow().isoformat()
        
        # Add data source
        record["data_source"] = "CERT.at"
    
    return records


def load_to_mongodb(records, collection):
    """
    Load: Insert transformed data into MongoDB
    """
    if not records:
        logger.info("No records to insert.")
        return
    
    try:
        # Insert records into MongoDB
        result = collection.insert_many(records, ordered=False)
        logger.info(f"Successfully inserted {len(result.inserted_ids)} records into MongoDB")
    except PyMongoError as e:
        logger.error(f"MongoDB insertion error: {str(e)}")


def main():
    """
    Main ETL Pipeline
    """
    logger.info("=" * 60)
    logger.info("CERT.at Threat Feeds ETL Pipeline Starting...")
    logger.info("=" * 60)
    
    try:
        # Connect to MongoDB
        logger.info(f"Connecting to MongoDB at {MONGO_URI}")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Test connection
        client.admin.command('ping')
        logger.info("MongoDB connection successful")
        
        # Process each endpoint
        total_records = 0
        for feed_name, url in ENDPOINTS.items():
            logger.info(f"\n--- Processing feed: {feed_name} ---")
            
            # Extract
            raw_records = extract_rss_feed(feed_name, url)
            
            # Transform
            transformed_records = transform(feed_name, raw_records)
            
            # Load
            load_to_mongodb(transformed_records, collection)
            
            total_records += len(transformed_records)
        
        logger.info("=" * 60)
        logger.info(f"ETL Pipeline Completed Successfully!")
        logger.info(f"Total records processed: {total_records}")
        logger.info("=" * 60)
        
    except PyMongoError as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        logger.error("Please ensure MongoDB is running and MONGO_URI is correct")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()