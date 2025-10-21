# CERT.at Threat Feeds ETL Data Connector

**Author:** Harine Cellam N S 
**Roll Number:** 3122225001034

## Overview

This Python ETL connector fetches cyber threat intelligence from the CERT.at public threat feeds API. The pipeline extracts, transforms, and loads data from multiple endpoints (malware, phishing, botnet, vulnerable systems) into a MongoDB collection for security analysis.

## Quickstart

1. **Install requirements:**
pip install -r requirements.txt
2. **Configure `.env` file:**
MONGO_URI=mongodb://localhost:27017
MONGO_DB=certat_feeds
MONGO_COLLECTION=certat_raw
*(add API key if needed)*
3. **Run the connector:**
python etl_connector.py

## API Endpoints Used

| Endpoint           | Path                                      | Purpose                                |
|--------------------|-------------------------------------------|----------------------------------------|
| Malware            | /malware                                  | Fetches malware indicators             |
| Phishing           | /phishing                                 | Fetches phishing campaigns             |
| Botnet             | /botnet                                   | Fetches active botnet IPs              |
| Vulnerable Systems | /vulnerable-systems                       | Fetches vulnerable/exposed systems     |

## MongoDB Collection

All threat records are loaded into the `certat_raw` collection, with ingestion timestamps for audit and update support.

## Error Handling

- Handles connection errors, invalid API responses, and logs all pipeline steps.
- Skips over empty payloads and network failures.

## Security Practices

- All credentials and secrets stored in `.env`.
- `.env` is ignored from Git commits.

## Assignment Checklist

- [x] API provider chosen and studied (CERT.at Threat Feeds)
- [x] Multi-endpoint ETL pipeline implemented
- [x] Secure usage of `.env` for all secrets
- [x] Data transformation and MongoDB storage validated
- [x] Descriptive README and instructions provided
- [x] Project ready for branch push and submission

## Example Output

Records will include endpoint name and UTC ingestion timestamp for downstream analytics.

---

## Validation & Testing

- Endpoints tested for live data fetching and error handling
- Consistent insertion format in MongoDB confirmed

---
