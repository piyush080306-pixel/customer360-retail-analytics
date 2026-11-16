"""
Data Extraction Script - Layer 1
Downloads the public e-commerce dataset and stages it for ADLS Gen2 Bronze ingestion.
"""
import os
import sys
import requests

DATASET_URL = "https://raw.githubusercontent.com/piyush080306-pixel/customer360-retail-analytics/main/Ecommerce_2000.csv"
OUTPUT_PATH = "Ecommerce_2000.csv"


def extract_data():
    print(f"Downloading dataset from {DATASET_URL}...")
    try:
        response = requests.get(DATASET_URL, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"ERROR: Failed to download dataset: {e}")
        sys.exit(1)

    with open(OUTPUT_PATH, "wb") as f:
        f.write(response.content)

    print(f"Saved {OUTPUT_PATH} ({len(response.content)} bytes)")
    return OUTPUT_PATH


if __name__ == "__main__":
    extract_data()
