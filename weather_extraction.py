"""
Weather Data Extraction Script - Layer 1 (Data Extraction)
Fetches weather data for e-commerce order cities using the OpenWeatherMap API.
Stores raw weather data as CSV for Bronze ingestion.

Requires:
    pip install requests

Configuration:
    Set environment variable OPENWEATHER_API_KEY with your OpenWeatherMap API key.
    Free tier: https://openweathermap.org/api
"""
import os
import sys
import time
import requests
import pandas as pd

API_BASE = "https://api.openweathermap.org/data/2.5/weather"
OUTPUT_PATH = "weather_data.csv"
REQUEST_DELAY_SECONDS = 1.1  # stay within free-tier rate limits (60 calls/min)


def extract_weather(cities, api_key=None):
    """
    Fetch current weather for a list of cities.
    Returns a pandas DataFrame with weather attributes per city.
    """
    if not api_key:
        api_key = os.getenv("OPENWEATHER_API_KEY")

    records = []
    for city in cities:
        params = {
            "q": city,
            "appid": api_key,
            "units": "metric",
        }
        try:
            response = requests.get(API_BASE, params=params, timeout=30)
            if response.status_code == 401:
                print(f"WARNING: Invalid OpenWeatherMap API key. Skipping {city}.")
                continue
            if response.status_code != 200:
                print(f"WARNING: Could not fetch weather for {city} (HTTP {response.status_code}).")
                continue
            data = response.json()
            records.append({
                "City": data.get("name"),
                "Weather_Temp_C": data.get("main", {}).get("temp"),
                "Weather_Humidity": data.get("main", {}).get("humidity"),
                "Weather_Condition": data.get("weather", [{}])[0].get("main"),
                "Weather_Description": data.get("weather", [{}])[0].get("description"),
                "Weather_Wind_Speed": data.get("wind", {}).get("speed"),
            })
        except requests.RequestException as e:
            print(f"WARNING: Request failed for {city}: {e}")
        time.sleep(REQUEST_DELAY_SECONDS)

    if not records:
        print("ERROR: No weather records were retrieved.")
        sys.exit(1)

    return pd.DataFrame(records)


if __name__ == "__main__":
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        print("WARNING: OPENWEATHER_API_KEY not set. Weather extraction will be skipped.")
        print("Set it and re-run: export OPENWEATHER_API_KEY=<your-key>")
        sys.exit(0)

    # Cities are discovered from the e-commerce orders CSV if present
    orders_path = "Ecommerce_2000.csv"
    if os.path.exists(orders_path):
        df_orders = pd.read_csv(orders_path)
        cities = df_orders["City"].dropna().unique().tolist()
    else:
        print(f"WARNING: {orders_path} not found. No cities to fetch weather for.")
        sys.exit(0)

    print(f"Fetching weather for {len(cities)} unique cities...")
    weather_df = extract_weather(cities, api_key)
    weather_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(weather_df)} weather records to {OUTPUT_PATH}")
