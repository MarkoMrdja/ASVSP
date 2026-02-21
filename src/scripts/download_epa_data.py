#!/usr/bin/env python3
"""
Download EPA AQS hourly data files for all 6 pollutants.

Usage:
    python download_epa_data.py [--start-year YEAR] [--end-year YEAR] [--output-dir DIR]

Default year range: 2017–2025
Output: data/raw/{pollutant}/hourly_{param_code}_{year}.csv
"""

import argparse
import os
import urllib.request

# EPA AQS bulk download base URL
EPA_BASE_URL = "https://aqs.epa.gov/aqsweb/airdata"

# Pollutant parameter codes
POLLUTANTS = {
    "O3": 44201,
    "PM25": 88101,
    "PM10": 81102,
    "NO2": 42602,
    "SO2": 42401,
    "CO": 42101,
}

DEFAULT_START_YEAR = 2017
DEFAULT_END_YEAR = 2025


def download_file(url, dest_path):
    print(f"  Downloading {url}")
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    try:
        urllib.request.urlretrieve(url, dest_path)
        size_mb = os.path.getsize(dest_path) / (1024 * 1024)
        print(f"  Saved to {dest_path} ({size_mb:.1f} MB)")
    except Exception as e:
        print(f"  ERROR downloading {url}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Download EPA AQS hourly data")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR)
    parser.add_argument("--output-dir", type=str, default="data/raw")
    args = parser.parse_args()

    for pollutant_name, param_code in POLLUTANTS.items():
        print(f"\nDownloading {pollutant_name} (param {param_code})...")
        for year in range(args.start_year, args.end_year + 1):
            filename = f"hourly_{param_code}_{year}.zip"
            url = f"{EPA_BASE_URL}/{filename}"
            dest_path = os.path.join(args.output_dir, pollutant_name, filename)

            if os.path.exists(dest_path):
                print(f"  Skipping {filename} (already exists)")
                continue

            download_file(url, dest_path)

    print("\nDownload complete!")


if __name__ == "__main__":
    main()
