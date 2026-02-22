#!/usr/bin/env python3
"""
Extract EPA AQS hourly CSV files from downloaded zip archives.

Reads zipped archives from data/raw/{pollutant}/ and extracts the CSV
into data/raw/{pollutant}/ with a clean filename: hourly_{pollutant}_{year}.csv

Usage:
    python extract_csv.py [--input-dir DIR] [--pollutant NAME]
"""

import argparse
import glob
import os
import re
import zipfile

POLLUTANTS = ["O3", "PM25", "PM10", "NO2", "SO2", "CO"]


def extract_zip(zip_path, output_dir):
    match = re.search(r"_(\d{4})\.zip$", os.path.basename(zip_path))
    if not match:
        print(f"  Skipping {zip_path}: cannot parse year from filename")
        return

    year = match.group(1)
    pollutant = os.path.basename(output_dir)
    csv_name = f"hourly_{pollutant}_{year}.csv"
    csv_path = os.path.join(output_dir, csv_name)

    if os.path.exists(csv_path):
        print(f"  Skipping {csv_name} (already extracted)")
        return

    print(f"  Extracting {os.path.basename(zip_path)} -> {csv_name}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
        if not csv_files:
            print(f"  No CSV found in {zip_path}")
            return
        with zf.open(csv_files[0]) as src, open(csv_path, "wb") as dst:
            dst.write(src.read())

    size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"  Written {csv_path} ({size_mb:.1f} MB)")


def main():
    parser = argparse.ArgumentParser(description="Extract EPA CSV files from zip archives")
    parser.add_argument("--input-dir", type=str, default="data/raw")
    parser.add_argument("--pollutant", type=str, default=None)
    args = parser.parse_args()

    targets = [args.pollutant] if args.pollutant else POLLUTANTS

    for pollutant in targets:
        pollutant_dir = os.path.join(args.input_dir, pollutant)
        if not os.path.isdir(pollutant_dir):
            print(f"\nSkipping {pollutant}: directory {pollutant_dir} not found")
            continue

        zip_files = sorted(glob.glob(os.path.join(pollutant_dir, "*.zip")))
        if not zip_files:
            print(f"\nSkipping {pollutant}: no zip files found in {pollutant_dir}")
            continue

        print(f"\nExtracting {pollutant} ({len(zip_files)} files)...")
        for zip_path in zip_files:
            extract_zip(zip_path, pollutant_dir)

    print("\nExtraction complete!")


if __name__ == "__main__":
    main()
