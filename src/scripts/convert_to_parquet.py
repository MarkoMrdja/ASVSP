#!/usr/bin/env python3
"""
Convert EPA AQS hourly CSV files (zipped) to Parquet format.

Reads zipped CSVs from data/raw/{pollutant}/ and writes Parquet files
partitioned by year to data/raw/{pollutant}/year={year}/.

Usage:
    python convert_to_parquet.py [--input-dir DIR] [--pollutant NAME]

Requires: pandas, pyarrow
"""

import argparse
import glob
import os
import re
import zipfile

import pandas as pd

# EPA hourly CSV schema
EPA_DTYPES = {
    "State Code": str,
    "County Code": str,
    "Site Num": str,
    "Parameter Code": "Int64",
    "POC": "Int64",
    "Latitude": float,
    "Longitude": float,
    "Datum": str,
    "Parameter Name": str,
    "Date Local": str,
    "Time Local": str,
    "Date GMT": str,
    "Time GMT": str,
    "Sample Measurement": float,
    "Units of Measure": str,
    "MDL": float,
    "Uncertainty": float,
    "Qualifier": str,
    "Method Type": str,
    "Method Code": str,
    "Method Name": str,
    "State Name": str,
    "County Name": str,
    "Date of Last Change": str,
}

POLLUTANTS = ["O3", "PM25", "PM10", "NO2", "SO2", "CO"]


def convert_zip_to_parquet(zip_path, output_dir):
    # Extract year from filename, e.g. hourly_44201_2019.zip
    match = re.search(r"_(\d{4})\.zip$", os.path.basename(zip_path))
    if not match:
        print(f"  Skipping {zip_path}: cannot parse year from filename")
        return

    year = match.group(1)
    parquet_dir = os.path.join(output_dir, f"year={year}")
    parquet_path = os.path.join(parquet_dir, "data.parquet")

    if os.path.exists(parquet_path):
        print(f"  Skipping year={year} (already converted)")
        return

    print(f"  Converting year={year}...")
    os.makedirs(parquet_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
        if not csv_files:
            print(f"  No CSV found in {zip_path}")
            return
        with zf.open(csv_files[0]) as f:
            df = pd.read_csv(f, dtype=EPA_DTYPES, low_memory=False)

    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
    print(f"  Written {parquet_path} ({size_mb:.1f} MB, {len(df):,} rows)")


def main():
    parser = argparse.ArgumentParser(description="Convert EPA CSV zips to Parquet")
    parser.add_argument("--input-dir", type=str, default="data/raw")
    parser.add_argument(
        "--pollutant",
        type=str,
        default=None,
        help="Process a single pollutant (default: all)",
    )
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

        print(f"\nConverting {pollutant} ({len(zip_files)} files)...")
        for zip_path in zip_files:
            convert_zip_to_parquet(zip_path, pollutant_dir)

    print("\nConversion complete!")


if __name__ == "__main__":
    main()
