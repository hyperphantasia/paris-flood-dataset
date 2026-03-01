#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
# File name: fluctuat_nec_mergitur.py
# Author: ΣΑΠΦΡΩΝ ΕΡΩΣ
# Date created: 2026-02-18
# Version = "1.0"
# License =  "CC0 1.0"
# Listening = "El tiempo de la revolución · Erik Truffaz"
# =============================================================================
""" Collects the Seine river daily max water levels through the Hub'Eau API 
and generates the Paris flood dataset."""
# =============================================================================

# Imports
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Tuple

# Parameters
API_BASE = "https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab"
METRIC = "HIXnJ"  # daily max height code
OUTPUT = "paris_flood_dataset.csv"
MAX_PER_PAGE = 20000  # per the API docs
MAX_WORKERS = 5

STATIONS = [
    "F700000109",  # before 1966
    "F700000110",  # 1966-1973
    "F700000111",  # 1974-1989
    "F700000102",  # 1990-2006 and after (unofficial)
    "F700000103",  # after 2006 (official)
]

# Session for connection reuse
session = requests.Session()


# ---------------------------------------------------------------------------
# Functions
def fetch_station(station: str) -> Optional[pd.DataFrame]:
    """
    Fetch data for a specific station from the Hubeau API.

    Args:
        station: Station code to fetch data for.

    Returns:
        pd.DataFrame: A DataFrame containing the station's data, or None if no data is found.
    """
    print(f"Processing station {station}")

    start_date = "1900-01-01"
    all_pages = []

    while True:
        params = {
            "code_entite": station,
            "grandeur_hydro_elab": METRIC,
            "date_debut_obs_elab": start_date,
            "size": MAX_PER_PAGE,
        }

        try:
            r = session.get(API_BASE, params=params, timeout=60)
            r.raise_for_status()
            data = r.json().get("data", [])
        except requests.RequestException as e:
            print(f"Error fetching data for station {station}: {e}")
            return None

        if not data:
            break

        df_page = pd.DataFrame(data)
        all_pages.append(df_page)

        print(f"   From {station} → {len(df_page)} rows")

        # Advance using last date + 1 day
        last_date = df_page["date_obs_elab"].max()
        start_date = (pd.to_datetime(last_date) +
                      pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        # Break if this is the last page
        if len(data) < MAX_PER_PAGE:
            break

    if not all_pages:
        return None

    # Combine all pages into one DataFrame
    df_station = pd.concat(all_pages, ignore_index=True)
    
    return df_station


def detect_global_gaps(df: pd.DataFrame, date_col: str = "date_obs_elab") -> Dict[str, any]:
    """
    Detect missing dates and gaps in the global time series.

    Args:
        df: The combined DataFrame with data from multiple stations.
        date_col: The name of the column containing the observation date.

    Returns:
        dict: A summary report containing start and end dates, missing day count, and missing ranges.
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    # Remove duplicates across stations, keeping the first occurrence
    df = df.sort_values(date_col)
    duplicates = df[df.duplicated(date_col)]

    # Drop duplicates and get unique dates
    df_unique = df.drop_duplicates(date_col)

    # Get the full date range and the observed dates
    start = df_unique[date_col].min()
    end = df_unique[date_col].max()
    full_range = pd.date_range(start, end, freq="D")
    observed = pd.Series(df_unique[date_col].unique())

    # Find missing days
    missing_days = full_range.difference(observed)

    # Build continuous missing intervals
    missing_ranges = []
    if not missing_days.empty:
        gap_start = missing_days[0]
        prev_day = missing_days[0]

        for current_day in missing_days[1:]:
            if current_day - prev_day > pd.Timedelta(days=1):
                missing_ranges.append((gap_start, prev_day))
                gap_start = current_day
            prev_day = current_day

        missing_ranges.append((gap_start, prev_day))

    # Generate summary
    summary = {
        "start_date": start,
        "end_date": end,
        "expected_days": len(full_range),
        "observed_days": len(df_unique),
        "missing_days_count": len(missing_days),
        "duplicate_records_count": len(duplicates),
        "missing_ranges": missing_ranges,
    }

    return summary


def concurrent_fetch(stations: List[str]) -> pd.DataFrame:
    """
    Fetch data for all stations concurrently using multiple threads.

    Args:
        stations: List of station codes to fetch data for.

    Returns:
        pd.DataFrame: Combined DataFrame containing data from all stations.
    """
    all_data = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(
            fetch_station, st): st for st in stations}

        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                all_data.append(result)

    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


# ---------------------------------------------------------------------------
# Main execution
if __name__ == "__main__":
    df_all = concurrent_fetch(STATIONS)

    if not df_all.empty:
        # Normalize date column, sort, and drop duplicates once
        df_all["date_obs_elab"] = pd.to_datetime(df_all["date_obs_elab"])
        df_sorted = df_all.sort_values(by="date_obs_elab", ascending=True).drop_duplicates(
            "date_obs_elab", keep="first")
        df_sorted.to_csv(OUTPUT, index=False)
        print(f"\nSorted and saved data to {OUTPUT}")

        # Generate and print gap report from the saved/sorted DataFrame
        report = detect_global_gaps(df_sorted)
        for k, v in report.items():
            print(f"{k}: {v}")
    else:
        print("No data collected.")
