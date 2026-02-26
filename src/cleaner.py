"""
ETL (Extract, Transform, Load) Cleaning Module
----------------------------------------------
Responsible for processing raw MQTT logs into clean, usable data.

Pipeline:
1. READ: Ingests raw CSVs (handling complex JSON payloads).
2. CLEAN: Parses JSON, handles missing values, and removes duplicates.
3. FILTER: Applies signal processing (Median filter for noise, Mean for smoothing).
4. LOAD (DB): Writes high-resolution cleaned data to InfluxDB.
5. EXPORT (File): Aggregates data to 1-hour intervals for the Prophet Forecaster.
"""

import json
import logging
import pandas as pd
import numpy as np
import pytz
from pathlib import Path
from typing import List, Dict, Optional
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from .config import Config
HELSINKI_TZ = pytz.timezone('Europe/Helsinki')
# -----------------------------------------------------------------------------
# LOGGING SETUP
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Cleaner")

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
# Define which raw files map to which sensor types
SENSOR_MAP: List[Dict[str, str]] = [
    {"type": "aiot", "filename": "aiot.csv"},
    {"type": "robo", "filename": "robo.csv"},
    {"type": "simulated", "filename": "simulated.csv"}
]

# -----------------------------------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------------------------------


def robust_read_raw_csv(file_path: Path) -> pd.DataFrame:
    """
    Reads a CSV line-by-line to handle potential formatting errors in raw logs.
    Raw logs often contain JSON with commas, which confuses standard parsers.
    """
    rows = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            first_line = f.readline()
            if not first_line.startswith("timestamp"):
                f.seek(0)

            for line in f:
                parts = line.strip().split(',', 2)
                if len(parts) == 3:
                    timestamp_str, _, payload_str = parts
                    rows.append({
                        "timestamp": timestamp_str,
                        "payload": payload_str.strip('"').replace('""', '"')
                    })
    except Exception as e:
        logger.error(f"Read error on {file_path.name}: {e}")
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    
    # ---------------------------------------------------------
    # TIMESTAMP HARMONIZATION LOGIC
    # ---------------------------------------------------------
    # 1. Convert to Datetime objects (mixed formats are okay)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # 2. Define Timezones
    helsinki_tz = pytz.timezone('Europe/Helsinki')

    def standardize_to_helsinki_naive(ts):
        if pd.isna(ts): return ts
        
        # Check if timestamp is UTC (New Data has timezone info)
        if ts.tz is not None:
            # Convert UTC -> Helsinki
            ts_local = ts.astimezone(helsinki_tz)
            # Remove timezone info to match Old Data format
            return ts_local.tz_localize(None)
        else:
            # Old Data (Already Helsinki Naive) - Leave it alone
            return ts

    # Apply the fix
    df['timestamp'] = df['timestamp'].apply(standardize_to_helsinki_naive)
    # ---------------------------------------------------------

    # Drop invalid times and sort
    df = df.dropna(subset=['timestamp']).sort_values('timestamp')
    df = df.drop_duplicates(subset=['timestamp'], keep='last')
    
    return df
#-----------------------------------------------------------------------------
# SIGNAL PROCESSING
#-----------------------------------------------------------------------------

def apply_signal_processing(df: pd.DataFrame, sensor_type: str) -> pd.DataFrame:
    """
    Applies filters to clean sensor data.
    - Physical sensors: only cap values above known capacity.
    - Simulated sensors: optional mean filter (as before).
    """
    if 'people' not in df.columns:
        return df

    # Get capacity from Config (fallback to 50 if sensor not defined)
    max_people = Config.SENSOR_CAPACITY.get(sensor_type, 60)

    # 1. Physical Sensors: cap outliers and keep everything else
    if sensor_type in ["aiot", "robo"]:
        # Replace values above capacity with NaN (will be interpolated later)
        df.loc[df['people'] > max_people, 'people'] = np.nan
        
        # Also handle negative values (if they occur)
        df.loc[df['people'] < 0, 'people'] = np.nan

        # No rolling filter – raw values are preserved

    # 2. Simulated Sensors: optional mean filter (since they can have arbitrary values)
    elif sensor_type == "simulated":
        df['people'] = df['people'].rolling(window=3, min_periods=1).mean()

    # Interpolate any NaNs (from capping or original gaps)
    df['people'] = df['people'].interpolate(method='linear', limit_direction='both')
    df['people'] = df['people'].fillna(method='bfill').fillna(method='ffill')
    
    # Round to integer (people counts are whole numbers)
    df['people'] = df['people'].round().astype(int)
    
    return df
# -----------------------------------------------------------------------------
# CORE LOGIC
# -----------------------------------------------------------------------------
def run_cleaning_cycle():
    """Main entry point called by the Scheduler."""
    logger.info("Starting ETL Cleaning Cycle...")

    # Setup Influx Client
    influx_client = None
    write_api = None
    
    if Config.INFLUX_TOKEN:
        try:
            influx_client = InfluxDBClient(
                url=Config.INFLUX_URL,
                token=Config.INFLUX_TOKEN,
                org=Config.INFLUX_ORG
            )
            write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        except Exception as e:
            logger.error(f"InfluxDB Connection Failed: {e}")

    # Process each sensor
    for sensor in SENSOR_MAP:
        _process_single_file(sensor, write_api)

    if influx_client:
        influx_client.close()
    
    logger.info("ETL Cleaning Cycle Complete.")

def _process_single_file(sensor: Dict[str, str], write_api):
    sensor_type = sensor["type"]
    filename = sensor["filename"]
    input_path = Config.RAW_DIR / filename

    if not input_path.exists():
        logger.debug(f"Skipping {sensor_type}: Raw file not found.")
        return

    # 1. READ & PARSE
    df_raw = robust_read_raw_csv(input_path)
    if df_raw.empty:
        return

    clean_rows = []
    
    # Extract JSON Data
    for _, row in df_raw.iterrows():
        try:
            data = json.loads(row['payload'])
            entry = {"timestamp": row['timestamp']}
            
            # --- Extraction Logic ---
            if sensor_type == "simulated":
                if "T" in data: entry["temperature"] = float(data["T"])
                if "H" in data: entry["humidity"] = float(data["H"])
                if "CO2" in data: entry["co2"] = int(float(data["CO2"]))
                
                p_val = data.get("pCount", data.get("people"))
                if p_val is not None:
                    entry["people"] = int(float(p_val))
            else:
                # Check multiple keys for physical sensors
                p_val = None
                for k in ["person count", "person_count", "people_total", "pCount", "people"]:
                    if k in data and data[k] is not None:
                        p_val = int(float(data[k]))
                        break
                if p_val is not None:
                    entry["people"] = p_val

            if len(entry) > 1: # Timestamp + at least one metric
                clean_rows.append(entry)

        except (json.JSONDecodeError, ValueError):
            continue

    if not clean_rows:
        return

    # Create DataFrame
    clean_df = pd.DataFrame(clean_rows)
    clean_df = clean_df.set_index('timestamp').sort_index()

    # 2. FILTER & SMOOTH
    clean_df = apply_signal_processing(clean_df, sensor_type)
    
    # Reset index for iteration/saving
    clean_df = clean_df.reset_index()

    # 3. WRITE TO INFLUXDB (High Resolution)
    if write_api:
        try:
            points = []
            for _, row in clean_df.iterrows():
                # Convert naive Helsinki timestamp to UTC
                ts_helsinki = row['timestamp']
                ts_helsinki_aware = HELSINKI_TZ.localize(ts_helsinki)
                ts_utc_aware = ts_helsinki_aware.astimezone(pytz.UTC)

                p = Point("sensor_data_clean").tag("sensor_id", sensor_type).time(ts_utc_aware)
                
                if 'people' in row:
                    p.field("people", int(row['people']))
                if 'temperature' in row and not pd.isna(row['temperature']):
                    p.field("temperature", float(row['temperature']))
                if 'humidity' in row and not pd.isna(row['humidity']):
                    p.field("humidity", float(row['humidity']))
                if 'co2' in row and not pd.isna(row['co2']):
                    p.field("co2", int(row['co2']))
                
                points.append(p)

            # Write in chunks of 1000
            batch_size = 1000
            for i in range(0, len(points), batch_size):
                write_api.write(bucket=Config.INFLUX_BUCKET, record=points[i:i + batch_size])
            
            logger.info(f"-> InfluxDB: Wrote {len(points)} cleaned points for {sensor_type}")

        except Exception as e:
            logger.error(f"InfluxDB Write Failed for {sensor_type}: {e}")

    # 4. EXPORT FOR FORECASTER (Hourly Aggregation)
    # Resample to 1H, taking the mean
    df_hourly = clean_df.set_index('timestamp').resample('1h').mean()
    
    # Ensure integer counts after averaging
    if 'people' in df_hourly.columns:
        df_hourly['people'] = df_hourly['people'].fillna(0).round().astype(int)
    
    df_hourly = df_hourly.reset_index()
    
    out_file = Config.PROCESSED_DIR / f"{sensor_type}_clean.csv"
    df_hourly.to_csv(out_file, index=False)
    logger.info(f"-> CSV: Saved {len(df_hourly)} hourly rows to {out_file.name}")

if __name__ == "__main__":
    # Allow running directly for testing
    run_cleaning_cycle()
