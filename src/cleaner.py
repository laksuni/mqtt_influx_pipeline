#!/usr/bin/env python3
"""
ETL Cleaning Module – Advanced Version
=======================================
Pipeline:
1. QUERY: Fetches 'sensor_data_raw' from InfluxDB.
2. ANOMALY DETECTION: Optional Hampel filter to remove isolated spikes.
3. CAPPING: Values above sensor capacity are set to NaN (handles sustained failures).
4. INTELLIGENT GAP FILLING:
   - Short gaps (< 6 hours) → linear interpolation.
   - Long gaps → seasonal fill (median of same hour from surrounding days).
5. FINAL SMOOTHING: Light EWMA for simulated sensor only.
6. LOAD: Writes to InfluxDB measurement 'sensor_data_clean'.

Manual usage:
  python3 -m src.cleaner --days 35
"""

import logging
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from scipy import signal  # for Hampel filter
from .config import Config

# -----------------------------------------------------------------------------
# TUNABLE PARAMETERS
# -----------------------------------------------------------------------------
# Hampel filter settings (set to None to disable)
USE_HAMPEL = True
HAMPEL_WINDOW = 10          # sliding window size
HAMPEL_N_SIGMA = 3          # points beyond n_sigma * MAD are replaced

# Gap filling thresholds
SHORT_GAP_HOURS = 6         # gaps shorter than this use linear interpolation
LONG_GAP_HOURS = 24         # gaps longer than this use seasonal fill (if enough history)
SEASONAL_PERIOD_HOURS = 24  # daily pattern

# Rolling average for simulated sensor
SIMULATED_ROLLING_WINDOW = 3

# -----------------------------------------------------------------------------
# LOGGING SETUP
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Cleaner")

# -----------------------------------------------------------------------------
# AUTHENTICATION HELPER
# -----------------------------------------------------------------------------
def get_influx_token():
    """Resolves InfluxDB token from environment variable or local secrets file."""
    token = getattr(Config, 'INFLUX_TOKEN', None)
    if token and str(token).strip():
        return token
    try:
        secret_path = Path(__file__).resolve().parent.parent / "secrets" / "influx_token.txt"
        if secret_path.exists():
            return secret_path.read_text("utf-8").strip()
    except Exception as e:
        logger.warning(f"Could not read local secret file: {e}")
    return None

# -----------------------------------------------------------------------------
# DATA FETCHING
# -----------------------------------------------------------------------------
def fetch_raw_data(query_api, sensor_id: str, lookback: str) -> pd.DataFrame:
    """Queries InfluxDB for raw data, pivots fields, returns DataFrame with timestamp index."""
    logger.info(f"Querying {sensor_id} with lookback: {lookback}...")
    query = f"""
    from(bucket: "{Config.INFLUX_BUCKET}")
      |> range(start: -{lookback})
      |> filter(fn: (r) => r["_measurement"] == "sensor_data_raw")
      |> filter(fn: (r) => r["sensor_id"] == "{sensor_id}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> keep(columns: ["_time", "people", "temperature", "humidity", "co2"])
    """
    try:
        df = query_api.query_data_frame(query)
        if df.empty:
            return pd.DataFrame()
        if '_time' in df.columns:
            df = df.rename(columns={'_time': 'timestamp'})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').sort_index()
        return df
    except Exception as e:
        logger.error(f"Query failed for {sensor_id}: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# ANOMALY DETECTION (Hampel Filter)
# -----------------------------------------------------------------------------
def hampel_filter(series: pd.Series, window: int = 10, n_sigma: float = 3.0) -> pd.Series:
    """
    Replaces outliers with NaN based on a sliding Hampel identifier.
    Outliers are points where |x - median(window)| > n_sigma * MAD(window).
    """
    filtered = series.copy()
    half = window // 2
    for i in range(len(series)):
        if pd.isna(series.iloc[i]):
            continue
        start = max(0, i - half)
        end = min(len(series), i + half + 1)
        window_data = series.iloc[start:end].dropna()
        if len(window_data) < 3:
            continue
        med = window_data.median()
        mad = np.median(np.abs(window_data - med))
        if mad == 0:
            continue
        if abs(series.iloc[i] - med) > n_sigma * mad:
            filtered.iloc[i] = np.nan
    return filtered

# -----------------------------------------------------------------------------
# INTELLIGENT GAP FILLING
# -----------------------------------------------------------------------------
def fill_gaps(series: pd.Series, short_hours: int = 6, long_hours: int = 24) -> pd.Series:
    """
    Combines linear interpolation (short gaps) and seasonal fill (long gaps).
    Gaps are measured in number of consecutive NaNs, not time, because frequency may be irregular.
    We'll use time difference to determine gap length in hours.
    """
    if series.isnull().sum() == 0:
        return series

    filled = series.copy()
    # Find consecutive NaN blocks
    is_na = filled.isna()
    blocks = (is_na != is_na.shift()).cumsum()
    na_blocks = blocks[is_na].unique()

    for block in na_blocks:
        block_mask = (blocks == block) & is_na
        start_idx = block_mask.idxmax()
        end_idx = block_mask[::-1].idxmax()
        # Calculate gap duration in hours
        start_time = start_idx if isinstance(start_idx, pd.Timestamp) else filled.index[start_idx]
        end_time = end_idx if isinstance(end_idx, pd.Timestamp) else filled.index[end_idx]
        duration_hours = (end_time - start_time).total_seconds() / 3600

        if duration_hours <= short_hours:
            # Linear interpolation on this block
            filled.loc[block_mask] = np.nan
            filled = filled.interpolate(method='linear', limit_area='inside')
        elif duration_hours <= long_hours:
            # Fill with seasonal median (hour of day)
            hour_vals = filled.groupby(filled.index.hour).median()
            for hour, median_val in hour_vals.items():
                mask = (filled.index.hour == hour) & block_mask
                filled.loc[mask] = median_val
        else:
            # Very long gap: we leave as NaN; will be handled by bfill/ffill later
            pass
    return filled

# -----------------------------------------------------------------------------
# SIGNAL PROCESSING
# -----------------------------------------------------------------------------
def apply_signal_processing(df: pd.DataFrame, sensor_id: str) -> pd.DataFrame:
    """
    Cleans the data with a multi‑stage pipeline:
      1. Ensure numeric types.
      2. Cap values above sensor capacity (→ NaN).
      3. Optional Hampel outlier detection.
      4. Fill gaps (short: linear, long: seasonal).
      5. Final back/forward fill for remaining edge NaNs.
      6. Gentle smoothing for simulated sensor.
    """
    if df.empty or 'people' not in df.columns:
        return df

    df = df.copy()
    # 1. Enforce numeric
    df['people'] = pd.to_numeric(df['people'], errors='coerce')

    # 2. Capacity capping (handles most extreme failures)
    max_people = Config.SENSOR_CAPACITY.get(sensor_id, 60)
    df.loc[df['people'] > max_people, 'people'] = np.nan
    df.loc[df['people'] < 0, 'people'] = np.nan

    # 3. Hampel filter (optional) – removes isolated spikes
    if USE_HAMPEL:
        df['people'] = hampel_filter(df['people'], window=HAMPEL_WINDOW, n_sigma=HAMPEL_N_SIGMA)

    # 4. Intelligent gap filling
    df['people'] = fill_gaps(df['people'], short_hours=SHORT_GAP_HOURS,
                              long_hours=LONG_GAP_HOURS)

    # 5. Final back/forward fill for any remaining NaNs (e.g., edges)
    df['people'] = df['people'].bfill().ffill()

    # 6. Light smoothing only for simulated sensor
    if sensor_id == "simulated":
        df['people'] = df['people'].rolling(window=SIMULATED_ROLLING_WINDOW, min_periods=1, center=True).mean()
        # Re‑round after smoothing
        df['people'] = df['people'].round().astype(int)
    else:
        df['people'] = df['people'].round().astype(int)

    return df

# -----------------------------------------------------------------------------
# MAIN EXECUTION
# -----------------------------------------------------------------------------
def run_cleaning_cycle(lookback_window="24h"):
    logger.info(f"--- Starting Cleaning Cycle (Window: {lookback_window}) ---")

    token = get_influx_token()
    if not token:
        logger.critical("Authentication Error: Could not find INFLUX_TOKEN")
        return

    try:
        client = InfluxDBClient(
            url=Config.INFLUX_URL,
            token=token,
            org=Config.INFLUX_ORG,
            timeout=30_000
        )
        query_api = client.query_api()
        write_api = client.write_api(write_options=SYNCHRONOUS)
        if not client.ping():
            logger.error(f"Could not ping InfluxDB at {Config.INFLUX_URL}")
            return
    except Exception as e:
        logger.critical(f"InfluxDB Connection Failed: {e}")
        return

    for sensor_id in Config.SENSOR_IDS:
        # 1. Fetch raw data
        df = fetch_raw_data(query_api, sensor_id, lookback_window)
        if df.empty:
            logger.warning(f"No raw data found for {sensor_id}.")
            continue

        # 2. Apply cleaning pipeline
        df_clean = apply_signal_processing(df, sensor_id)

        # 3. Write cleaned data to InfluxDB
        try:
            points = []
            for ts, row in df_clean.iterrows():
                p = Point("sensor_data_clean").tag("sensor_id", sensor_id).time(ts)
                if 'people' in row:
                    p.field("people", int(row['people']))
                if 'temperature' in row and not pd.isna(row['temperature']):
                    p.field("temperature", float(row['temperature']))
                if 'humidity' in row and not pd.isna(row['humidity']):
                    p.field("humidity", float(row['humidity']))
                if 'co2' in row and not pd.isna(row['co2']):
                    p.field("co2", int(row['co2']))
                points.append(p)

            batch_size = 5000
            for i in range(0, len(points), batch_size):
                write_api.write(bucket=Config.INFLUX_BUCKET, record=points[i:i+batch_size])
            logger.info(f"-> InfluxDB: Wrote {len(points)} points for {sensor_id}")
        except Exception as e:
            logger.error(f"Write failed for {sensor_id}: {e}")

    client.close()
    logger.info("Cycle Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="InfluxDB IoT Cleaner")
    parser.add_argument('--days', type=int, default=1, help='Number of days to look back')
    parser.add_argument('--hours', type=int, default=0, help='Number of hours to look back')
    args = parser.parse_args()

    if args.days > 0:
        window = f"{args.days}d"
    elif args.hours > 0:
        window = f"{args.hours}h"
    else:
        window = "24h"

    run_cleaning_cycle(lookback_window=window)
