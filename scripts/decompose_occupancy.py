#!/usr/bin/env python3
"""
Time Series Decomposition
-------------------------
Fetches cleaned occupancy data from InfluxDB, performs STL decomposition,
and writes the trend, seasonal, and residual components back to InfluxDB.

Usage (run inside Docker container from project root):
  docker exec -it iot_analytics python /app/scripts/decompose_occupancy.py --sensor robo --days 30
  docker exec -it iot_analytics python /app/scripts/decompose_occupancy.py --sensor aiot --start 2026-02-01 --end 2026-03-01
"""

import argparse
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from statsmodels.tsa.seasonal import STL

# Local imports – adjust path to import from src
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import Config
from src.forecaster import get_influx_token  # reuse the token helper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger("Decompose")

def fetch_clean_data(query_api, sensor_id: str, start: str, stop: str) -> pd.DataFrame:
    """
    Fetch cleaned hourly occupancy data for a given time range.
    Returns DataFrame with index 'ds' (datetime) and column 'y' (people).
    """
    query = f"""
    from(bucket: "{Config.INFLUX_BUCKET}")
      |> range(start: {start}, stop: {stop})
      |> filter(fn: (r) => r._measurement == "sensor_data_clean")
      |> filter(fn: (r) => r.sensor_id == "{sensor_id}")
      |> filter(fn: (r) => r._field == "people")
      |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """
    try:
        df = query_api.query_data_frame(query)
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={'_time': 'ds', 'people': 'y'})
        df['ds'] = pd.to_datetime(df['ds'])
        df = df.set_index('ds').sort_index()
        # Ensure hourly frequency (fill missing with NaN, but decomposition will handle)
        df = df.asfreq('H')
        return df[['y']]
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return pd.DataFrame()

def decompose_and_write(write_api, sensor_id: str, df: pd.DataFrame):
    """
    Perform STL decomposition and write components to InfluxDB.
    """
    if df.empty or len(df) < 2 * 24:  # need at least 2 days of data
        logger.warning(f"Insufficient data for {sensor_id}, skipping decomposition.")
        return

    # Fill any remaining NaNs with forward fill (decomposition requires no missing)
    df = df.fillna(method='ffill').fillna(method='bfill')

    try:
        # STL decomposition – period=24 for daily seasonality
        stl = STL(df['y'], period=24, seasonal=13)  # seasonal window odd > period
        result = stl.fit()

        # Prepare points
        points = []
        for ts in result.trend.index:
            p = Point("decomposition") \
                .tag("sensor_id", sensor_id) \
                .time(ts.replace(tzinfo=timezone.utc))
            if not np.isnan(result.trend[ts]):
                p.field("trend", float(result.trend[ts]))
            if not np.isnan(result.seasonal[ts]):
                p.field("seasonal", float(result.seasonal[ts]))
            if not np.isnan(result.resid[ts]):
                p.field("residual", float(result.resid[ts]))
            points.append(p)

        # Write in batches
        batch_size = 5000
        for i in range(0, len(points), batch_size):
            write_api.write(bucket=Config.INFLUX_BUCKET, record=points[i:i+batch_size])
        logger.info(f"Wrote {len(points)} decomposition points for {sensor_id}")
    except Exception as e:
        logger.error(f"Decomposition failed for {sensor_id}: {e}")

def main():
    parser = argparse.ArgumentParser(description="STL decomposition of occupancy data")
    parser.add_argument('--sensor', required=True, help='Sensor ID (e.g., aiot, robo)')
    parser.add_argument('--days', type=int, help='Number of days to look back (alternative to start/end)')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date (YYYY-MM-DD)')
    args = parser.parse_args()

    token = get_influx_token()
    if not token:
        logger.error("No InfluxDB token")
        return

    client = InfluxDBClient(
        url=Config.INFLUX_URL,
        token=token,
        org=Config.INFLUX_ORG,
        timeout=30_000
    )
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Determine time range and format as RFC3339 strings (naive + "Z")
    if args.days:
        end = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC
        start = end - timedelta(days=args.days)
        start_str = start.isoformat() + "Z"
        end_str = end.isoformat() + "Z"
    elif args.start and args.end:
        # Parse dates and set to start/end of day (optional, here we use whole days)
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end = datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=1)  # include end day
        start_str = start.isoformat() + "Z"
        end_str = end.isoformat() + "Z"
    else:
        logger.error("Either --days or both --start and --end must be provided")
        return

    logger.info(f"Query range: {start_str} to {end_str}")

    # Fetch data
    df = fetch_clean_data(query_api, args.sensor, start_str, end_str)
    if df.empty:
        logger.warning(f"No data for {args.sensor} in the given range.")
        return

    logger.info(f"Fetched {len(df)} hourly points for {args.sensor}")

    # Decompose and write
    decompose_and_write(write_api, args.sensor, df)

    client.close()

if __name__ == "__main__":
    main()
