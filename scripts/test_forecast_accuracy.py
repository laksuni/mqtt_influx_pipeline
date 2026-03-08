#!/usr/bin/env python3
"""
Forecast Accuracy Test
----------------------
Now located in scripts/ folder.

Tests forecast accuracy for a specific week by training on all data before a cutoff date,
and writes the results to InfluxDB (measurement: forecast_accuracy).
Optionally stores the detailed forecast points.

Run inside Docker container from project root:
  docker exec -it iot_analytics python /app/scripts/test_forecast_accuracy.py --cutoff 2026-02-20 --days 7
  docker exec -it iot_analytics python /app/scripts/test_forecast_accuracy.py --cutoff 2026-02-20 --days 7 --store-details
"""

import argparse
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to path to import src modules
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import Config
from src.forecaster import get_influx_token, fetch_hourly_history, generate_forecast

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# -----------------------------------------------------------------------------
# LOGGING SETUP
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger("AccuracyTest")

# -----------------------------------------------------------------------------
# HELPER: FETCH ACTUAL DATA FROM INFLUXDB
# -----------------------------------------------------------------------------
def fetch_actuals(query_api, sensor_id, start, stop):
    """
    Fetch actual cleaned data for a given time range using parameterized query.
    start and stop must be RFC3339 strings (e.g., "2026-02-20T00:00:00Z").
    """
    query = f"""
    from(bucket: "{Config.INFLUX_BUCKET}")
      |> range(start: time(v: start_time), stop: time(v: stop_time))
      |> filter(fn: (r) => r["_measurement"] == "sensor_data_clean")
      |> filter(fn: (r) => r["sensor_id"] == "{sensor_id}")
      |> filter(fn: (r) => r["_field"] == "people")
      |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """
    params = {"start_time": start, "stop_time": stop}
    try:
        df = query_api.query_data_frame(query, params=params)
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={'_time': 'ds', 'people': 'y'})
        df['ds'] = pd.to_datetime(df['ds']).dt.tz_localize(None)
        return df[['ds', 'y']]
    except Exception as e:
        logger.error(f"Query failed for {sensor_id}: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# MAIN ACCURACY TEST FUNCTION
# -----------------------------------------------------------------------------
def run_accuracy_test(cutoff_date, forecast_days=7, store_details=False):
    """
    Train on data before cutoff_date, forecast for forecast_days after cutoff,
    compare with actuals, print error metrics, and write results to InfluxDB.
    If store_details=True, also write the hourly forecast points to InfluxDB.
    """
    token = get_influx_token()
    if not token:
        logger.error("No InfluxDB token found. Exiting.")
        return

    client = InfluxDBClient(
        url=Config.INFLUX_URL,
        token=token,
        org=Config.INFLUX_ORG,
        timeout=30_000
    )
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Convert cutoff to pandas Timestamp (naive)
    cutoff = pd.Timestamp(cutoff_date).tz_localize(None)
    test_end = cutoff + timedelta(days=forecast_days)

    # Prepare RFC3339 strings for InfluxDB queries
    start_str = cutoff.isoformat() + "Z"
    end_str = test_end.isoformat() + "Z"

    results = {}
    summary_points = []
    detail_points = [] if store_details else None

    for sensor_id in Config.SENSOR_IDS:
        logger.info(f"Testing {sensor_id}...")

        # 1. Fetch training data (last 60 days, then filter to before cutoff)
        train_df = fetch_hourly_history(query_api, sensor_id, days=60)
        if train_df.empty:
            logger.warning(f"  No training data for {sensor_id}")
            continue
        train_df = train_df[train_df['ds'] < cutoff].copy()
        if len(train_df) < 48:
            logger.warning(f"  Insufficient training data for {sensor_id} (got {len(train_df)} points)")
            continue

        # 2. Generate forecast for forecast_days
        try:
            forecast_df = generate_forecast(train_df, sensor_id, forecast_days * 24)
        except Exception as e:
            logger.error(f"  Forecast generation failed for {sensor_id}: {e}")
            continue

        # 3. Fetch actuals for the test period
        actual_df = fetch_actuals(query_api, sensor_id, start_str, end_str)
        if actual_df.empty:
            logger.warning(f"  No actual data for test period for {sensor_id}")
            continue

        # 4. Merge on ds (inner join)
        merged = pd.merge(actual_df, forecast_df[['ds', 'yhat']], on='ds', how='inner')
        if merged.empty:
            logger.warning(f"  No overlapping timestamps for {sensor_id}")
            continue

        # 5. Compute error metrics
        merged['error'] = merged['y'] - merged['yhat']
        mae = merged['error'].abs().mean()
        rmse = np.sqrt((merged['error'] ** 2).mean())
        # MAPE: avoid division by zero
        mape = (merged['error'].abs() / merged['y'].replace(0, np.nan)).mean() * 100

        results[sensor_id] = {
            'MAE': mae,
            'RMSE': rmse,
            'MAPE': mape,
            'n_points': len(merged)
        }
        logger.info(f"  {sensor_id}: MAE={mae:.2f}, RMSE={rmse:.2f}, MAPE={mape:.1f}% (n={len(merged)})")

        # 6. Prepare summary InfluxDB point
        now = datetime.now(timezone.utc)
        summary_point = Point("forecast_accuracy") \
            .tag("sensor_id", sensor_id) \
            .tag("cutoff_date", cutoff_date) \
            .field("MAE", float(mae)) \
            .field("RMSE", float(rmse)) \
            .field("MAPE", float(mape)) \
            .field("n_points", int(len(merged))) \
            .time(now)
        summary_points.append(summary_point)

        # 7. Optionally store detailed forecast points
        if store_details:
            # Write each forecast point
            for _, row in forecast_df.iterrows():
                ts_utc = row['ds'].replace(tzinfo=timezone.utc)
                detail_point = Point("forecast_test_details") \
                    .tag("sensor_id", sensor_id) \
                    .tag("cutoff_date", cutoff_date) \
                    .tag("type", "forecast") \
                    .field("yhat", float(row['yhat'])) \
                    .field("yhat_lower", float(row['yhat_lower'])) \
                    .field("yhat_upper", float(row['yhat_upper'])) \
                    .time(ts_utc)
                detail_points.append(detail_point)
            # Write actuals for comparison
            for _, row in actual_df.iterrows():
                ts_utc = row['ds'].replace(tzinfo=timezone.utc)
                actual_point = Point("forecast_test_details") \
                    .tag("sensor_id", sensor_id) \
                    .tag("cutoff_date", cutoff_date) \
                    .tag("type", "actual") \
                    .field("y", float(row['y'])) \
                    .time(ts_utc)
                detail_points.append(actual_point)

    # Write summary points
    if summary_points:
        try:
            write_api.write(bucket=Config.INFLUX_BUCKET, record=summary_points)
            logger.info(f"-> Wrote {len(summary_points)} accuracy summary records to InfluxDB")
        except Exception as e:
            logger.error(f"Failed to write summary to InfluxDB: {e}")

    # Write detail points if any
    if detail_points:
        try:
            batch_size = 5000
            for i in range(0, len(detail_points), batch_size):
                write_api.write(bucket=Config.INFLUX_BUCKET, record=detail_points[i:i+batch_size])
            logger.info(f"-> Wrote {len(detail_points)} detailed forecast points to InfluxDB")
        except Exception as e:
            logger.error(f"Failed to write details to InfluxDB: {e}")

    client.close()
    return results

# -----------------------------------------------------------------------------
# COMMAND‑LINE ENTRY POINT
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Forecast accuracy test")
    parser.add_argument('--cutoff', required=True, help='Cutoff date (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=7, help='Number of days to forecast (default: 7)')
    parser.add_argument('--store-details', action='store_true', help='Store hourly forecast and actual points')
    args = parser.parse_args()

    run_accuracy_test(args.cutoff, args.days, store_details=args.store_details)
