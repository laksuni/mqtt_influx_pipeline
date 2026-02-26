"""
Time Series Forecasting Module
------------------------------
Uses Facebook Prophet to predict future sensor trends based on cleaned historical data.
This module is designed to run as a scheduled batch job.

Logic:
1. Reads cleaned CSVs from data/processed/.
2. Trains a Prophet model on the 'people' count (logistic growth bounded by sensor capacity).
3. Forecasts the next 168 hours (1 week).
4. Writes prediction (yhat) and confidence intervals (yhat_lower/upper) to InfluxDB,
   with timestamps correctly converted to UTC.
5. (Optional) Backtest mode to evaluate accuracy on past data.
"""

import logging
import pandas as pd
import numpy as np
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

# -----------------------------------------------------------------------------
# EARLY CONFIGURATION LOADING (BEFORE IMPORTING src.config)
# -----------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print(f"Loaded .env from {env_path}")
    else:
        print(f".env not found at {env_path}, relying on system environment")
except ImportError:
    print("python-dotenv not installed, skipping .env loading")

if not os.getenv("INFLUX_TOKEN"):
    secrets_file = Path(__file__).parent.parent / 'secrets' / 'influx_token.txt'
    if secrets_file.exists():
        with open(secrets_file, 'r') as f:
            token = f.read().strip()
            os.environ["INFLUX_TOKEN"] = token
            print(f"Loaded INFLUX_TOKEN from {secrets_file}")
    else:
        print(f"Secrets file not found: {secrets_file}")

# -----------------------------------------------------------------------------
# Now import the rest
# -----------------------------------------------------------------------------
import pytz
from prophet import Prophet
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from src.config import Config

# -----------------------------------------------------------------------------
# LOGGING SETUP
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Forecaster")

# Silence Stan/Prophet logs
logging.getLogger('cmdstanpy').setLevel(logging.WARNING)
logging.getLogger('prophet').setLevel(logging.WARNING)

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
SENSORS_TO_FORECAST: List[Dict[str, str]] = [
    {"name": "aiot", "file": "aiot_clean.csv"},
    {"name": "robo", "file": "robo_clean.csv"},
    {"name": "simulated", "file": "simulated_clean.csv"}
]

MIN_DATA_POINTS = 24 * 7 * 2   # at least 2 weeks of hourly data for weekly forecast
FORECAST_PERIODS = 24 * 7       # 168 hours = 1 week

# Timezone for conversion
HELSINKI_TZ = pytz.timezone('Europe/Helsinki')

# -----------------------------------------------------------------------------
# CORE PRODUCTION FUNCTION
# -----------------------------------------------------------------------------
def run_forecasting_cycle():
    """
    Main entry point called by the Scheduler.
    Iterates through all defined sensors and runs the prediction pipeline.
    """
    logger.info("Starting Forecasting Cycle...")

    if not Config.INFLUX_TOKEN:
        logger.warning("No InfluxDB Token found. Skipping write step.")
        return

    try:
        client = InfluxDBClient(
            url=Config.INFLUX_URL,
            token=Config.INFLUX_TOKEN,
            org=Config.INFLUX_ORG
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)

        for sensor in SENSORS_TO_FORECAST:
            _process_single_sensor(sensor["name"], sensor["file"], write_api)

        client.close()
        logger.info("Forecasting Cycle Complete.")

    except Exception as e:
        logger.error(f"Critical error in forecasting cycle: {e}")

def _process_single_sensor(sensor_name: str, filename: str, write_api,
                           forecast_periods: int = FORECAST_PERIODS,
                           save_csv: bool = True):
    """
    Handles the load -> train -> predict -> write pipeline for a single sensor.
    """
    file_path = Config.PROCESSED_DIR / filename

    if not file_path.exists():
        logger.debug(f"Skipping {sensor_name}: File not found ({filename})")
        return

    try:
        # 1. Load Data
        df = pd.read_csv(file_path)

        required_cols = {'timestamp', 'people'}
        if not required_cols.issubset(df.columns):
            logger.warning(f"Skipping {sensor_name}: Missing columns {required_cols - set(df.columns)}")
            return

        if len(df) < MIN_DATA_POINTS:
            logger.info(f"Skipping {sensor_name}: Not enough data ({len(df)} rows, need {MIN_DATA_POINTS})")
            return

        # 2. Prepare for Prophet – convert timestamp to naive (Helsinki local time)
        df['ds'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
        df['y'] = df['people']

        # 3. Get sensor capacity and set up logistic growth
        capacity = Config.SENSOR_CAPACITY.get(sensor_name, 50)
        df['cap'] = capacity
        df['floor'] = 0

        # 4. Train Model with logistic growth (bounds predictions between 0 and capacity)
        m = Prophet(growth='logistic',
                    daily_seasonality=True,
                    weekly_seasonality=True,
                    yearly_seasonality=False)
        m.fit(df)

        # 5. Forecast into the future – future dataframe must also contain cap/floor
        future = m.make_future_dataframe(periods=forecast_periods, freq='h')
        future['cap'] = capacity
        future['floor'] = 0
        forecast = m.predict(future)

        # 6. Filter for future data only (use UTC, then make naive for comparison)
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        future_forecast = forecast[forecast['ds'] > (now_utc - timedelta(hours=1))]

        if future_forecast.empty:
            logger.info(f"{sensor_name}: No future data generated.")
            return

        # 7. Save forecast to CSV (optional)
        if save_csv:
            try:
                csv_path = Config.PROCESSED_DIR / f"{sensor_name}_forecast_debug.csv"
                future_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv(csv_path, index=False)
                logger.info(f"Forecast saved to {csv_path}")
            except PermissionError:
                logger.warning(f"Cannot save CSV to {csv_path} (permission denied). Continuing to InfluxDB write.")
            except Exception as e:
                logger.warning(f"Unexpected error saving CSV: {e}")

        # 8. Write to InfluxDB – convert timestamps to UTC
        points = []
        for _, row in future_forecast.iterrows():
            # row['ds'] is naive, but conceptually represents Helsinki local time
            ts_helsinki = row['ds']
            ts_helsinki_aware = HELSINKI_TZ.localize(ts_helsinki)
            ts_utc_aware = ts_helsinki_aware.astimezone(pytz.UTC)   # now correct UTC instant

            p = Point("people_count_forecast") \
                .tag("sensor_id", f"{sensor_name}_pred") \
                .field("count_pred", float(row['yhat'])) \
                .field("count_lower", float(row['yhat_lower'])) \
                .field("count_upper", float(row['yhat_upper'])) \
                .time(ts_utc_aware)   # aware datetime – InfluxDB client stores as UTC
            points.append(p)

        write_api.write(bucket=Config.INFLUX_BUCKET, record=points)
        logger.info(f"-> Forecast written for {sensor_name} ({len(points)} points)")

    except Exception as e:
        logger.error(f"Failed to process {sensor_name}: {e}")

# -----------------------------------------------------------------------------
# BACKTESTING FUNCTION (unchanged – works locally with naive timestamps)
# -----------------------------------------------------------------------------
def backtest_sensor(sensor_name: str, filename: str,
                    train_weeks: int = 3, test_weeks: int = 1,
                    output_csv: Optional[str] = None,
                    write_api: Optional = None):
    """
    Backtest forecast accuracy by training on a past period and forecasting a subsequent period.
    If write_api is provided, results are also written to InfluxDB.
    """
    file_path = Config.PROCESSED_DIR / filename
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return

    df = pd.read_csv(file_path)
    df['ds'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
    df['y'] = df['people']

    if len(df) < (train_weeks + test_weeks) * 24 * 0.8:
        logger.error(f"Not enough data for backtest. Need ~{(train_weeks+test_weeks)*24} hours.")
        return

    # Use timezone‑aware now for consistency, then make naive
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    test_end = today
    test_start = test_end - timedelta(days=7 * test_weeks)
    train_end = test_start
    train_start = train_end - timedelta(days=7 * train_weeks)

    logger.info(f"Backtest for {sensor_name}:")
    logger.info(f"  Train: {train_start.date()} to {train_end.date()}")
    logger.info(f"  Test:  {test_start.date()} to {test_end.date()}")

    train_df = df[(df['ds'] >= train_start) & (df['ds'] < train_end)].copy()
    test_df = df[(df['ds'] >= test_start) & (df['ds'] < test_end)].copy()

    if len(train_df) < 24 * train_weeks * 0.5:
        logger.warning(f"Training data too sparse: {len(train_df)} points")
        return
    if len(test_df) < 24 * test_weeks * 0.5:
        logger.warning(f"Test data too sparse: {len(test_df)} points")
        return

    # Use logistic growth for consistency
    capacity = Config.SENSOR_CAPACITY.get(sensor_name, 50)
    train_df['cap'] = capacity
    train_df['floor'] = 0

    m = Prophet(growth='logistic',
                daily_seasonality=True,
                weekly_seasonality=True,
                yearly_seasonality=False)
    m.fit(train_df)

    # Future dataframe must have cap/floor
    future = pd.DataFrame({'ds': pd.date_range(start=test_start, end=test_end, freq='h', inclusive='left')})
    future['cap'] = capacity
    future['floor'] = 0
    forecast = m.predict(future)

    comparison = test_df[['ds', 'y']].merge(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], on='ds', how='inner')
    if comparison.empty:
        logger.warning("No overlapping timestamps between forecast and test data.")
        return

    comparison['error'] = comparison['y'] - comparison['yhat']
    mae = comparison['error'].abs().mean()
    rmse = np.sqrt((comparison['error'] ** 2).mean())
    mape = (comparison['error'].abs() / comparison['y'].replace(0, np.nan)).mean() * 100

    logger.info(f"Backtest results for {sensor_name}:")
    logger.info(f"  MAE : {mae:.2f} people")
    logger.info(f"  RMSE: {rmse:.2f} people")
    logger.info(f"  MAPE: {mape:.1f}%")

    # Save to CSV if requested
    if output_csv:
        comp_path = Config.PROCESSED_DIR / output_csv
        try:
            comparison.to_csv(comp_path, index=False)
            logger.info(f"Comparison saved to {comp_path}")
        except PermissionError:
            logger.warning(f"Cannot save CSV to {comp_path} (permission denied).")
        except Exception as e:
            logger.warning(f"Unexpected error saving CSV: {e}")

    # Write to InfluxDB if write_api provided
    if write_api is not None:
        points = []
        for _, row in comparison.iterrows():
            # Convert naive Helsinki timestamp to UTC
            ts_helsinki = row['ds']
            ts_helsinki_aware = HELSINKI_TZ.localize(ts_helsinki)
            ts_utc_aware = ts_helsinki_aware.astimezone(pytz.UTC)

            p = Point("forecast_backtest") \
                .tag("sensor_id", sensor_name) \
                .field("y", float(row['y'])) \
                .field("yhat", float(row['yhat'])) \
                .field("yhat_lower", float(row['yhat_lower'])) \
                .field("yhat_upper", float(row['yhat_upper'])) \
                .field("error", float(row['error'])) \
                .time(ts_utc_aware)
            points.append(p)

        # Write in batches of 1000
        batch_size = 1000
        try:
            for i in range(0, len(points), batch_size):
                write_api.write(bucket=Config.INFLUX_BUCKET, record=points[i:i+batch_size])
            logger.info(f"  -> Backtest results for {sensor_name} written to InfluxDB ({len(points)} points)")
        except Exception as e:
            logger.error(f"Failed to write backtest results to InfluxDB for {sensor_name}: {e}")

    return comparison
# -----------------------------------------------------------------------------
# COMMAND-LINE INTERFACE
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Forecast sensor data with Prophet.")
    parser.add_argument("--backtest", action="store_true",
                        help="Run backtest on historical data instead of production forecast.")
    parser.add_argument("--sensor", type=str, choices=["aiot", "robo", "simulated", "all"],
                        default="all", help="Sensor to backtest (default: all)")
    parser.add_argument("--train-weeks", type=int, default=3,
                        help="Number of weeks to train on (default: 3)")
    parser.add_argument("--test-weeks", type=int, default=1,
                        help="Number of weeks to forecast/test (default: 1)")
    parser.add_argument("--output", type=str, help="Output CSV filename for comparison (optional)")
    parser.add_argument("--write-influx", action="store_true",
                        help="Write backtest results to InfluxDB (requires valid token)")

    args = parser.parse_args()

    if args.backtest:
        logger.info("Running backtest mode...")
        sensors = SENSORS_TO_FORECAST if args.sensor == "all" else \
                  [s for s in SENSORS_TO_FORECAST if s["name"] == args.sensor]

        # Set up InfluxDB client if requested
        write_api = None
        influx_client = None
        if args.write_influx:
            if not Config.INFLUX_TOKEN:
                logger.warning("No InfluxDB token found. Cannot write backtest results to InfluxDB.")
            else:
                try:
                    influx_client = InfluxDBClient(
                        url=Config.INFLUX_URL,
                        token=Config.INFLUX_TOKEN,
                        org=Config.INFLUX_ORG
                    )
                    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
                    logger.info("InfluxDB client initialized for backtest writes.")
                except Exception as e:
                    logger.error(f"Failed to initialize InfluxDB client: {e}")

        for sensor in sensors:
            out_file = f"{sensor['name']}_backtest.csv" if args.output is None else args.output
            backtest_sensor(
                sensor["name"],
                sensor["file"],
                train_weeks=args.train_weeks,
                test_weeks=args.test_weeks,
                output_csv=out_file,
                write_api=write_api
            )

        if influx_client:
            influx_client.close()
    else:
        run_forecasting_cycle()
