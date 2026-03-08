"""
Time Series Forecasting Module (Influx-Native)
-------------------------------------------------
Pipeline:
1. QUERY: Fetches hourly aggregated 'people' data from InfluxDB (sensor_data_clean).
2. TRAIN: Fits a Prophet model (Logistic Growth).
3. PREDICT: Generates a 7-day forecast.
4. LOAD: Writes predictions to InfluxDB (people_count_forecast).

Usage:
  docker exec -it iot_analytics python -m src.forecaster            # Run production forecast
  docker exec -it iot_analytics python -m src.forecaster --backtest # Run backtest accuracy check
"""

import logging
import argparse
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta, timezone
from pathlib import Path
from prophet import Prophet
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from .config import Config

# -----------------------------------------------------------------------------
# LOGGING & SETUP
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Forecaster")

# Silence Prophet logs
logging.getLogger('cmdstanpy').setLevel(logging.WARNING)
logging.getLogger('prophet').setLevel(logging.WARNING)

TRAINING_DAYS = 60               # How much history to fetch for training
FORECAST_HORIZON = 168            # 1 week in hours
MIN_TRAINING_HOURS = 168          # Minimum data points required (7 days) – adjust as needed

# -----------------------------------------------------------------------------
# AUTHENTICATION HELPER
# -----------------------------------------------------------------------------
def get_influx_token():
    """Resolves InfluxDB token from Env Var or local secrets file."""
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
# DATA INGESTION
# -----------------------------------------------------------------------------
def fetch_hourly_history(query_api, sensor_id: str, days: int) -> pd.DataFrame:
    """
    Queries InfluxDB for 'people' count, aggregated to 1-hour means.
    Returns DataFrame compatible with Prophet (ds, y).
    """
    logger.info(f"Fetching last {days} days of data for {sensor_id}...")
    
    query = f"""
    from(bucket: "{Config.INFLUX_BUCKET}")
      |> range(start: -{days}d)
      |> filter(fn: (r) => r["_measurement"] == "sensor_data_clean")
      |> filter(fn: (r) => r["sensor_id"] == "{sensor_id}")
      |> filter(fn: (r) => r["_field"] == "people")
      |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """
    
    try:
        df = query_api.query_data_frame(query)
        
        if df.empty:
            return pd.DataFrame()

        # 1. Rename Time
        if '_time' in df.columns:
            df = df.rename(columns={'_time': 'ds'})

        # 2. Rename Value (After pivot, the column name is the field name: "people")
        if 'people' in df.columns:
            df = df.rename(columns={'people': 'y'})
        elif '_value' in df.columns: 
            # Fallback for non-pivoted data (just in case)
            df = df.rename(columns={'_value': 'y'})
        
        # Ensure 'ds' is datetime and strip timezone for Prophet
        df['ds'] = pd.to_datetime(df['ds'])
        df['ds'] = df['ds'].dt.tz_localize(None)
        
        return df[['ds', 'y']]
        
    except Exception as e:
        logger.error(f"Query failed for {sensor_id}: {e}")
        return pd.DataFrame()
    
# -----------------------------------------------------------------------------
# FORECASTING LOGIC
# -----------------------------------------------------------------------------
def generate_forecast(df: pd.DataFrame, sensor_id: str, periods: int) -> pd.DataFrame:
    """Trains Prophet model and predicts future values."""
    
    # 1. Setup Capacity (Logistic Growth)
    capacity = Config.SENSOR_CAPACITY.get(sensor_id, 60)
    df['cap'] = capacity
    df['floor'] = 0

    # 2. Train
    m = Prophet(growth='logistic',
                daily_seasonality=True,
                weekly_seasonality=True,
                yearly_seasonality=False)
    m.fit(df)

    # 3. Predict
    future = m.make_future_dataframe(periods=periods, freq='h')
    future['cap'] = capacity
    future['floor'] = 0
    
    forecast = m.predict(future)
    
    # Clip forecast values to non-negative (Prophet can sometimes produce tiny negative values)
    forecast['yhat'] = forecast['yhat'].clip(lower=0)
    forecast['yhat_lower'] = forecast['yhat_lower'].clip(lower=0)
    forecast['yhat_upper'] = forecast['yhat_upper'].clip(lower=0)
    
    # Filter only future dates
    last_real_date = df['ds'].max()
    future_forecast = forecast[forecast['ds'] > last_real_date].copy()
    
    return future_forecast

# -----------------------------------------------------------------------------
# PRODUCTION PIPELINE
# -----------------------------------------------------------------------------
def run_forecasting_cycle():
    logger.info("--- Starting Forecasting Cycle ---")
    
    token = get_influx_token()
    if not token:
        logger.critical("Auth Failed: No Token Found.")
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
    except Exception as e:
        logger.critical(f"InfluxDB Connect Failed: {e}")
        return

    for sensor_id in Config.SENSOR_IDS:
        # 1. Fetch Data (Influx -> Pandas)
        df = fetch_hourly_history(query_api, sensor_id, TRAINING_DAYS)
        
        if len(df) < MIN_TRAINING_HOURS:
            logger.warning(f"Skipping {sensor_id}: Insufficient data ({len(df)} rows, need {MIN_TRAINING_HOURS})")
            continue

        # 2. Generate Forecast
        try:
            forecast_df = generate_forecast(df, sensor_id, FORECAST_HORIZON)
            
            if forecast_df.empty:
                continue

            # 3. Write to InfluxDB
            points = []
            for _, row in forecast_df.iterrows():
                # Convert Naive Prophet timestamp back to UTC Aware for Influx
                # Assuming the naive time was UTC (since we stripped UTC earlier)
                ts_utc = row['ds'].replace(tzinfo=timezone.utc)

                p = Point("people_count_forecast") \
                    .tag("sensor_id", sensor_id) \
                    .field("yhat", float(row['yhat'])) \
                    .field("yhat_lower", float(row['yhat_lower'])) \
                    .field("yhat_upper", float(row['yhat_upper'])) \
                    .time(ts_utc)
                points.append(p)

            write_api.write(bucket=Config.INFLUX_BUCKET, record=points)
            logger.info(f"-> Wrote {len(points)} forecast points for {sensor_id}")

        except Exception as e:
            logger.error(f"Modeling failed for {sensor_id}: {e}")

    client.close()
    logger.info("Forecasting Cycle Complete.")

# -----------------------------------------------------------------------------
# BACKTESTING PIPELINE (CLI Only)
# -----------------------------------------------------------------------------
def run_backtest_cycle():
    logger.info("--- Starting Backtest Cycle ---")
    token = get_influx_token()
    client = InfluxDBClient(url=Config.INFLUX_URL, token=token, org=Config.INFLUX_ORG)
    query_api = client.query_api()

    for sensor_id in Config.SENSOR_IDS:
        # Fetch more history for backtesting (e.g., 60 days)
        df = fetch_hourly_history(query_api, sensor_id, 60)
        
        # Use a higher threshold for backtesting (2 weeks is already used)
        if len(df) < 24 * 14:  # 2 weeks
            logger.warning(f"Skipping {sensor_id}: Not enough data for backtest.")
            continue

        # Split: Train on first 75%, Test on last 25%
        split_idx = int(len(df) * 0.75)
        train_df = df.iloc[:split_idx].copy()
        test_df = df.iloc[split_idx:].copy()

        # Train
        capacity = Config.SENSOR_CAPACITY.get(sensor_id, 60)
        train_df['cap'] = capacity
        train_df['floor'] = 0
        
        m = Prophet(growth='logistic', daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=False)
        m.fit(train_df)

        # Predict
        future = m.make_future_dataframe(periods=len(test_df), freq='h')
        future['cap'] = capacity
        future['floor'] = 0
        forecast = m.predict(future)

        # Compare
        # Merge actuals (y) with predictions (yhat)
        comparison = pd.merge(test_df, forecast[['ds', 'yhat']], on='ds')
        comparison['error'] = comparison['y'] - comparison['yhat']
        
        mae = comparison['error'].abs().mean()
        rmse = np.sqrt((comparison['error'] ** 2).mean())
        
        logger.info(f"Backtest {sensor_id} | MAE: {mae:.2f} | RMSE: {rmse:.2f}")

    client.close()

# -----------------------------------------------------------------------------
# MAIN ENTRY POINT
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backtest", action="store_true", help="Run historical backtest")
    args = parser.parse_args()

    if args.backtest:
        run_backtest_cycle()
    else:
        run_forecasting_cycle()
