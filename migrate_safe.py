#!/usr/bin/env python3
"""
Migration script: merges raw CSV files from old and new projects,
deduplicates, sorts by UTC timestamp, and writes the raw data to InfluxDB.
Reuses validation and InfluxDB logic from src modules.
"""

import os
import sys
import json
import logging
from pathlib import Path

# -----------------------------------------------------------------------------
# EARLY CONFIGURATION LOADING (BEFORE IMPORTING src.config)
# -----------------------------------------------------------------------------
# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Try to load .env file using python-dotenv
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print(f"Loaded .env from {env_path}")
    else:
        print(f".env not found at {env_path}, relying on system environment")
except ImportError:
    print("python-dotenv not installed, skipping .env loading")

# If INFLUX_TOKEN is still not set, try reading from secrets file
if not os.getenv("INFLUX_TOKEN"):
    secrets_file = Path(__file__).parent / 'secrets' / 'influx_token.txt'
    if secrets_file.exists():
        with open(secrets_file, 'r') as f:
            token = f.read().strip()
            os.environ["INFLUX_TOKEN"] = token
            print(f"Loaded INFLUX_TOKEN from {secrets_file}")
    else:
        print(f"Secrets file not found: {secrets_file}")

# Now import the rest (Config will inherit the environment variables)
import pandas as pd
import pytz
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from src.config import Config
from src.utils import DataValidator

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Migration")

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
OLD_RAW_DIR = Path("../iot_bridge/data/raw").resolve()
NEW_RAW_DIR = Path("./data/raw").resolve()

FILES_TO_PROCESS = [
    ("aiot_raw.csv", "aiot.csv"),
    ("robo_raw.csv", "robo.csv"),
    ("simulated_raw.csv", "simulated.csv")
]

# Timezone handling (same as in collector/cleaner)
HELSINKI_TZ = pytz.timezone('Europe/Helsinki')
UTC = pytz.UTC

BATCH_SIZE = 1000

# -----------------------------------------------------------------------------
# TIMESTAMP PARSING
# -----------------------------------------------------------------------------
def parse_timestamp(ts_str):
    """
    Convert a timestamp string to a UTC datetime object.
    - If the string is timezone‑aware (e.g., includes offset), it is converted to UTC.
    - If it is naive, it is assumed to be in Europe/Helsinki and then converted to UTC.
    Returns None if parsing fails.
    """
    dt = pd.to_datetime(ts_str, errors='coerce')
    if pd.isna(dt):
        return None
    if dt.tz is None:
        # Naive → assume Helsinki
        dt = HELSINKI_TZ.localize(dt)
    # Convert to UTC
    return dt.astimezone(UTC)

# -----------------------------------------------------------------------------
# INFLUXDB WRITER
# -----------------------------------------------------------------------------
class InfluxWriter:
    def __init__(self):
        # Check token again
        if not Config.INFLUX_TOKEN:
            raise ValueError("INFLUX_TOKEN is still empty after loading attempts")
        self.client = InfluxDBClient(
            url=Config.INFLUX_URL,
            token=Config.INFLUX_TOKEN,
            org=Config.INFLUX_ORG
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.bucket = Config.INFLUX_BUCKET
        self.points = []
        logger.info("InfluxDB client initialized.")

    def add_point(self, point):
        self.points.append(point)
        if len(self.points) >= BATCH_SIZE:
            self.flush()

    def flush(self):
        if not self.points:
            return
        try:
            self.write_api.write(bucket=self.bucket, record=self.points)
            logger.info(f"Wrote {len(self.points)} points to InfluxDB.")
            self.points = []
        except Exception as e:
            logger.error(f"Failed to write batch to InfluxDB: {e}")

    def close(self):
        self.flush()
        self.client.close()

# -----------------------------------------------------------------------------
# CORE MIGRATION FUNCTION
# -----------------------------------------------------------------------------
def migrate_file(old_name, new_name, influx_writer):
    old_path = OLD_RAW_DIR / old_name
    new_path = NEW_RAW_DIR / new_name

    logger.info(f"Processing {new_name}...")

    lines_with_ts = []   # (utc_datetime, original_line)

    # Read existing new file (if any)
    if new_path.exists():
        with open(new_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("timestamp,"):
                    continue
                ts_str = line.split(',', 1)[0]
                dt = parse_timestamp(ts_str)
                if dt is not None:
                    lines_with_ts.append((dt, line))

    # Read old file
    if old_path.exists():
        with open(old_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("timestamp,"):
                    continue
                ts_str = line.split(',', 1)[0]
                dt = parse_timestamp(ts_str)
                if dt is not None:
                    lines_with_ts.append((dt, line))

    if not lines_with_ts:
        logger.info(f"  -> No data found. Skipping.")
        return

    # Sort by UTC datetime
    lines_with_ts.sort(key=lambda x: x[0])

    # Deduplicate by raw timestamp string (keep last)
    seen_ts = set()
    unique_lines = []
    for dt, line in reversed(lines_with_ts):
        ts_str = line.split(',', 1)[0]
        if ts_str not in seen_ts:
            seen_ts.add(ts_str)
            unique_lines.append(line)
    unique_lines.reverse()

    logger.info(f"  -> Merged & sorted: {len(unique_lines)} records.")

    # Write merged CSV back to new_path
    with open(new_path, 'w', encoding='utf-8') as f:
        f.write("timestamp,topic,payload\n")
        for line in unique_lines:
            f.write(line + "\n")

    # Fix permissions
    os.chmod(new_path, 0o666)

    # Replay each line into InfluxDB
    logger.info(f"  -> Replaying data to InfluxDB...")
    replayed = 0
    for line in unique_lines:
        parts = line.split(',', 2)
        if len(parts) != 3:
            continue
        ts_str, topic, payload_str = parts
        # Unescape payload (simple handling)
        if payload_str.startswith('"') and payload_str.endswith('"'):
            payload_str = payload_str[1:-1].replace('""', '"')
        else:
            payload_str = payload_str.replace('""', '"')

        try:
            data = json.loads(payload_str)
        except json.JSONDecodeError:
            logger.debug(f"Skipping line with invalid JSON: {line[:50]}...")
            continue

        timestamp = parse_timestamp(ts_str)
        if timestamp is None:
            continue

        sensor_id = data.get("id", "unknown")
        point = Point("sensor_data_raw").tag("sensor_id", sensor_id).time(timestamp)
        fields_added = False

        if sensor_id == "simulated":
            if DataValidator.validate_simulated_payload(data):
                if "T" in data:
                    point.field("temperature", float(data["T"]))
                    fields_added = True
                if "H" in data:
                    point.field("humidity", float(data["H"]))
                    fields_added = True
                if "CO2" in data:
                    point.field("co2", int(float(data["CO2"])))
                    fields_added = True
                p_val = data.get("pCount", data.get("people"))
                if p_val is not None:
                    point.field("people", int(float(p_val)))
                    fields_added = True
        else:
            count = DataValidator.extract_person_count(data)
            if count is not None:
                point.field("people", int(count))
                fields_added = True
                # Optional: log out‑of‑bounds values for debugging
                if not DataValidator.is_within_bounds(sensor_id, count):
                    logger.debug(f"Out-of-bounds value for {sensor_id}: {count}")
            # else: no count found – no field added

        if fields_added:
            influx_writer.add_point(point)
            replayed += 1

    logger.info(f"  -> Replayed {replayed} points to InfluxDB for {new_name}.")

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    if not OLD_RAW_DIR.exists():
        logger.error(f"Could not find old directory: {OLD_RAW_DIR}")
        sys.exit(1)

    # Final check for token
    if not Config.INFLUX_TOKEN:
        logger.error("INFLUX_TOKEN is not configured after all loading attempts. Aborting.")
        sys.exit(1)

    logger.info("Starting timestamp‑aware migration with InfluxDB replay...")

    influx_writer = InfluxWriter()
    try:
        for old, new in FILES_TO_PROCESS:
            migrate_file(old, new, influx_writer)
    finally:
        influx_writer.close()

    logger.info("Migration complete.")
