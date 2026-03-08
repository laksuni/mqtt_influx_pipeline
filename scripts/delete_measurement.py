#!/usr/bin/env python3
"""
Delete data from an InfluxDB bucket with optional tag filtering.
Now located in scripts/ folder.

Usage examples (run from project root):
  # Delete entire measurement
  docker exec -it iot_analytics python /app/scripts/delete_measurement.py --bucket your-bucket --measurement people_count_forecast

  # Delete all points with a specific tag key (e.g., cutoff) – regardless of value
  docker exec -it iot_analytics python /app/scripts/delete_measurement.py --bucket your-bucket --measurement people_count_forecast --tag-key cutoff

  # Delete points with a specific tag key and value
  docker exec -it iot_analytics python /app/scripts/delete_measurement.py --bucket your-bucket --measurement people_count_forecast --tag-key cutoff --tag-value "2026-02-20T00:00:00"

  # Delete within a time range
  docker exec -it iot_analytics python /app/scripts/delete_measurement.py --bucket your-bucket --measurement people_count_forecast --start 2026-02-01 --end 2026-02-10
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path
from influxdb_client import InfluxDBClient, DeleteService
from influxdb_client.rest import ApiException

# Add project root to path to import src modules
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("delete_measurement")

def get_influx_token():
    """Resolves InfluxDB token using Config (preferred) or falls back to secrets file."""
    # 1. Use Config (which already handles Docker secrets and env vars)
    if hasattr(Config, 'INFLUX_TOKEN') and Config.INFLUX_TOKEN:
        return Config.INFLUX_TOKEN

    # 2. Fallback: read from /run/secrets (Docker swarm secrets)
    try:
        with open('/run/secrets/influx_token', 'r') as f:
            return f.read().strip()
    except Exception:
        pass

    # 3. Fallback: read from secrets file in project root (for local testing)
    try:
        secret_path = Path(__file__).parent.parent / "secrets" / "influx_token.txt"
        if secret_path.exists():
            return secret_path.read_text(encoding="utf-8").strip()
    except Exception as e:
        logger.warning(f"Could not read local secret file: {e}")

    return None

def delete_data(bucket: str, measurement: str = None,
                start: datetime = None, end: datetime = None,
                tag_key: str = None, tag_value: str = None,
                url: str = None, org: str = None):
    """
    Delete data matching the given criteria.
    If measurement is None, deletes from entire bucket (use with caution!).
    If tag_key is provided, adds predicate to filter by that tag.
    If tag_value is also provided, matches exact value; otherwise matches any value (tag_key != '').
    """
    token = get_influx_token()
    if not token:
        logger.error("No InfluxDB token found.")
        return False

    # Use Config defaults if not overridden
    if url is None:
        url = Config.INFLUX_URL
    if org is None:
        org = Config.INFLUX_ORG
        if org is None:
            import os
            org = os.getenv("INFLUX_ORG", "automaatio")

    client = InfluxDBClient(url=url, token=token, org=org, timeout=30_000)

    try:
        if not client.ping():
            logger.error(f"Cannot ping InfluxDB at {url}")
            return False
        logger.info("Connected to InfluxDB")

        # Build predicate
        predicate_parts = []
        if measurement:
            predicate_parts.append(f'_measurement="{measurement}"')
        if tag_key:
            if tag_value is not None:
                predicate_parts.append(f'{tag_key}="{tag_value}"')
            else:
                # Match any point where the tag exists (non-empty value)
                predicate_parts.append(f'{tag_key}!=""')
        predicate = " AND ".join(predicate_parts) if predicate_parts else None

        # Default time range if not provided
        if start is None:
            start = datetime(1970, 1, 1)
        if end is None:
            end = datetime.now()

        logger.info(f"Deleting data from bucket '{bucket}'")
        if measurement:
            logger.info(f"Measurement: {measurement}")
        if tag_key:
            if tag_value:
                logger.info(f"Tag: {tag_key}={tag_value}")
            else:
                logger.info(f"Tag: {tag_key} exists (any value)")
        logger.info(f"Time range: {start.isoformat()} to {end.isoformat()}")
        if predicate:
            logger.info("Predicate: " + predicate)

        response = input("Are you sure? This operation is irreversible. Type 'yes' to continue: ")
        if response.lower() != 'yes':
            logger.info("Deletion cancelled.")
            return False

        delete_api = client.delete_api()
        delete_api.delete(
            start=start,
            stop=end,
            predicate=predicate,
            bucket=bucket,
            org=org
        )
        logger.info("Deletion completed successfully.")
        return True

    except ApiException as e:
        logger.error(f"InfluxDB API error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    finally:
        client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete data from InfluxDB with optional tag filtering.")
    parser.add_argument("--url", help="InfluxDB URL (default: from Config)")
    parser.add_argument("--org", help="InfluxDB organization (default: from Config)")
    parser.add_argument("--bucket", required=True, help="Bucket name")
    parser.add_argument("--measurement", help="Measurement name to delete (if omitted, entire bucket)")
    parser.add_argument("--tag-key", help="Tag key to filter by")
    parser.add_argument("--tag-value", help="Tag value (if omitted, matches any value of the tag key)")
    parser.add_argument("--start", help="Delete start time (ISO format, e.g., 2026-01-01T00:00:00Z)")
    parser.add_argument("--end", help="Delete end time (ISO format, e.g., 2026-02-01T00:00:00Z)")
    args = parser.parse_args()

    start = datetime.fromisoformat(args.start.replace('Z', '+00:00')) if args.start else None
    end = datetime.fromisoformat(args.end.replace('Z', '+00:00')) if args.end else None

    delete_data(
        bucket=args.bucket,
        measurement=args.measurement,
        start=start,
        end=end,
        tag_key=args.tag_key,
        tag_value=args.tag_value,
        url=args.url,
        org=args.org
    )
