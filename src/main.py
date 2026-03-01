"""
IoT Data Collector Service
--------------------------
Entry point for the real-time data collection container.
Responsibilities:
1. Connect to MQTT Broker.
2. Receive sensor data payloads.
3. Log raw data to CSV (Local Backup/History) – can be disabled via config.
4. Validate and write cleaned data to InfluxDB (Real-time Dashboarding).
"""

import sys
import json
import time
import signal
import logging
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from typing import Dict, Any
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Local Module Imports
from .config import Config
from .utils import DataValidator

# -----------------------------------------------------------------------------
# LOGGING SETUP
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Collector")

# -----------------------------------------------------------------------------
# MAIN SERVICE CLASS
# -----------------------------------------------------------------------------
class IoTCollector:
    def __init__(self):
        """Initialize configuration, database clients, and flags."""
        self.running = True
        
        # Setup InfluxDB Client
        self._setup_influx()
        
        # Setup MQTT Client
        self.mqtt_client = mqtt.Client(client_id=f"collector_{int(time.time())}")
        self.mqtt_client.username_pw_set(Config.MQTT_USERNAME, Config.MQTT_PASSWORD)
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message
        self.mqtt_client.on_disconnect = self._on_disconnect

    def _setup_influx(self):
        """Initialize the InfluxDB connection."""
        try:
            self.influx_client = InfluxDBClient(
                url=Config.INFLUX_URL,
                token=Config.INFLUX_TOKEN,
                org=Config.INFLUX_ORG
            )
            self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
            logger.info(f"InfluxDB Client initialized (Org: {Config.INFLUX_ORG})")
        except Exception as e:
            logger.critical(f"Failed to initialize InfluxDB: {e}")
            sys.exit(1)

    # -------------------------------------------------------------------------
    # MQTT CALLBACKS
    # -------------------------------------------------------------------------
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when the client receives a CONNACK response from the server."""
        if rc == 0:
            logger.info(f"Connected to MQTT Broker: {Config.MQTT_BROKER}")
            client.subscribe(Config.MQTT_TOPIC)
            logger.info(f"Subscribed to topic: {Config.MQTT_TOPIC}")
        else:
            logger.error(f"Failed to connect via MQTT. Return code: {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """Callback for disconnect."""
        if rc != 0:
            logger.warning("Unexpected MQTT disconnection. Auto-reconnecting...")

    def _on_message(self, client, userdata, msg):
        """
        Core logic pipeline:
        RAW MSG -> JSON PARSE -> TIMESTAMP -> (optional CSV LOG) -> INFLUXDB
        """
        payload_str = msg.payload.decode('utf-8')
        
        try:
            # 1. Parse JSON
            data = json.loads(payload_str)
            
            # 2. Capture precise UTC time (used for both CSV and InfluxDB)
            ts = datetime.now(timezone.utc)
            
            # 3. Log Raw Data to CSV if enabled (configurable)
            if Config.WRITE_CSV_BACKUP:
                self._log_to_csv(msg.topic, payload_str, data, ts)
            
            # 4. Write to InfluxDB (only valid data)
            self._write_to_influx(data, ts)

        except json.JSONDecodeError:
            logger.warning(f"Received malformed JSON on {msg.topic}: {payload_str[:50]}...")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    # -------------------------------------------------------------------------
    # DATA PROCESSING
    # -------------------------------------------------------------------------
    def _log_to_csv(self, topic: str, payload_str: str, data: Dict[str, Any], timestamp: datetime):
        """Appends raw message to a device-specific CSV file using the provided timestamp."""
        try:
            # Determine filename based on sensor ID
            sensor_id = data.get("id", "unknown")
            safe_name = "".join([c for c in sensor_id if c.isalnum() or c in ('_', '-')])
            
            # Fallback if ID is empty
            if not safe_name: 
                safe_name = "unknown_device"

            file_path = Config.RAW_DIR / f"{safe_name}.csv"
            
            # Check if we need a header
            file_exists = file_path.exists()
            
            # Format timestamp as ISO 8601 (e.g., 2026-02-16T14:50:00.123456+00:00)
            timestamp_iso = timestamp.isoformat()

            # Clean payload for CSV (remove newlines)
            clean_payload = payload_str.replace('\n', ' ').replace('\r', '')

            with open(file_path, 'a', encoding='utf-8') as f:
                if not file_exists:
                    f.write("timestamp,topic,payload\n")
                f.write(f"{timestamp_iso},{topic},{clean_payload}\n")
       
        except Exception as e:
            logger.error(f"CSV Logging Failed: {e}")

    def _write_to_influx(self, data: Dict[str, Any], timestamp: datetime):
        """Writes data points to InfluxDB, always writing, with a validity tag."""
        try:
            sensor_id = data.get("id", "unknown")
            point = Point("sensor_data_raw").tag("sensor_id", sensor_id).time(timestamp)
            fields_added = False
            valid = False  # will be set to True if data passes validation

            # Logic for Simulated Sensors
            if sensor_id == "simulated":
                if DataValidator.validate_simulated_payload(data):
                    valid = True
                    # Map JSON keys to Influx Fields
                    if "T" in data:
                        point.field("temperature", float(data["T"]))
                        fields_added = True
                    if "H" in data:
                        point.field("humidity", float(data["H"]))
                        fields_added = True
                    if "CO2" in data:
                        point.field("co2", int(float(data["CO2"])))
                        fields_added = True
                    # Simulated people count
                    p_val = data.get("pCount", data.get("people"))
                    if p_val is not None:
                        point.field("people", int(float(p_val)))
                        fields_added = True
                else:
                    # If validation fails, we don't add any fields (just the tag and timestamp)
                    pass

            # Logic for Physical Sensors
            else:
                count = DataValidator.extract_person_count(data)
                # Always write the count if it exists, regardless of bounds
                if count is not None:
                    point.field("people", int(count))
                    fields_added = True
                    # Check bounds for validity flag
                    if DataValidator.is_within_bounds(sensor_id, count):
                        valid = True
                    else:
                        logger.debug(f"Out-of-bounds value for {sensor_id}: {count}")
                else:
                    # No count found – maybe still valid? But no fields, so we skip.
                    pass

            # Add validity tag if any fields were added
            if fields_added:
                point.tag("valid", "true" if valid else "false")
                self.write_api.write(bucket=Config.INFLUX_BUCKET, record=point)
                logger.info(f"Influx Write OK | Device: {sensor_id} | valid: {valid}")
            else:
                logger.debug(f"Skipped Influx write (No valid fields) | Device: {sensor_id}")

        except Exception as e:
            logger.error(f"Influx Write Failed: {e}")

    # -------------------------------------------------------------------------
    # LIFECYCLE MANAGEMENT
    # -------------------------------------------------------------------------
    def stop(self):
        """Gracefully shuts down clients."""
        logger.info("Stopping Collector Service...")
        self.running = False
        try:
            self.mqtt_client.disconnect()
            self.influx_client.close()
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

    def run(self):
        """Starts the main event loop."""
        try:
            logger.info(f"Attempting connection to MQTT Broker at {Config.MQTT_BROKER}:{Config.MQTT_PORT}...")
            self.mqtt_client.connect(Config.MQTT_BROKER, Config.MQTT_PORT, 60)
            self.mqtt_client.loop_forever()
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            logger.critical(f"Fatal Service Error: {e}")
            self.stop()
            sys.exit(1)

# -----------------------------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. Instantiate Service
    service = IoTCollector()

    # 2. Register System Signals (Docker Stop / Ctrl+C)
    def signal_handler(sig, frame):
        logger.info(f"Signal received ({sig}). Shutting down...")
        service.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 3. Start
    service.run()

