"""
Central Configuration Module
----------------------------
Loads environment variables and defines file paths for the entire application.
This ensures all modules (main, cleaner, forecaster) look at the exact same
folders and database credentials.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Robustly find the project root (up 2 levels from src/config.py)
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env from project root
load_dotenv(BASE_DIR / ".env")

# Helper function to read secrets from Docker's /run/secrets path or environment variables
def get_secret(secret_name):
    try:
        # First, try to read from Docker Secrets mounted path
        with open(f'/run/secrets/{secret_name}', 'r') as secret_file:
            return secret_file.read().strip()
    except IOError:
        # If not found in secrets, fall back to environment variable (e.g., for local dev)
        return os.getenv(secret_name.upper())

class Config:
    # -------------------------------------------------------------------------
    # DIRECTORY STRUCTURE
    # -------------------------------------------------------------------------
    DATA_DIR = BASE_DIR / "data"
    RAW_DIR = DATA_DIR / "raw"
    PROCESSED_DIR = DATA_DIR / "processed"

    # -------------------------------------------------------------------------
    # INFLUXDB CREDENTIALS
    # -------------------------------------------------------------------------
    INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
    INFLUX_TOKEN = get_secret("influx_token") 
    INFLUX_ORG = os.getenv("INFLUX_ORG")
    INFLUX_BUCKET = os.getenv("INFLUX_BUCKET") 

    # -------------------------------------------------------------------------
    # MQTT CONFIGURATION
    # -------------------------------------------------------------------------
    MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
    MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
    MQTT_TOPIC = os.getenv("MQTT_TOPIC", "automaatio/#")
    MQTT_USERNAME = get_secret("mqtt_username")
    MQTT_PASSWORD = get_secret("mqtt_password")
    
    # -------------------------------------------------------------------------
    # SENSOR CAPACITY (for cleaning unrealistic values)
    # Adjust these based on actual room sizes and sensor types
    # -------------------------------------------------------------------------
    SENSOR_CAPACITY = {
        "aiot": 60,        # adjust based on actual room size
        "robo": 60,
        "simulated": 150   # simulated may have arbitrary values
}
    # Environmental sensor limits (global, but could become sensor‑specific later)
    TEMP_RANGE = (-30.0, 60.0)      # °C
    HUM_RANGE = (0.0, 100.0)        # %
    CO2_RANGE = (350.0, 5000.0)     # ppm

    @classmethod
    def ensure_dirs(cls):
        """Creates necessary data directories if they don't exist."""
        cls.RAW_DIR.mkdir(parents=True, exist_ok=True)
        cls.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Run directory check immediately on import
Config.ensure_dirs()
