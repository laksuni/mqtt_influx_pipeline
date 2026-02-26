"""
Shared Utilities Module
-----------------------
Contains data validation logic, parsing helpers, and domain-specific constants 
for the IoT sensor network.

Usage:
    from .utils import DataValidator
    count = DataValidator.extract_person_count(payload)
"""

import json
import re
import logging
from typing import Optional, Dict, Union, Any

# Import Config to access all limits
from .config import Config

# -----------------------------------------------------------------------------
# CONFIGURATION & CONSTANTS
# -----------------------------------------------------------------------------

logger = logging.getLogger("Utils")

# JSON Keys often used for people counting
COUNT_KEYS = [
    "person count", "person_count", 
    "people_total", "pCount", 
    "people", "count"
]

# Regex patterns for extracting counts from raw non‑JSON strings
_RAW_PATTERNS = [
    re.compile(r'"person count"\s*:\s*([0-9]+)', re.IGNORECASE),
    re.compile(r'"person_count"\s*:\s*([0-9]+)', re.IGNORECASE),
    re.compile(r'"people_total"\s*:\s*([0-9]+)', re.IGNORECASE),
    re.compile(r'"pCount"\s*:\s*([0-9]+)', re.IGNORECASE),
    re.compile(r'"people"\s*:\s*([0-9]+)', re.IGNORECASE),
]

# Default maximum people count if sensor not found in Config
DEFAULT_MAX_PEOPLE = 50

# -----------------------------------------------------------------------------
# DATA VALIDATOR CLASS
# -----------------------------------------------------------------------------
class DataValidator:
    """
    Static utility class for validating and extracting data from sensor payloads.
    """

    @staticmethod
    def extract_person_count(payload: Union[str, Dict[str, Any]]) -> Optional[int]:
        """
        Attempts to extract a people count integer from a Dict or a JSON string.
        
        Args:
            payload: The raw MQTT payload (string or parsed dict).
            
        Returns:
            int: The extracted count if found and valid.
            None: If no count could be extracted.
        """
        # 1. Handle pre-parsed Dictionary
        if isinstance(payload, dict):
            return DataValidator._extract_from_dict(payload)

        # 2. Handle Raw String
        if not isinstance(payload, str):
            return None
        
        s_payload = payload.strip()

        # A. Try parsing string as JSON first
        try:
            data = json.loads(s_payload)
            if isinstance(data, dict):
                return DataValidator._extract_from_dict(data)
        except (json.JSONDecodeError, ValueError):
            pass  # Fallback to Regex

        # B. Fallback: Try Regex on raw string (useful for malformed JSON)
        for pattern in _RAW_PATTERNS:
            match = pattern.search(s_payload)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue
                    
        return None

    @staticmethod
    def _extract_from_dict(data: Dict[str, Any]) -> Optional[int]:
        """Helper to iterate through known keys in a dictionary."""
        for key in COUNT_KEYS:
            if key in data:
                try:
                    val = data[key]
                    # Handle cases where value might be a float string "12.0"
                    return int(float(val))
                except (ValueError, TypeError):
                    continue
        return None

    @staticmethod
    def is_within_bounds(sensor_id: str, count: Optional[int]) -> bool:
        """
        Checks if the extracted people count is physically realistic,
        using the sensor‑specific capacity from Config.SENSOR_CAPACITY.
        
        Args:
            sensor_id: The ID of the device.
            count: The extracted integer.
            
        Returns:
            bool: True if valid, False if outlier/None.
        """
        if count is None:
            return False
        
        max_people = Config.SENSOR_CAPACITY.get(sensor_id, DEFAULT_MAX_PEOPLE)
        return 0 <= count <= max_people

    @staticmethod
    def validate_simulated_payload(data: Any) -> bool:
        """
        Validates the multi-metric payload from the simulation environment.
        Checks Temperature (T), Humidity (H), and CO2 against physical limits
        defined in Config.
        """
        if not isinstance(data, dict):
            return False

        try:
            # 1. Validate Temperature
            if "T" in data:
                t = float(data["T"])
                if not (Config.TEMP_RANGE[0] <= t <= Config.TEMP_RANGE[1]):
                    return False

            # 2. Validate Humidity
            if "H" in data:
                h = float(data["H"])
                if not (Config.HUM_RANGE[0] <= h <= Config.HUM_RANGE[1]):
                    return False

            # 3. Validate CO2
            if "CO2" in data:
                co2 = float(data["CO2"])
                if not (Config.CO2_RANGE[0] <= co2 <= Config.CO2_RANGE[1]):
                    return False
            
            # 4. Validate People Count (if present) using sensor‑specific capacity
            sensor_id = data.get("id", "simulated")
            for k in ["pCount", "people", "person_count"]:
                if k in data:
                    val = int(float(data[k]))
                    max_people = Config.SENSOR_CAPACITY.get(sensor_id, DEFAULT_MAX_PEOPLE)
                    if not (0 <= val <= max_people):
                        return False
                    
        except (ValueError, TypeError, KeyError):
            # If data is malformed (e.g. "T": "error"), it's invalid
            return False
            
        return True
