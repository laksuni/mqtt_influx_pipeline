```markdown
# IoT Sensor Pipeline

A complete, containerized IoT pipeline for collecting, cleaning, forecasting, and visualizing real-time occupancy data from multiple sensors.

## Overview

This project demonstrates an end-to-end data pipeline that:

- Subscribes to an MQTT broker to receive raw occupancy readings from three sensors (`aiot`, `robo`, `simulated`).
- Stores raw data in **InfluxDB** (`sensor_data_raw`) and optionally writes CSV backups (configurable).
- Runs an hourly **cleaning job** that fetches raw data, applies robust signal processing (capping, Hampel filter, seasonal gap filling), and writes cleaned data to `sensor_data_clean`.
- Runs an hourly **forecasting job** that fetches cleaned data, trains a **Facebook Prophet** model with logistic growth, and writes 7‑day forecasts to `people_count_forecast`.
- Provides additional analysis tools: **STL decomposition** (`decompose_occupancy.py`) and **backtesting** (`test_forecast_accuracy.py`).
- Orchestrates everything via a **scheduler** and offers ready‑to‑use **Grafana** dashboards for visualisation.

All components are containerised with Docker.

## Architecture

```
[MQTT Broker] -> [Collector (main.py)] -> InfluxDB (raw)
                                             |
                                     [Cleaner (cleaner.py)]
                                             |
                                    InfluxDB (clean data)
                                             |
                                  [Forecaster (forecaster.py)]
                                             |
                                    InfluxDB (forecasts)
                                             |
                                        [Grafana]
```

- **Collector** – connects to MQTT, parses JSON, writes to InfluxDB and optionally to CSV.
- **Cleaner** – reads raw data, removes outliers, fills gaps intelligently, writes cleaned data.
- **Forecaster** – fetches cleaned data, trains Prophet, writes forecasts.
- **Scheduler** – runs cleaner and forecaster every hour.
- **InfluxDB** – time‑series database for raw, cleaned, and forecast data.
- **Grafana** – visualisation layer.

## Technologies Used

- **Python 3.10+** – core logic, data processing, forecasting.
- **InfluxDB 2.x** – time‑series storage.
- **Grafana** – interactive dashboards.
- **Facebook Prophet** – forecasting with seasonality and logistic growth.
- **Docker & Docker Compose** – containerisation and orchestration.
- **MQTT (paho‑mqtt)** – real‑time data ingestion.
- **pandas / numpy / scipy / statsmodels** – data manipulation, filtering, and decomposition.

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your server.
- An MQTT broker (e.g., a public instance at [shiftr.io](https://shiftr.io) or your own).
- (Optional) Grafana – already included if you run the provided `docker-compose.yml`.

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/laksuni/mqtt_influx_pipeline.git
   cd mqtt_influx_pipeline
   ```

2. **Set up environment variables**
   Copy the example environment file and edit it with your settings:
   ```bash
   cp .env.example .env
   nano .env
   ```
   Fill in your InfluxDB organisation, bucket, MQTT broker details, etc.

3. **Create secret files** (these will **never** be committed)
   ```bash
   mkdir -p secrets
   echo "your-influxdb-token" > secrets/influx_token.txt
   echo "your-mqtt-username" > secrets/mqtt_username.txt
   echo "your-mqtt-password" > secrets/mqtt_password.txt
   ```

4. **Build and start the containers**
   ```bash
   docker-compose up -d --build
   ```

5. **Verify the logs**
   ```bash
   docker-compose logs -f
   ```
   After ~10 seconds you should see the scheduler start its first cleaning/forecasting cycle.

6. **Access Grafana** (if you included it in your `docker-compose.yml`)
   - Open `http://your-server:3000` (default credentials: `admin` / `admin`).
   - Add InfluxDB as a data source (URL: `http://influxdb:8086`, organisation and bucket from your `.env`, token from `secrets/influx_token.txt`).
   - Import the provided dashboards (located in `grafana/`) or create your own.

## Running Scripts with Docker (No Host Installation Required)

All Python scripts can be executed directly inside the `iot_analytics` container, so you **do not need to install any libraries on your host machine**. The container already contains all dependencies (`pandas`, `prophet`, `statsmodels`, etc.).

### Cleaner Module
The cleaner can be run manually to process a specific time window:
```bash
docker exec -it iot_analytics python -m src.cleaner --days 35
```
This fetches raw data from the last 35 days, cleans it, and writes to `sensor_data_clean`.

### Forecaster Module
Run the production forecast (hourly job is automatic, but you can trigger it manually):
```bash
docker exec -it iot_analytics python -m src.forecaster
```
For backtesting (historical accuracy evaluation):
```bash
docker exec -it iot_analytics python -m src.forecaster --backtest
```

### Utility Scripts
All utility scripts in the `scripts/` folder are also accessible:

- **STL decomposition**:
  ```bash
  docker exec -it iot_analytics python /app/scripts/decompose_occupancy.py --sensor robo --days 30
  ```
- **Forecast accuracy test**:
  ```bash
  docker exec -it iot_analytics python /app/scripts/test_forecast_accuracy.py --cutoff 2026-02-20 --days 7
  ```
- **Delete data from InfluxDB**:
  ```bash
  docker exec -it iot_analytics python /app/scripts/delete_measurement.py --bucket your-bucket --measurement people_count_forecast --tag-key cutoff
  ```

> **Note:** The `iot_analytics` container mounts the `./scripts` folder as a volume, so any changes you make to the scripts are immediately available without rebuilding.

## Configuration

### Environment Variables (`.env`)
| Variable        | Description                | Example                 |
|-----------------|----------------------------|-------------------------|
| `INFLUX_URL`    | InfluxDB URL               | `http://localhost:8086` |
| `INFLUX_ORG`    | InfluxDB organisation      | `my-org`                |
| `INFLUX_BUCKET` | InfluxDB bucket            | `sensor_data`           |
| `MQTT_BROKER`   | MQTT broker address        | `broker.shiftr.io`      |
| `MQTT_PORT`     | MQTT port                  | `1883`                  |
| `MQTT_TOPIC`    | MQTT topic to subscribe to | `automation/#`          |

### Sensor Configuration
All sensor‑specific settings are centralized in `src/config.py`:

- **Sensor IDs**: `SENSOR_IDS = ["aiot", "robo", "simulated"]` – add or remove sensors here.
- **Capacity limits**: `SENSOR_CAPACITY` dictionary defines maximum realistic occupancy per sensor.

### Optional CSV Backup
In `src/config.py`, you can disable CSV logging by setting:
```python
WRITE_CSV_BACKUP = False   # default is True
```

### Cleaning Parameters (in `src/cleaner.py`)
- Hampel filter window and sigma (`HAMPEL_WINDOW`, `HAMPEL_N_SIGMA`)
- Gap‑filling thresholds (`SHORT_GAP_HOURS`, `LONG_GAP_HOURS`)
- Rolling window for the simulated sensor (`SIMULATED_ROLLING_WINDOW`)

### Forecasting Parameters (in `src/forecaster.py`)
- `TRAINING_DAYS` – how much history to fetch (default 60)
- `FORECAST_HORIZON` – forecast length in hours (default 168)
- `MIN_TRAINING_HOURS` – minimum data points required to run a forecast (default 168, i.e., 7 days)

## Data Cleaning Pipeline

The cleaner applies a multi‑stage process:

1. **Capping** – any value above the sensor’s capacity (from `config.py`) is set to `NaN`.
2. **Hampel filter** (optional) – removes isolated spikes by comparing each point with the median of a sliding window.
3. **Intelligent gap filling**  
   - Short gaps (<6 hours) → linear interpolation.  
   - Medium gaps (6–24 hours) → seasonal fill using the median of the same hour from the rest of the dataset.  
   - Long gaps (>24 hours) → left as `NaN` (will be handled by back/forward fill).
4. **Back/forward fill** – any remaining `NaN`s at the beginning or end are filled with the nearest valid value.
5. **Rounding** – final values are rounded to integers (people counts are whole numbers).

For the simulated sensor, a light 3‑point moving average is applied after cleaning.

## Forecasting

The forecaster (`src/forecaster.py`) runs hourly and:

- Fetches the last `TRAINING_DAYS` of hourly‑aggregated cleaned data from InfluxDB.
- Trains a Prophet model with **logistic growth**, bounded by the sensor capacity (floor = 0, cap = sensor capacity).
- Generates a `FORECAST_HORIZON`‑hour forecast with confidence intervals (`yhat_lower`, `yhat_upper`).
- Clips any negative forecast values to zero before writing.
- Writes the forecast to the `people_count_forecast` measurement in InfluxDB.

### Backtesting
The script `scripts/test_forecast_accuracy.py` evaluates forecast accuracy for a given cutoff date. Example:

```bash
docker exec -it iot_analytics python /app/scripts/test_forecast_accuracy.py --cutoff 2026-02-20 --days 7
```

This will compute MAE, RMSE, and MAPE for each sensor and optionally store detailed forecast points (with `--store-details`).

## Time Series Decomposition

The script `scripts/decompose_occupancy.py` performs **STL decomposition** (Seasonal‑Trend decomposition using LOESS) on cleaned occupancy data and writes the components back to InfluxDB as a new measurement `decomposition`. This allows you to visualize trend, seasonal, and residual components in Grafana.

```bash
# Decompose last 30 days for robo sensor
docker exec -it iot_analytics python /app/scripts/decompose_occupancy.py --sensor robo --days 30

# Decompose a fixed date range
docker exec -it iot_analytics python /app/scripts/decompose_occupancy.py --sensor aiot --start 2026-02-01 --end 2026-02-28
```

The decomposition requires `statsmodels` (included in `requirements.txt`). The results are stored with fields `trend`, `seasonal`, and `residual`.

## Deleting Data

Use `scripts/delete_measurement.py` to remove data from InfluxDB with flexible filters. Examples:

```bash
# Delete all points with a specific tag (e.g., backfilled forecasts)
docker exec -it iot_analytics python /app/scripts/delete_measurement.py --bucket your-bucket --measurement people_count_forecast --tag-key cutoff

# Delete a measurement entirely
docker exec -it iot_analytics python /app/scripts/delete_measurement.py --bucket your-bucket --measurement people_count_forecast

# Delete within a time range
docker exec -it iot_analytics python /app/scripts/delete_measurement.py --bucket your-bucket --measurement sensor_data_raw --start 2026-01-01 --end 2026-02-01
```

The script will ask for confirmation before executing.

## Adapting for Your Own Sensors

This project is designed to be easily customised:

- **New sensor IDs** – add them to `SENSOR_IDS` in `src/config.py`.
- **Different payload structure** – modify the extraction logic in `main.py` (e.g., look for different JSON keys).
- **Other measurement types** – the cleaner already handles `temperature`, `humidity`, `co2` for the simulated sensor; extend as needed.
- **Timezone** – replace `HELSINKI_TZ` in `cleaner.py` and `forecaster.py` with your local timezone.
- **Forecast horizon** – change `FORECAST_HORIZON` in `forecaster.py` (default 168 hours).

## Project Structure


```
.
├── .env.example                     # Environment variables template
├── .gitignore                       # Files to ignore in git
├── docker-compose.yml               # Docker services
├── Dockerfile                       # Python container build
├── entrypoint.sh                    # Container entrypoint
├── requirements.txt                 # Python dependencies
├── scripts/                         # Utility scripts (run from project root)
│   ├── decompose_occupancy.py       # STL decomposition
│   ├── delete_measurement.py
│   └── test_forecast_accuracy.py
├── secrets/                         # (ignored except README) – credentials
│   └── README.md
└── src/                             # Core source code
    ├── cleaner.py
    ├── config.py
    ├── forecaster.py
    ├── __init__.py
    ├── main.py
    ├── scheduler.py
    └── utils.py
```

## Context

Developed as part of an IoT data analysis course.

```
