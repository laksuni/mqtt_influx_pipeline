"""
Analytics Scheduler Service
---------------------------
Entry point for the Analytics container.
Responsibilities:
1. Orchestrate the ETL (Cleaning) and ML (Forecasting) pipeline.
2. Execute jobs on a fixed schedule (e.g., hourly).
3. Handle graceful shutdowns and error reporting.

This service runs independently from the real-time Collector.
"""

import time
import schedule
import logging
import signal
import sys
from datetime import datetime

# Local Module Imports
from .cleaner import run_cleaning_cycle
from .forecaster import run_forecasting_cycle

# -----------------------------------------------------------------------------
# LOGGING SETUP
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Scheduler")

class AnalyticsScheduler:
    def __init__(self):
        self.running = True

    def run_batch_job(self):
        """
        The core workflow:
        1. Clean Data (ETL)
        2. Forecast Data (ML)
        
        If cleaning fails, forecasting is skipped to maintain data integrity.
        """
        start_time = time.time()
        logger.info("=== Starting Batch Analytics Job ===")

        # Step 1: Cleaning
        try:
            run_cleaning_cycle()
        except Exception as e:
            logger.error(f"ETL Cleaning failed. Aborting batch job. Error: {e}")
            return

        # Step 2: Forecasting
        try:
            run_forecasting_cycle()
        except Exception as e:
            logger.error(f"Forecasting failed. Error: {e}")

        duration = time.time() - start_time
        logger.info(f"=== Batch Job Finished in {duration:.2f} seconds ===")

    def start(self):
        """Configures schedule and starts the main loop."""
        
        # Schedule the job to run every 1 hour
        # You can adjust this to .minutes for testing
        schedule.every(1).hours.do(self.run_batch_job)
        
        logger.info("Scheduler initialized. Job frequency: Every 1 Hour.")

        # --- WARMUP & IMMEDIATE RUN ---
        # Wait 10 seconds for the Collector to initialize and connect to DB/MQTT
        logger.info("Waiting 10s for system warmup...")
        time.sleep(10)
        
        # Run once immediately on startup to ensure data is fresh
        logger.info("Executing initial startup job...")
        self.run_batch_job()

        # --- MAIN LOOP ---
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1) # Sleep to prevent high CPU usage
            except KeyboardInterrupt:
                self.stop()
            except Exception as e:
                logger.critical(f"Scheduler loop crashed: {e}")
                # Don't exit, try to recover on next tick
                time.sleep(5)

    def stop(self, *args):
        """Gracefully stops the scheduler."""
        logger.info("Stopping Scheduler Service...")
        self.running = False
        sys.exit(0)

# -----------------------------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    service = AnalyticsScheduler()

    # Handle Docker Signals (SIGTERM) and Ctrl+C (SIGINT)
    signal.signal(signal.SIGTERM, service.stop)
    signal.signal(signal.SIGINT, service.stop)

    service.start()
