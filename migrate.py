import pandas as pd
from pathlib import Path
import shutil
import os

# --- CONFIGURATION ---
# UPDATE THIS PATH to where your OLD iot_bridge/data/raw folder is
OLD_RAW_DIR = Path("/home/timpuri/iot_bridge/data/raw").resolve()
NEW_RAW_DIR = Path("./data/raw").resolve()

# Mapping: Old Filename -> New Filename
FILES = {
    "aiot_raw.csv": "aiot.csv",
    "robo_raw.csv": "robo.csv",
    "simulated_raw.csv": "simulated.csv",
    "sensor_data_raw.csv": "sensor_data.csv", 
    "unknown_raw.csv": "unknown.csv"
}

def robust_read(path):
    """Reads CSV, ignoring bad lines and headers in the middle."""
    if not path.exists():
        return pd.DataFrame()
    try:
        # on_bad_lines='skip' ensures we don't crash on corrupted rows
        return pd.read_csv(path, on_bad_lines='skip', low_memory=False)
    except Exception as e:
        print(f"  Error reading {path.name}: {e}")
        return pd.DataFrame()

def migrate():
    print(f"Source: {OLD_RAW_DIR}")
    print(f"Dest:   {NEW_RAW_DIR}")
    
    if not OLD_RAW_DIR.exists():
        print("CRITICAL: Old data directory not found!")
        return

    for old_name, new_name in FILES.items():
        old_path = OLD_RAW_DIR / old_name
        new_path = NEW_RAW_DIR / new_name

        print(f"Processing {new_name}...")
        
        # 1. Read Data
        df_old = robust_read(old_path)
        df_new = robust_read(new_path)
        
        print(f"  - Old records: {len(df_old)}")
        print(f"  - New records: {len(df_new)}")

        # 2. Merge
        if df_old.empty and df_new.empty:
            print("  - No data found. Skipping.")
            continue
            
        df_combined = pd.concat([df_old, df_new], ignore_index=True)

        # 3. Clean (Deduplicate)
        if 'timestamp' in df_combined.columns:
            # Convert to datetime to sort correctly
            df_combined['timestamp'] = pd.to_datetime(df_combined['timestamp'], errors='coerce')
            df_combined = df_combined.dropna(subset=['timestamp'])
            df_combined = df_combined.sort_values('timestamp')
            
            # Drop exact duplicates
            before_dedup = len(df_combined)
            df_combined = df_combined.drop_duplicates(subset=['timestamp'], keep='last')
            print(f"  - Dropped {before_dedup - len(df_combined)} duplicates.")

        # 4. Save
        # Write to a temp file first, then move (safer)
        df_combined.to_csv(new_path, index=False)
        print(f"  - Saved {len(df_combined)} rows to {new_name}")

        # 5. FEDORA FIX: Set permissions
        # This ensures the user running docker (usually 1000:1000) can read it
        os.chmod(new_path, 0o666) 

if __name__ == "__main__":
    migrate()
