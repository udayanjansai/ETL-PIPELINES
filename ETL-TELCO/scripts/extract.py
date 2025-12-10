import os
import shutil
import pandas as pd

def extract_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    # Path where user manually places CSV file
    source_path = os.path.join(base_dir, "C:\\Users\\udaya\\Downloads\\archive\\WA_Fn-UseC_-Telco-Customer-Churn.csv")

    # Destination path inside raw/
    dest_path = os.path.join(raw_dir, "telco_raw.csv")

    # Copy user file into raw/
    shutil.copy(source_path, dest_path)

    print(f"✅ Extracted data copied to: {dest_path}")
    return dest_path


if __name__ == "__main__":
    extract_data()