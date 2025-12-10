# ===========================
# transform.py
# ===========================

import os
import pandas as pd

def transform_data(raw_path):
    # Ensure the path is relative to the project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    # Load data
    df = pd.read_csv(raw_path)

    # ----------------------------------------------------
    # 1️⃣ CLEANING TASKS
    # ----------------------------------------------------

    # Convert TotalCharges to numeric (spaces become NaN)
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

    # Fill missing numeric values using TotalCharges median
    df["TotalCharges"] = df["TotalCharges"].fillna(df["TotalCharges"].median())

    # ----------------------------------------------------
    # 2️⃣ FEATURE ENGINEERING
    # ----------------------------------------------------

    # (1) tenure_group
    def tenure_group_func(x):
        if x <= 12:
            return "New"
        elif x <= 36:
            return "Regular"
        elif x <= 60:
            return "Loyal"
        else:
            return "Champion"

    df["tenure_group"] = df["tenure"].apply(tenure_group_func)

    # (2) monthly_charge_segment
    def charge_segment(x):
        if x < 30:
            return "Low"
        elif x <= 70:
            return "Medium"
        else:
            return "High"

    df["monthly_charge_segment"] = df["MonthlyCharges"].apply(charge_segment)

    # (3) has_internet_service
    df["has_internet_service"] = df["InternetService"].map({
        "DSL": 1,
        "Fiber optic": 1,
        "No": 0
    })

    # (4) is_multi_line_user
    df["is_multi_line_user"] = df["MultipleLines"].apply(
        lambda x: 1 if x == "Yes" else 0
    )

    # (5) contract_type_code
    df["contract_type_code"] = df["Contract"].map({
        "Month-to-month": 0,
        "One year": 1,
        "Two year": 2
    })

    # ----------------------------------------------------
    # 3️⃣ DROP UNNECESSARY COLUMNS
    # ----------------------------------------------------
    df.drop(columns=["customerID", "gender"], inplace=True, errors="ignore")

    # ----------------------------------------------------
    # 4️⃣ SAVE TRANSFORMED DATA
    # ----------------------------------------------------
    staged_path = os.path.join(staged_dir, "telco_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path


if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()   # expects you modified extract.py for Telco dataset
    transform_data(raw_path)
