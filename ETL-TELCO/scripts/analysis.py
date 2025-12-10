# ===========================
# etl_analysis.py
# ===========================
# Purpose:
# Read Telco dataset from Supabase → compute ETL analysis metrics →
# save output to data/processed/analysis_summary.csv
# (Optional) generate visualizations
# ===========================

import os
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()


# ---------------------------------------------
# Supabase Connection
# ---------------------------------------------
def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)


# ---------------------------------------------
# Fetch Data From Supabase
# ---------------------------------------------
def fetch_telco_data():
    supabase = get_supabase()

    try:
        response = supabase.table("telco_data").select("*").execute()
        df = pd.DataFrame(response.data)
        print(f"📥 Loaded {len(df)} rows from Supabase")
        return df
    except Exception as e:
        print(f"❌ Error fetching data from Supabase: {e}")
        return None


# ---------------------------------------------
# Analysis Metrics
# ---------------------------------------------
def compute_metrics(df: pd.DataFrame):

    # 🔥 FIX: Normalize column names
    df.columns = df.columns.str.lower()

# 🔥 Normalize churn to string
    if df['churn'].apply(lambda x: isinstance(x, dict)).any():
        df['churn'] = df['churn'].apply(lambda d: d.get('value') if isinstance(d, dict) else d)

    if df['churn'].apply(lambda x: isinstance(x, list)).any():
        df['churn'] = df['churn'].apply(lambda lst: lst[0] if isinstance(lst, list) and len(lst) > 0 else None)
    df['churn'] = df['churn'].astype(str)

    summary = {}

    # 1. Churn percentage
    churn_rate = (df['churn'].str.lower() == "yes").mean() * 100
    summary["churn_percentage"] = round(churn_rate, 2)

    # 2. Avg monthly charges by contract
    summary["avg_monthlycharges_by_contract"] = (
        df.groupby("contract")["monthlycharges"].mean().round(2).to_dict()
    )

    # 3. Tenure group distribution
    summary["tenure_group_counts"] = df["tenure_group"].value_counts().to_dict()

    # 4. Internet service distribution
    summary["internet_service_distribution"] = (
        df["internetservice"].value_counts().to_dict()
    )
    df['churn_flag'] = (df['churn'].str.lower() == 'yes').astype(int)

    pivot = (
        df.pivot_table(
            values='churn_flag',
            index='tenure_group',
            columns='churn',
            aggfunc='count',
            fill_value=0
        ).reset_index()
    )

    return summary, pivot


# ---------------------------------------------
# Save Output CSV
# ---------------------------------------------
def save_summary(summary: dict, pivot_df: pd.DataFrame):

    # Prepare output directory
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed_dir = os.path.join(base_dir, "data", "processed")
    os.makedirs(processed_dir, exist_ok=True)

    # Save summary as a single-row CSV
    summary_path = os.path.join(processed_dir, "analysis_summary.csv")
    
    # Convert nested dict → flattened printable structure
    flat_summary = {
        "churn_percentage": summary["churn_percentage"],
        "avg_monthlycharges_by_contract": str(summary["avg_monthlycharges_by_contract"]),
        "tenure_group_counts": str(summary["tenure_group_counts"]),
        "internet_service_distribution": str(summary["internet_service_distribution"]),
    }

    pd.DataFrame([flat_summary]).to_csv(summary_path, index=False)

    # Save pivot table
    pivot_path = os.path.join(processed_dir, "churn_vs_tenure_group.csv")
    pivot_df.to_csv(pivot_path, index=False)

    print(f"📊 Summary saved at: {summary_path}")
    print(f"📊 Pivot saved at:    {pivot_path}")


# ---------------------------------------------
# Optional Visualizations
# ---------------------------------------------
def generate_plots(df):

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    viz_dir = os.path.join(base_dir, "data", "processed", "visualizations")
    os.makedirs(viz_dir, exist_ok=True)

    # 1 — Churn rate by charge segment
    plt.figure(figsize=(7, 5))
    df.groupby("monthly_charge_segment")["churn"].apply(lambda x: (x=="Yes").mean()).plot(kind="bar")
    plt.title("Churn Rate by Monthly Charge Segment")
    plt.xlabel("Segment")
    plt.ylabel("Churn Rate")
    plt.tight_layout()
    plt.savefig(os.path.join(viz_dir, "churn_by_charge_segment.png"))
    plt.close()

    # 2 — Histogram of TotalCharges
    plt.figure(figsize=(7, 5))
    df["totalcharges"].dropna().plot(kind="hist", bins=30)
    plt.title("Distribution of Total Charges")
    plt.xlabel("TotalCharges")
    plt.tight_layout()
    plt.savefig(os.path.join(viz_dir, "hist_totalcharges.png"))
    plt.close()

    # 3 — Bar plot of contract types
    plt.figure(figsize=(7, 5))
    df["contract"].value_counts().plot(kind="bar")
    plt.title("Contract Type Distribution")
    plt.xlabel("Contract Type")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(os.path.join(viz_dir, "contract_distribution.png"))
    plt.close()

    print("📈 Visualizations saved.")


# ---------------------------------------------
# MAIN
# ---------------------------------------------
if __name__ == "__main__":

    df = fetch_telco_data()
    if df is None:
        exit()

    summary, pivot = compute_metrics(df)

    save_summary(summary, pivot)

    # Optional — comment if not needed
    generate_plots(df)

    print("\n🎯 ETL Analysis Complete.\n")
