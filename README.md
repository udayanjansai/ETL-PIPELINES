# ETL-PIPELINES
📘 ETL-TELCO Pipeline  A complete, production-ready ETL pipeline built in Python for processing the WA_Fn-UseC_-Telco-Customer-Churn dataset. This pipeline performs Extract → Transform → Load → Validate → Analyze, then pushes cleaned data into Supabase for downstream analytics.
             ┌──────────┐
             │  Extract │  → Copy raw Telco dataset
             └────┬─────┘
                  ↓
             ┌──────────┐
             │ Transform│  → Clean, feature engineer, store transformed CSV
             └────┬─────┘
                  ↓
             ┌──────────┐
             │   Load   │  → Upload to Supabase DB
             └────┬─────┘
                  ↓
             ┌──────────┐
             │ Validate │  → Data quality checks
             └────┬─────┘
                  ↓
             ┌──────────┐
             │ Analysis │ → KPIs, pivot tables, charts
             └──────────┘
