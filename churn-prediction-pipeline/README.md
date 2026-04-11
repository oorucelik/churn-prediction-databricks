# Churn Prediction Pipeline

> **dbt + Airflow with Fabric now, Databricks later?**

An end-to-end data engineering portfolio project demonstrating platform-portable data pipelines built with open-source tools.

## Architecture

```
Source Data (Synthetic/API) → dbt (Transform) → Airflow (Orchestrate)
                                    ↓
                         Microsoft Fabric Lakehouse
                                    ↓
                           Power BI Dashboard
```

## Why This Stack?

| Tool | Role | Why Not the Alternative? |
|------|------|------------------------|
| **dbt** | Transformation | Testable, version-controlled SQL vs. notebooks |
| **Airflow** | Orchestration | Open-source, vendor-agnostic vs. Fabric Data Factory |
| **Fabric** | Data Platform | Delta Lake + OneLake |
| **Power BI** | Serving | Native Fabric integration via Direct Lake |

**Platform portability:** The same dbt models run on DuckDB (local dev), Fabric, or Databricks — just change `profiles.yml`.

## Local Development

```bash
# 1. Generate synthetic data
cd scripts
python generate_synthetic_data.py

# 2. Install dbt packages
dbt deps

# 3. Load seed data
dbt seed

# 4. Run models
dbt run

# 5. Test everything
dbt test

# 6. Generate docs
dbt docs generate
dbt docs serve
```

## Project Structure

```
churn-prediction-pipeline/
├── dbt_project.yml
├── profiles.yml            # DuckDB (dev) → Fabric (prod)
├── packages.yml
├── models/
│   ├── staging/            # Clean, cast, dedup (1:1 with source)
│   │   ├── customers/
│   │   ├── subscriptions/
│   │   └── support/
│   ├── intermediate/       # Cross-source joins, aggregations
│   └── marts/
│       ├── core/           # dim_customer, dim_date
│       ├── churn/          # fct_subscription_event
│       ├── finance/        # fct_mrr (future)
│       └── ml/             # ml_churn_feature_store (future)
├── seeds/                  # Synthetic CSV data
├── snapshots/              # SCD Type 2 (future)
├── macros/
├── tests/
├── scripts/                # Data generators, utilities
└── dags/                   # Airflow DAGs (future)
```