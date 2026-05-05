"""
churn_pipeline_dag.py
─────────────────────
Daily churn prediction pipeline:

  [1] DockerOperator       → fetch_tmdb container → writes raw_content_catalog to Databricks
  [2] DbtTaskGroup (cosmos) → dbt build (seeds + staging + marts + ml feature store)
  [3] DatabricksRunNowOp   → triggers XGBoost training notebook (registered as a Databricks Job)

Schedule: daily at 02:00 UTC (after TMDB data is stable)

Connections required (set in Airflow UI → Admin → Connections):
  - databricks_default  (type: Databricks, host: dbc-d93ecbbd-cf6c.cloud.databricks.com, token: <token>)

Variables required (set in Airflow UI → Admin → Variables):
  - databricks_ml_job_id   → Job ID of the 'train_churn_model' notebook job in Databricks
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.models import Variable
from docker.types import Mount

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import DatabricksTokenProfileMapping
from cosmos.constants import ExecutionMode

# ── Constants ─────────────────────────────────────────────────────────────────

DBT_PROJECT_PATH   = "/opt/airflow/dbt"
DBT_PROFILES_PATH  = "/opt/airflow/dbt"
FETCH_TMDB_IMAGE   = "fetch_tmdb:latest"   # built locally in Phase 1

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Profile Config (cosmos → dbt-databricks) ──────────────────────────────────

profile_config = ProfileConfig(
    profile_name="tmdb_churn",
    target_name="dev",                      # switch to "prod" for production runs
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id="databricks_default",
        profile_args={
            "catalog":    "prod",
            "schema":     "dbt_{{ var.value.get('dbt_user', 'airflow') }}",
            "http_path":  os.environ.get("DBT_DATABRICKS_HTTP_PATH", ""),
        },
    ),
)

# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="churn_pipeline",
    description="Daily: TMDB ingest → dbt build → XGBoost training",
    schedule="0 2 * * *",              # 02:00 UTC daily
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["churn", "tmdb", "dbt", "databricks"],
    doc_md=__doc__,
) as dag:

    # ── [0] Pipeline start marker ─────────────────────────────────────────────
    start = EmptyOperator(task_id="pipeline_start")

    # ── [1] Fetch TMDB content → write to Databricks ─────────────────────────
    fetch_tmdb = DockerOperator(
        task_id="fetch_tmdb_content",
        image=FETCH_TMDB_IMAGE,
        # Pass secrets from Airflow env (loaded via docker-compose .env.docker)
        environment={
            "TMDB_ACCESS_TOKEN":    os.environ.get("TMDB_ACCESS_TOKEN", ""),
            "DATABRICKS_HOST":      os.environ.get("DATABRICKS_HOST", ""),
            "DATABRICKS_HTTP_PATH": os.environ.get("DATABRICKS_HTTP_PATH", ""),
            "DATABRICKS_TOKEN":     os.environ.get("DATABRICKS_TOKEN", ""),
            "TMDB_MOVIE_PAGES":     "25",
            "TMDB_TV_PAGES":        "10",
            "N_USERS":              "1000",
            "N_TICKETS":            "5000",
        },
        # Mount seeds dir so CSVs are available to dbt seeds
        mounts=[
            Mount(
                source="/opt/airflow/dbt/seeds",   # inside Airflow container (already mounted)
                target="/app/seeds",               # inside fetch_tmdb container
                type="bind",
            )
        ],
        mount_tmp_dir=False,
        auto_remove="success",         # clean up container after success
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        # If the container fails, retry up to DEFAULT_ARGS["retries"] times
    )

    # ── [2] dbt build via astronomer-cosmos ───────────────────────────────────
    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,    # runs dbt inside Airflow container
        ),
        render_config=RenderConfig(
            select=["path:models"],                # build all models
            # To run only the feature store + dependencies:
            # select=["models/marts/ml/ml_churn_feature_store+"]
        ),
        operator_args={
            "install_deps": True,                 # runs `dbt deps` before build
        },
    )

    # ── [3] Trigger Databricks ML training job ────────────────────────────────
    train_model = DatabricksRunNowOperator(
        task_id="train_churn_model",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.databricks_ml_job_id }}",
        # Optional: pass notebook params to override defaults
        notebook_params={
            "catalog": "prod",
            "schema":  "dbo_marts",
        },
        wait_for_termination=True,     # block until notebook finishes
        polling_period_seconds=30,
    )

    # ── [4] Pipeline end marker ───────────────────────────────────────────────
    end = EmptyOperator(task_id="pipeline_end")

    # ── Dependencies ──────────────────────────────────────────────────────────
    start >> fetch_tmdb >> dbt_build >> train_model >> end
