#airbnb_databricks_dag.py

"""
Airbnb pipeline (Databricks + LLM sentiment) orchestrated by Airflow.

What it does
------------
1) Runs the Databricks notebook that cleans raw CSVs and writes curated Parquet:
   - dim_listing, dim_date, fact_reviews_stage under s3://.../curated/<table>/ds=YYYY-MM-DD/
2) Runs the Databricks notebook that adds LLM sentiment and writes:
   - fact_reviews under s3://.../curated/fact_reviews/ds=YYYY-MM-DD/

How to run with a specific day and sampling (optional)
------------------------------------------------------
UI  : Trigger DAG ➜ “Configure” ➜ JSON:
      {"run_ds":"2025-08-25", "max_rows": 2000, "sample_frac": 0.1, "max_tokens": 80, "seed": 42}
CLI : airflow dags trigger -c '{"run_ds":"2025-08-25"}' airbnb_databricks
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# --- Configuration pulled from Airflow Variables (set these once in the UI) ---
DATABRICKS_CONN_ID = Variable.get("databricks_conn_id", default_var="databricks_default")
CLUSTER_ID         = Variable.get("databricks_cluster_id", default_var="PUT_CLUSTER_ID_HERE")
CLEAN_NB_PATH      = Variable.get("clean_nb_path",     default_var="/Shared/airbnb/clean_transformed")
SENTIMENT_NB_PATH  = Variable.get("sentiment_nb_path", default_var="/Shared/airbnb/sentiment_analysis")

# Reasonable defaults; Jobs can still be retried manually from the UI
default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="airbnb_databricks",
    description="Curate Airbnb data in Databricks, then score review sentiment via OpenAI.",
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    schedule="@daily",          # daily logical date window; can still be triggered ad-hoc
    catchup=False,              # run only for the latest schedule, not the backlog
    default_args=default_args,
    tags=["airbnb", "databricks", "llm"],
) as dag:

    # Marker tasks keep the graph tidy and make failures easier to spot
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # Use run_ds passed at trigger-time if present; otherwise fall back to Airflow's {{ ds }}
    run_ds = "{{ dag_run.conf.get('run_ds') or ds }}"

    # 1) Databricks: clean & transform raw data ➜ writes curated/dim_* and fact_reviews_stage
    clean = DatabricksSubmitRunOperator(
        task_id="clean_transformed",
        databricks_conn_id=DATABRICKS_CONN_ID,
        # Option A: reuse an existing cluster; Option B: swap to new_cluster={...}
        existing_cluster_id=CLUSTER_ID,
        notebook_task={
            "notebook_path": CLEAN_NB_PATH,
            "base_parameters": {
                # The notebook reads this widget param and writes to ds=RUN_DS
                "RUN_DS": run_ds,
            },
        },
    )

    # 2) Databricks: LLM sentiment on staged comments ➜ writes curated/fact_reviews
    # Exposes simple sampling knobs so long runs can be throttled per trigger.
    sentiment = DatabricksSubmitRunOperator(
        task_id="sentiment_analysis",
        databricks_conn_id=DATABRICKS_CONN_ID,
        existing_cluster_id=CLUSTER_ID,
        notebook_task={
            "notebook_path": SENTIMENT_NB_PATH,
            "base_parameters": {
                "RUN_DS":      run_ds,  # same day as the clean step
                # Sampling controls (read by Databricks widgets). Strings are fine.
                "MAX_ROWS":    "{{ dag_run.conf.get('max_rows')    or 1000 }}",  # hard cap
                "SAMPLE_FRAC": "{{ dag_run.conf.get('sample_frac') or 0 }}",     # 0..1
                "MAX_TOKENS":  "{{ dag_run.conf.get('max_tokens')  or 0 }}",     # 0 means no filter
                "SEED":        "{{ dag_run.conf.get('seed')        or 42 }}",
            },
        },
    )

    # Simple linear dependency: clean ➜ sentiment
    start >> clean >> sentiment >> end
