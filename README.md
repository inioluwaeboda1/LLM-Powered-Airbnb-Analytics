End-to-end ELT that cleans raw Airbnb data in Databricks, enriches review text with LLM sentiment, orchestrates via Airflow (Docker), and loads analytics-ready tables to Amazon Redshift. Built to demonstrate practical data engineering: orchestration, Lakehouse transforms, warehouse modeling, and safe/cost-aware LLM use.

🧭 Architecture
flowchart LR
  S3Raw["S3 /raw\n(listings.csv, reviews.csv)"]
  DBXClean["Databricks Notebook\nclean_transformed.py\n→ Parquet partitions"]
  S3Cur["S3 /curated\n(dim_listing, dim_date,\n  fact_reviews_stage)"]
  LLM["Databricks Notebook\nsentiment_analysis.py\n(OpenAI, batched)"]
  S3Fact["S3 /curated/fact_reviews"]
  AF["Airflow DAG\n(DatabricksSubmitRunOperator)"]
  RS["Amazon Redshift\n(schema: airbnb)"]
  BI["SQL / Views / BI"]

  S3Raw --> DBXClean --> S3Cur --> LLM --> S3Fact
  AF --> DBXClean
  AF --> LLM
  S3Cur -->|COPY (Parquet)| RS
  S3Fact -->|COPY (Parquet)| RS --> BI

📁 Repo Contents

dags/airbnb_databricks_dag.py — Airflow DAG that runs clean → sentiment on Databricks.

notebooks/clean_transformed.py — Spark ETL: /raw → /curated (partitioned by ds).

notebooks/sentiment_analysis.py — Samples reviews, calls OpenAI, writes /curated/fact_reviews.

sql/redshift_setup_and_analysis.sql — DDL, COPY loads, DQ checks, and analytical queries.

docker-compose.yml — Minimal Airflow stack (webserver, scheduler, Postgres).

docs/ (optional) — Screenshots, pipeline diagram.

🔧 Prerequisites

S3 bucket:

/raw/listings/ and /raw/reviews/ (CSV)

Databricks:

Workspace + cluster ID

Secret scope openai with key OPENAI_API_KEY

OpenAI:

API key (same value as above)

Redshift (Serverless or RA3):

IAM role with s3:ListBucket on s3://<your-bucket> and
s3:GetObject on s3://<your-bucket>/curated/*

🚀 Quickstart
1) Bring up Airflow (Docker)
# from repo root
printf "AIRFLOW_UID=%s\nAIRFLOW_GID=0\n" "$(id -u)" > .env
docker compose down -v
docker compose up -d


Open http://localhost:8080
 (default creds in this compose: admin / admin).

2) Configure Airflow

Connection: Admin → Connections → +

Conn Id: databricks_default

Conn Type: Databricks

Host: https://<your-workspace>.cloud.databricks.com (no trailing slash)

Password: <your-Databricks-PAT>

Variables: Admin → Variables → +

databricks_cluster_id → e.g. 0825-203211-a2sy9jbk

clean_nb_path → e.g. /Shared/airbnb/clean_transformed

sentiment_nb_path → e.g. /Shared/airbnb/sentiment_analysis

3) Trigger the DAG

In the UI (or CLI), trigger with a date matching your snapshot:

{
  "run_ds": "2025-08-25",
  "max_rows": 1000,
  "sample_frac": 0,
  "max_tokens": 0,
  "seed": 42
}


What happens

clean_transformed.py writes:

/curated/dim_listing/ds=YYYY-MM-DD/

/curated/dim_date/ds=YYYY-MM-DD/

/curated/fact_reviews_stage/ds=YYYY-MM-DD/

sentiment_analysis.py reads fact_reviews_stage, adds sentiment, writes:

/curated/fact_reviews/ds=YYYY-MM-DD/

Tuning knobs (to control cost/latency): max_rows, sample_frac, max_tokens, seed.

4) Load Redshift & Explore

Run sql/redshift_setup_and_analysis.sql in Redshift Query Editor v2.
It will:

Create schema/tables (airbnb.dim_listing, airbnb.dim_date, airbnb.fact_reviews)

COPY Parquet from S3 for your ds

Run data-quality checks (row counts, PK/joins)

Create two convenience views and several analysis queries

🧱 Data Model

dim_date(date_key, yyyy, mm, dd, dow, is_weekend)

dim_listing(listing_id PK, price, capacity, superhost, availability_*, …)

fact_reviews(review_id PK, listing_id FK, date_key FK, comments, tokens_ct, sentiment_label, sentiment_score)

All are daily snapshots keyed by ds in the S3 layout (Redshift loads per partition).

📊 Sample Insights (from included SQL)

Inventory mix by property_type and room_type

Price bands vs. review satisfaction

Superhost impact on price and ratings

Review seasonality (by month / weekend)

Positive vs. negative sentiment by city/neighborhood

Top and bottom listings by sentiment (min review threshold)

🧪 Reliability & Cost Controls

Safe LLM calls: batched, retries, strict JSON extraction, conservative fallbacks

Sampling: max_rows, sample_frac, max_tokens widgets to cap volume/cost

Data quality: counts, dedupe checks, listing join coverage in SQL

Idempotent loads: partition overwrite by ds, COPY per partition

🛠️ Skills Demonstrated

Lakehouse ELT (Spark → Parquet on S3), Airflow orchestration, Redshift warehousing, production hygiene (secrets, retries, partitioning, DQ checks), and LLM augmentation at scale with cost guardrails.

⚖️ Notes

Use your own dataset and respect its license.

Replace bucket/ARN/host/cluster IDs with your values.

For a “single-shot” portfolio run, trigger the DAG with a fixed run_ds, load Redshift once, and keep screenshots in docs/.
