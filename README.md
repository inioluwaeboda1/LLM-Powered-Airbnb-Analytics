# Airbnb Reviews Analytics (Databricks + Airflow + Redshift + LLM Sentiment)

End-to-end ELT that cleans raw Airbnb data in Databricks, enriches reviews with LLM sentiment, orchestrates runs via Airflow (Docker), and lands analytics-ready tables in Amazon Redshift. The goal: show practical data engineering chops (orchestration, cloud storage, Lakehouse transforms, warehouse modeling, and using an LLM safely/cost-aware).

---

## ⚙️ Architecture (high level)

```mermaid
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
