Airbnb Reviews Analytics (Databricks + Airflow + Redshift + LLM Sentiment)
==========================================================================

End-to-end ELT that cleans raw Airbnb data in **Databricks**, enriches reviews with **LLM sentiment**, orchestrates runs via **Airflow (Docker)**, and lands analytics-ready tables in **Amazon Redshift**. The goal: show practical data engineering chops (orchestration, cloud storage, Lakehouse transforms, warehouse modeling, and using an LLM safely/cost-aware).

⚙️ Architecture (high level)
----------------------------

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   flowchart LR    S3Raw["S3 /raw\n(listings.csv, reviews.csv)"]    DBXClean["Databricks Notebook\nclean_transformed.py\n→ Parquet partitions"]    S3Cur["S3 /curated\n(dim_listing, dim_date,\n  fact_reviews_stage)"]    LLM["Databricks Notebook\nsentiment_analysis.py\n(OpenAI, batched)"]    S3Fact["S3 /curated/fact_reviews"]    AF["Airflow DAG\n(DatabricksSubmitRunOperator)"]    RS["Amazon Redshift\n(schema: airbnb)"]    BI["SQL / Views / BI"]    S3Raw --> DBXClean --> S3Cur --> LLM --> S3Fact    AF --> DBXClean    AF --> LLM    S3Cur -->|COPY (Parquet)| RS    S3Fact -->|COPY (Parquet)| RS --> BI   `

📁 What’s in this repo
----------------------

*   dags/airbnb\_databricks\_dag.py — runs **clean** then **sentiment** notebooks on Databricks.
    
*   clean\_transformed.py — reads S3 **/raw**, standardizes & writes **/curated** (partitioned by ds).
    
*   sentiment\_analysis.py — samples reviews, calls **OpenAI** to label sentiment, writes **/curated/fact\_reviews**.
    
*   redshift\_analysis.sql — DDL, COPY loads, data-quality checks, and **analysis queries** (+ views).
    
*   docker-compose.yml — minimal Airflow on Docker (webserver + scheduler + Postgres).
    
*   README.md — this file.
    

> Screenshots/diagram optional: add under docs/ and reference them here.

🔑 Tech choices (and why)
-------------------------

*   **Parquet on S3** for cheap, columnar, partitioned storage → easy COPY into Redshift.
    
*   **Databricks** to do robust Spark cleaning + light feature engineering.
    
*   **Airflow** to make the pipeline reproducible and parameterized (run\_ds, sampling knobs).
    
*   **OpenAI** for sentiment → wrapped with strict JSON schema, batching, retries, and sampling limits.
    
*   **Redshift** for fast analytical SQL and shareable views.
    

🚀 Quickstart (minimal)
-----------------------

**Prereqs**

*   S3 bucket with /raw/listings/ and /raw/reviews/ (CSV).
    
*   Databricks workspace + cluster; a secret openai/OPENAI\_API\_KEY.
    
*   OpenAI API key (same secret).
    
*   Redshift Serverless (IAM role with **ListBucket/GetObject** on your bucket).
    

**1) Airflow via Docker**

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   # From repo root  printf "AIRFLOW_UID=%s\nAIRFLOW_GID=0\n" "$(id -u)" > .env  docker compose down -v  docker compose up -d   `

Open http://localhost:8080 → login (admin/admin by default in this compose).

**2) Airflow connection & variables**

*   Admin → **Connections** → databricks\_default
    
    *   Host: https://.cloud.databricks.com (no trailing slash)
        
    *   Password:
        
*   Admin → **Variables**
    
    *   databricks\_cluster\_id → e.g. 0825-203211-a2sy9jbk
        
    *   clean\_nb\_path → e.g. /Shared/airbnb/clean\_transformed
        
    *   sentiment\_nb\_path → e.g. /Shared/airbnb/sentiment\_analysis
        

**3) Run the DAG**

*   {"run\_ds":"2025-08-25","max\_rows":1000,"sample\_frac":0,"max\_tokens":0,"seed":42}
    
*   The DAG:
    
    1.  writes /curated/{dim\_listing, dim\_date, fact\_reviews\_stage}/ds=YYYY-MM-DD/
        
    2.  writes /curated/fact\_reviews/ds=YYYY-MM-DD/ with sentiment.
        

**4) Load Redshift & analyze**

Open Redshift Query Editor v2 → run sql/redshift\_setup\_and\_analysis.sql(Find/replace :DS\_DAY with your date and set your IAM role ARN).

🧱 Data model (warehouse)
-------------------------

*   dim\_date(date\_key, yyyy, mm, dd, dow, is\_weekend)
    
*   dim\_listing(listing\_id PK, … attributes, price, superhost, availability\_\*)
    
*   fact\_reviews(review\_id PK, listing\_id FK, date\_key FK, comments, tokens\_ct, sentiment\_label, sentiment\_score)
    

All are **daily snapshots** (COPY from one ds partition).

📊 Example questions answered
-----------------------------

*   **Which neighborhoods have the most inventory?** (dim\_listing counts)
    
*   **Do Superhosts command higher price or ratings?** (avg price/rating by host\_is\_superhost)
    
*   **Is instant-book correlated with satisfaction?** (avg rating)
    
*   **Review seasonality?** (reviews by yyyy, mm)
    
*   **Where is sentiment strongest/weakest?** (city-level positive rate, listing top/bottom 10, price band vs positive rate)
    

These are all included in sql/redshift\_setup\_and\_analysis.sql (plus two reusable views).

🧪 Reliability & cost controls
------------------------------

*   **Sampling knobs** in sentiment\_analysis.py: MAX\_ROWS, SAMPLE\_FRAC, MAX\_TOKENS, SEED to keep cost/latency predictable.
    
*   **Batched LLM calls** with retries, strict JSON parsing, safe defaults if parsing fails.
    
*   **Data-quality checks** in SQL: row counts, PK duplicates, referential integrity, value ranges.
    

🔮 Extensions
-------------

*   Add **Great Expectations** for formal DQ tests.
    
*   Multi-class sentiment (e.g., positive/neutral/negative) or topic tagging.
    
*   Incremental model with ds column in Redshift and late-arriving handling.
    
*   Swap Redshift COPY for an ELT tool (e.g., dbt) to model downstream marts.
    

### Credit & data

This project expects an Airbnb-style listings/reviews d
