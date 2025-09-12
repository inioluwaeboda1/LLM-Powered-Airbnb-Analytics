# clean_transformed.py  — Databricks notebook script

"""
Curate Airbnb raw CSVs from S3 into analysis-ready Parquet partitions.

Outputs (partitioned by ds=YYYY-MM-DD under s3a://.../curated/):
  • dim_listing        – one clean row per listing
  • dim_date           – calendar rows derived from review dates
  • fact_reviews_stage – per-review rows (with comments) for LLM scoring

This notebook is called by Airflow with a RUN_DS parameter, but it also
runs fine by hand (it will create the widget if missing).
"""

from datetime import date
from pyspark.sql import functions as F, types as T

# ------------------------------- Config ------------------------------------

RAW = "s3a://airbnb-bucket-data/raw"
CUR = "s3a://airbnb-bucket-data/curated"

# Resolve RUN_DS from a Databricks widget or fall back to a default
DEFAULT_DS = "2025-08-25"  # <- change for ad-hoc runs
if "dbutils" in globals():
    try:
        RUN_DS = dbutils.widgets.get("RUN_DS")
    except Exception:
        dbutils.widgets.text("RUN_DS", DEFAULT_DS)
        RUN_DS = dbutils.widgets.get("RUN_DS")
else:
    RUN_DS = DEFAULT_DS

print(f"[clean_transformed] Using RUN_DS={RUN_DS}")

# Common CSV options for messy/large CSVs
read_opts = dict(
    header="true", inferSchema="true", sep=",",
    quote='"', escape='"', multiLine="true", mode="PERMISSIVE"
)

# ------------------------------ Helpers ------------------------------------

def to_date_key(c):
    """YYYYMMDD integer key (good for joins, clustering, Redshift)."""
    return (F.year(c)*10000 + F.month(c)*100 + F.dayofmonth(c)).cast("int")

def price_to_bucket(c):
    """Simple price bands for aggregates/visuals."""
    return (F.when(c < 50, "lt_50")
             .when((c >= 50) & (c < 100), "50_99")
             .when((c >= 100) & (c < 200), "100_199")
             .when((c >= 200) & (c < 400), "200_399")
             .otherwise("gte_400"))

def norm(df):
    """Lowercase/trim column names so downstream code is stable."""
    for old in df.columns:
        new = old.strip().lower()
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df

def parse_bool(colname):
    """Coerce common truthy strings to boolean."""
    return (F.lower(F.col(colname)).isin("t","true","1","y","yes")).cast("boolean")

def safe_pct(colname):
    """'97%' → 97.0; malformed → NULL."""
    return F.expr(f"try_cast(nullif(regexp_replace(cast({colname} as string), '%', ''), '') as double)")

def parse_bathrooms():
    """Handle '1.5 baths' or numeric bathrooms to double."""
    return F.coalesce(
        F.col("bathrooms").cast("double"),
        F.expr("try_cast(nullif(regexp_extract(coalesce(cast(bathrooms_text as string), ''), '(\\d+(\\.\\d+)?)', 1), '') as double)")
    )

def word_count(col):
    """Rough token/word count used later to cap LLM cost/latency."""
    return F.size(F.split(F.coalesce(col, F.lit("")), r"\s+"))

def overwrite_partition_path(df, base_path, table, run_ds):
    """Idempotent write of exactly one ds partition."""
    path = f"{base_path}/{table}/ds={run_ds}"
    dbutils.fs.rm(path, True)
    df.write.mode("overwrite").parquet(path)
    return path

# ------------------------------- Read raw ----------------------------------

listings = norm(spark.read.options(**read_opts).csv(f"{RAW}/listings/"))
reviews  = norm(spark.read.options(**read_opts).csv(f"{RAW}/reviews/"))

# ------------------------ Standardize keys/types ---------------------------

# Ensure expected IDs exist and are strings (avoids join/type drift)
if "listing_id" not in listings.columns and "id" in listings.columns:
    listings = listings.withColumnRenamed("id", "listing_id")
if "review_id" not in reviews.columns and "id" in reviews.columns:
    reviews = reviews.withColumnRenamed("id", "review_id")

listings = listings.withColumn("listing_id", F.col("listing_id").cast("string"))
reviews  = (reviews
            .withColumn("listing_id", F.col("listing_id").cast("string"))
            .withColumn("review_id",  F.col("review_id").cast("string")))

# ---------------------- Listings cleanup/enrichment ------------------------

listings = (
    listings
    # If your dataset spans multiple cities, replace these literals with real fields
    .withColumn("city",    F.lit("London"))
    .withColumn("state",   F.lit("England"))
    .withColumn("country", F.lit("UK"))
    # Price: strip currency symbols/commas safely
    .withColumn("price_num", F.expr(
        "try_cast(nullif(regexp_replace(cast(price as string), '[^0-9.]', ''), '') as double)"
    ))
    .withColumn("price_bucket",      price_to_bucket(F.col("price_num")))
    .withColumn("bathrooms_count",   parse_bathrooms())
    .withColumn("host_is_superhost_bool", parse_bool("host_is_superhost"))
    .withColumn("instant_bookable_bool",  parse_bool("instant_bookable"))
    .withColumn("has_availability_bool",  parse_bool("has_availability"))
    .withColumn("host_response_rate_pct",    safe_pct("host_response_rate"))
    .withColumn("host_acceptance_rate_pct",  safe_pct("host_acceptance_rate"))
    .dropna(subset=["listing_id"])
)

# amenities: try JSON array first; otherwise split a flat string; count non-empty items
amen_arr     = F.from_json(F.col("amenities"), T.ArrayType(T.StringType()))
fallback_str = F.translate(F.coalesce(F.col("amenities"), F.lit("")), '[]"', '   ')
fallback_arr = F.split(F.regexp_replace(fallback_str, r'\s*,\s*', ','), ',')

listings = (
    listings
    .withColumn("amenities_array", F.when(amen_arr.isNotNull(), amen_arr).otherwise(fallback_arr))
    .withColumn("amenities_count", F.size(F.array_remove(F.col("amenities_array"), "")))
    .drop("amenities_array")
)

# -------------------------- Dimensions & Staging ---------------------------

# dim_listing: one row per listing (de-duplicated)
dim_listing = (
    listings
    .select(
        "listing_id","host_id",
        "city","state","country",
        "property_type","room_type",
        F.col("accommodates").alias("capacity"),
        F.col("price_num").alias("price"), "price_bucket",
        F.col("bathrooms_count").alias("bathrooms"),
        "bedrooms","beds",
        "neighbourhood_cleansed","neighbourhood_group_cleansed",
        "latitude","longitude",
        "minimum_nights","maximum_nights",
        "availability_30","availability_60","availability_90","availability_365",
        "number_of_reviews","number_of_reviews_ltm","number_of_reviews_l30d",
        "review_scores_rating",
        F.col("host_is_superhost_bool").alias("host_is_superhost"),
        "host_response_time",
        F.col("host_response_rate_pct").alias("host_response_rate"),
        F.col("host_acceptance_rate_pct").alias("host_acceptance_rate"),
        F.col("instant_bookable_bool").alias("instant_bookable"),
        "amenities_count"
    )
    .dropDuplicates(["listing_id"])
    .withColumn("created_at", F.current_timestamp())
)

# Reviews cleaned: valid date + unique review rows
reviews = (
    reviews
    .withColumn("review_date", F.to_date("date"))
    .dropna(subset=["listing_id","review_id","review_date"])
    .dropDuplicates(["review_id"])
)

# dim_date: derived calendar rows from actual review dates present
dim_date = (
    reviews.select(F.col("review_date").alias("d")).dropDuplicates()
    .withColumn("date_key",   to_date_key(F.col("d")))
    .withColumn("yyyy",       F.year("d"))
    .withColumn("mm",         F.month("d"))
    .withColumn("dd",         F.dayofmonth("d"))
    .withColumn("dow",        F.dayofweek("d"))
    .withColumn("is_weekend", F.col("dow").isin([1,7]))
    .select("date_key","yyyy","mm","dd","dow","is_weekend")
)

# fact_reviews_stage: per-review records with comments (input for LLM)
fact_reviews_stage = (
    reviews.alias("r")
    # Keep only reviews tied to a known listing
    .join(dim_listing.select("listing_id").alias("l_idx"), "listing_id", "left_semi")
    .withColumn("date_key",  to_date_key(F.col("review_date")))
    .withColumn("tokens_ct", word_count(F.col("comments")))
    .withColumn("rating",    F.lit(None).cast("int"))  # placeholder when per-review rating missing
    .select("review_id","listing_id","date_key","rating","comments","tokens_ct")
)

# ------------------------------- Write out ---------------------------------

p1 = overwrite_partition_path(dim_listing,        CUR, "dim_listing",        RUN_DS)
p2 = overwrite_partition_path(dim_date,           CUR, "dim_date",           RUN_DS)
p3 = overwrite_partition_path(fact_reviews_stage, CUR, "fact_reviews_stage", RUN_DS)

print("Curated write complete")
print(" ├─", p1)
print(" ├─", p2)
print(" └─", p3)

# Small sanity peek (safe even on big partitions)
display(spark.read.parquet(p1).limit(5))
display(spark.read.parquet(p3).select("review_id","listing_id","tokens_ct","comments").limit(5))
