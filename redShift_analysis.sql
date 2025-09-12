/* ============================================================================
   Airbnb — Redshift Schema, Loads & Analysis
   Author: inioluwa
   Notes:
     - Designed for curated Parquet written by Databricks:
       s3://<bucket>/curated/{dim_listing,dim_date,fact_reviews}/ds=:DS_DAY/
     - Tables do NOT include a ds column. We treat each COPY as a full snapshot.
     - All COPYs read from a single ds partition (day).
   ============================================================================ */

-- ─────────────────────────────────────────────────────────────────────────────
-- 0) One-time schema housekeeping (safe to re-run)
-- ─────────────────────────────────────────────────────────────────────────────

-- Optional: start completely fresh
-- DROP SCHEMA IF EXISTS airbnb CASCADE;

CREATE SCHEMA IF NOT EXISTS airbnb;
SET search_path TO airbnb;

-- Clean up objects if they exist (idempotent)
DROP VIEW  IF EXISTS vw_city_sentiment_daily;
DROP VIEW  IF EXISTS vw_listing_sentiment;
DROP TABLE IF EXISTS fact_reviews;
DROP TABLE IF EXISTS dim_listing;
DROP TABLE IF EXISTS dim_date;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1) DDLs — star-ish model (small but realistic)
-- ─────────────────────────────────────────────────────────────────────────────

/* Dimension: Calendar keyed by yyyymmdd */
CREATE TABLE dim_date (
  date_key     INTEGER PRIMARY KEY,  -- e.g., 20250825
  yyyy         INTEGER,
  mm           INTEGER,
  dd           INTEGER,
  dow          INTEGER,              -- 1..7
  is_weekend   BOOLEAN
);

/* Dimension: Listings (one row per active listing on the day) */
CREATE TABLE dim_listing (
  listing_id                       VARCHAR(64)   PRIMARY KEY,
  host_id                          BIGINT,               -- curated output often numeric
  city                             VARCHAR(64),
  state                            VARCHAR(64),
  country                          VARCHAR(64),
  property_type                    VARCHAR(200),
  room_type                        VARCHAR(50),
  capacity                         INTEGER,
  price                            DOUBLE PRECISION,
  price_bucket                     VARCHAR(16),
  bathrooms                        DOUBLE PRECISION,
  bedrooms                         INTEGER,
  beds                             INTEGER,
  neighbourhood_cleansed           VARCHAR(200),
  neighbourhood_group_cleansed     VARCHAR(200),
  latitude                         DOUBLE PRECISION,
  longitude                        DOUBLE PRECISION,
  minimum_nights                   INTEGER,
  maximum_nights                   INTEGER,
  availability_30                  INTEGER,
  availability_60                  INTEGER,
  availability_90                  INTEGER,
  availability_365                 INTEGER,
  number_of_reviews                INTEGER,
  number_of_reviews_ltm            INTEGER,
  number_of_reviews_l30d           INTEGER,
  review_scores_rating             DOUBLE PRECISION,
  host_is_superhost                BOOLEAN,
  host_response_time               VARCHAR(50),
  host_response_rate               DOUBLE PRECISION,
  host_acceptance_rate             DOUBLE PRECISION,
  instant_bookable                 BOOLEAN,
  amenities_count                  INTEGER,
  created_at                       TIMESTAMP
);

/* Fact: Reviews with LLM sentiment */
CREATE TABLE fact_reviews (
  review_id        VARCHAR(64) PRIMARY KEY,
  listing_id       VARCHAR(64) REFERENCES dim_listing(listing_id),
  date_key         INTEGER      REFERENCES dim_date(date_key),
  rating           INTEGER,
  comments         VARCHAR(65535),
  tokens_ct        INTEGER,
  sentiment_label  VARCHAR(16),           -- POSITIVE / NEGATIVE
  sentiment_score  DOUBLE PRECISION       -- model confidence [0..1]
);

-- (Optional) Let Redshift auto-tune dist/sort strategies
ALTER TABLE dim_listing  ALTER DISTSTYLE AUTO;
ALTER TABLE dim_date     ALTER DISTSTYLE AUTO;
ALTER TABLE fact_reviews ALTER DISTSTYLE AUTO;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2) Loads — point at the ds partition you want to publish
--     Replace :DS_DAY and the IAM role before running.
-- ─────────────────────────────────────────────────────────────────────────────

-- ► Set your run day here for readability in the script
--    (Redshift Query Editor doesn’t support variables; do a find/replace)
-- :DS_DAY = '2025-08-25';

-- Your IAM role that can read S3
--   Replace <ACCOUNT_ID> with your AWS account id (don’t hard-code in GitHub).
--   Or set this once in the Namespace (Serverless) and rely on default role.
-- iam_role = 'arn:aws:iam::<ACCOUNT_ID>:role/RedshiftS3Access';

-- Dim Date
TRUNCATE TABLE dim_date;
COPY dim_date
FROM 's3://airbnb-bucket-data/curated/dim_date/ds=:DS_DAY/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/RedshiftS3Access'
FORMAT AS PARQUET;
ANALYZE dim_date;

-- Dim Listing
TRUNCATE TABLE dim_listing;
COPY dim_listing (
  listing_id, host_id, city, state, country,
  property_type, room_type, capacity, price, price_bucket,
  bathrooms, bedrooms, beds,
  neighbourhood_cleansed, neighbourhood_group_cleansed,
  latitude, longitude,
  minimum_nights, maximum_nights,
  availability_30, availability_60, availability_90, availability_365,
  number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d,
  review_scores_rating,
  host_is_superhost, host_response_time, host_response_rate, host_acceptance_rate,
  instant_bookable, amenities_count, created_at
)
FROM 's3://airbnb-bucket-data/curated/dim_listing/ds=:DS_DAY/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/RedshiftS3Access'
FORMAT AS PARQUET;
ANALYZE dim_listing;

-- Fact Reviews
TRUNCATE TABLE fact_reviews;
COPY fact_reviews
  (review_id, listing_id, date_key, rating, comments, tokens_ct, sentiment_label, sentiment_score)
FROM 's3://airbnb-bucket-data/curated/fact_reviews/ds=:DS_DAY/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/RedshiftS3Access'
FORMAT AS PARQUET;
ANALYZE fact_reviews;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3) Data Quality gates — quick, explainable checks
-- ─────────────────────────────────────────────────────────────────────────────

-- Row counts
SELECT 'dim_date' AS table_name, COUNT(*) AS rows FROM dim_date
UNION ALL SELECT 'dim_listing', COUNT(*) FROM dim_listing
UNION ALL SELECT 'fact_reviews', COUNT(*) FROM fact_reviews;

-- Key nulls (should be zero)
SELECT 'dim_date.date_key_nulls' AS check_name, COUNT(*) AS n_bad FROM dim_date WHERE date_key IS NULL
UNION ALL
SELECT 'dim_listing.listing_id_nulls', COUNT(*) FROM dim_listing WHERE listing_id IS NULL
UNION ALL
SELECT 'fact_reviews.review_id_nulls', COUNT(*) FROM fact_reviews WHERE review_id IS NULL;

-- Duplicate PKs (should be zero)
SELECT 'dup_listing_id' AS check_name, listing_id, COUNT(*) AS c
FROM dim_listing GROUP BY 1,2 HAVING COUNT(*) > 1
UNION ALL
SELECT 'dup_review_id', review_id, COUNT(*) FROM fact_reviews GROUP BY 1,2 HAVING COUNT(*) > 1;

-- Referential integrity (facts should match dims)
SELECT COUNT(*) AS fact_without_listing
FROM fact_reviews f LEFT JOIN dim_listing l USING (listing_id)
WHERE l.listing_id IS NULL;

SELECT COUNT(*) AS fact_without_calendar
FROM fact_reviews f LEFT JOIN dim_date d ON d.date_key = f.date_key
WHERE d.date_key IS NULL;

-- Basic reasonableness
SELECT
  SUM(CASE WHEN price < 0 OR price > 10000 THEN 1 ELSE 0 END) AS price_out_of_range,
  SUM(CASE WHEN review_scores_rating < 0 OR review_scores_rating > 5 THEN 1 ELSE 0 END) AS rating_out_of_range
FROM dim_listing;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4) Analysis Portfolio — employer-friendly slices
--     (Each block answers a concrete product/ops/business question.)
-- ─────────────────────────────────────────────────────────────────────────────

-- 4.1 Supply mix (room_type x price_band)
SELECT room_type, price_bucket, COUNT(*) AS listings
FROM dim_listing
GROUP BY 1,2
ORDER BY 1,2;

-- 4.2 Price landscape by property_type (avg/median/p90)
SELECT property_type,
       COUNT(*) AS listings,
       ROUND(AVG(price),2) AS avg_price,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price),2) AS median_price,
       ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY price),2) AS p90_price
FROM dim_listing
WHERE price IS NOT NULL
GROUP BY 1
ORDER BY listings DESC;

-- 4.3 Top neighborhoods by supply
SELECT neighbourhood_cleansed, COUNT(*) AS listings
FROM dim_listing
GROUP BY 1
ORDER BY listings DESC
LIMIT 20;

-- 4.4 Are Superhosts different? (pricing + ratings)
SELECT host_is_superhost,
       COUNT(*) AS listings,
       ROUND(AVG(price),2) AS avg_price,
       ROUND(AVG(review_scores_rating),2) AS avg_rating
FROM dim_listing
GROUP BY 1
ORDER BY 1 DESC;

-- 4.5 Instant bookable ≟ guest satisfaction
SELECT instant_bookable,
       COUNT(*) AS listings,
       ROUND(AVG(review_scores_rating),2) AS avg_rating
FROM dim_listing
GROUP BY 1
ORDER BY 1 DESC;

-- 4.6 Review volume by month (fact -> date)
SELECT d.yyyy, d.mm, COUNT(*) AS reviews
FROM fact_reviews f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY 1,2
ORDER BY 1,2;

-- 4.7 Sentiment split overall
SELECT sentiment_label, COUNT(*) AS reviews
FROM fact_reviews
GROUP BY 1
ORDER BY reviews DESC;

-- 4.8 Sentiment by city (requires enough volume)
SELECT l.city,
       COUNT(*) AS reviews,
       ROUND(AVG(CASE WHEN f.sentiment_label='POSITIVE' THEN 1.0 ELSE 0.0 END),3) AS positive_rate,
       ROUND(AVG(f.sentiment_score),3) AS avg_conf
FROM fact_reviews f
JOIN dim_listing l ON l.listing_id = f.listing_id
GROUP BY 1
HAVING COUNT(*) >= 200
ORDER BY positive_rate DESC;

-- 4.9 Best / worst listings by sentiment (min 20 reviews)
WITH agg AS (
  SELECT listing_id,
         COUNT(*) AS n_reviews,
         AVG(CASE WHEN sentiment_label='POSITIVE' THEN 1.0 ELSE 0.0 END) AS positive_rate
  FROM fact_reviews
  GROUP BY 1
)
SELECT 'TOP' AS band, listing_id, n_reviews, ROUND(positive_rate,3) AS positive_rate
FROM agg
WHERE n_reviews >= 20
ORDER BY positive_rate DESC, n_reviews DESC
LIMIT 10
UNION ALL
SELECT 'BOTTOM', listing_id, n_reviews, ROUND(positive_rate,3)
FROM agg
WHERE n_reviews >= 20
ORDER BY positive_rate ASC, n_reviews DESC
LIMIT 10;

-- 4.10 Are higher-priced stays reviewed more positively?
SELECT
  CASE
    WHEN l.price < 50  THEN 'lt_50'
    WHEN l.price < 100 THEN '50_99'
    WHEN l.price < 200 THEN '100_199'
    WHEN l.price < 400 THEN '200_399'
    ELSE 'gte_400'
  END AS price_bucket_calc,
  COUNT(*) AS reviews,
  ROUND(AVG(CASE WHEN f.sentiment_label='POSITIVE' THEN 1.0 ELSE 0.0 END),3) AS positive_rate
FROM fact_reviews f
JOIN dim_listing l ON l.listing_id = f.listing_id
GROUP BY 1
ORDER BY 1;

-- 4.11 Availability signal — where is inventory easiest to book?
SELECT neighbourhood_cleansed,
       ROUND(AVG(availability_30),1) AS avg_avail_30,
       COUNT(*) AS listings
FROM dim_listing
GROUP BY 1
HAVING COUNT(*) >= 50
ORDER BY avg_avail_30 DESC
LIMIT 20;

-- 4.12 Value picks — low price, high rating
SELECT listing_id, price, review_scores_rating
FROM dim_listing
WHERE price BETWEEN 60 AND 150
  AND review_scores_rating >= 4.8
ORDER BY review_scores_rating DESC, price ASC
LIMIT 50;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5) Reusable Views (great for BI / dashboards)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW vw_listing_sentiment AS
SELECT f.listing_id,
       COUNT(*) AS reviews,
       AVG(CASE WHEN f.sentiment_label='POSITIVE' THEN 1.0 ELSE 0.0 END) AS positive_rate,
       AVG(f.sentiment_score) AS avg_conf
FROM fact_reviews f
GROUP BY 1;

CREATE OR REPLACE VIEW vw_city_sentiment_daily AS
SELECT d.yyyy, d.mm, d.dd, l.city,
       COUNT(*) AS reviews,
       AVG(CASE WHEN f.sentiment_label='POSITIVE' THEN 1.0 ELSE 0.0 END) AS positive_rate
FROM fact_reviews f
JOIN dim_listing l ON l.listing_id = f.listing_id
JOIN dim_date    d ON d.date_key   = f.date_key
GROUP BY 1,2,3,4;

-- Example view usage: list inventory with attached sentiment KPIs
SELECT l.listing_id, l.city, l.price, l.review_scores_rating,
       s.reviews AS reviews_scored, ROUND(s.positive_rate,3) AS positive_rate, ROUND(s.avg_conf,3) AS avg_conf
FROM dim_listing l
LEFT JOIN vw_listing_sentiment s ON s.listing_id = l.listing_id
ORDER BY s.positive_rate DESC NULLS LAST
LIMIT 50;

