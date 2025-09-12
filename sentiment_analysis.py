# sentiment_analysis.py — light/sampled scoring with safe ds fallback

"""
Reads curated/fact_reviews_stage for a given ds, runs small-batch
OpenAI sentiment, and writes curated/fact_reviews/ds=YYYY-MM-DD.

Designed to be driven by Airflow/Databricks widgets but safe to run by hand.
You can control sample size with widgets/env (see knobs below).
"""

from datetime import date
import os, json, re, time, random
from typing import Iterable, List, Tuple

from pyspark.sql import functions as F, types as T
from openai import OpenAI

CUR = "s3a://airbnb-bucket-data/curated"

# ---------- Resolve RUN_DS (widget -> env default -> today) ----------
DEFAULT_DS = date.today().isoformat()
RUN_DS = None
if "dbutils" in globals():
    try:
        RUN_DS = dbutils.widgets.get("RUN_DS")
    except Exception:
        dbutils.widgets.text("RUN_DS", DEFAULT_DS)
        RUN_DS = dbutils.widgets.get("RUN_DS")
RUN_DS = RUN_DS or DEFAULT_DS

# ---------- Sampling knobs (can be passed via Databricks widgets) ----------
def _get_widget_or_default(name, default):
    if "dbutils" in globals():
        try:
            return dbutils.widgets.get(name)
        except Exception:
            dbutils.widgets.text(name, str(default))
            return dbutils.widgets.get(name)
    return str(default)

MAX_ROWS    = int(_get_widget_or_default("MAX_ROWS",     1000))  # hard cap
SAMPLE_FRAC = float(_get_widget_or_default("SAMPLE_FRAC", 0.0))  # 0 = off
MAX_TOKENS  = int(_get_widget_or_default("MAX_TOKENS",   0))     # 0 = off
SEED        = int(_get_widget_or_default("SEED",         42))

MODEL, MAX_CHARS = "gpt-4o-mini", 1000
BATCH_SIZE, SLEEP_SEC = 8, 0.25

print(f"[sentiment] RUN_DS={RUN_DS}  MAX_ROWS={MAX_ROWS}  SAMPLE_FRAC={SAMPLE_FRAC}  MAX_TOKENS={MAX_TOKENS}  SEED={SEED}")

# ---------- OpenAI client (env first, then Databricks secret) ----------
api_key = os.environ.get("OPENAI_API_KEY")
if not api_key and "dbutils" in globals():
    try:
        api_key = dbutils.secrets.get("openai", "OPENAI_API_KEY")
        os.environ["OPENAI_API_KEY"] = api_key
    except Exception:
        api_key = None
if not api_key:
    raise RuntimeError("OPENAI_API_KEY missing (env or Databricks secret).")
client = OpenAI(api_key=api_key)

# ---------- Pick a valid input partition (fallback to latest) ----------
stage_root = f"{CUR}/fact_reviews_stage"
requested_path = f"{stage_root}/ds={RUN_DS}"

def _path_exists(p: str) -> bool:
    try:
        if "dbutils" in globals():
            dbutils.fs.ls(p)
            return True
    except Exception:
        return False
    return False

if not _path_exists(requested_path):
    latest_ds = None
    try:
        parts = []
        for x in dbutils.fs.ls(stage_root):
            if x.name.startswith("ds="):  # e.g. ds=2025-08-25/
                parts.append(x.name.strip("/").split("=", 1)[1])
        if parts:
            latest_ds = sorted(parts)[-1]  # ISO date sorts lexicographically
    except Exception:
        pass

    if not latest_ds:
        msg = f"[sentiment] No stage partitions found under {stage_root}. Run clean_transformed first."
        print(msg)
        if "dbutils" in globals():
            dbutils.notebook.exit("EMPTY_STAGE")
        raise RuntimeError(msg)

    print(f"[sentiment] Requested ds={RUN_DS} not found; falling back to latest ds={latest_ds}.")
    RUN_DS = latest_ds

read_path = f"{stage_root}/ds={RUN_DS}"
print(f"[sentiment] Reading input from {read_path}")

# ---------- Read + (optional) filter/sample ----------
df = (spark.read.parquet(read_path)
        .select("review_id","listing_id","date_key","comments","tokens_ct")
        .where("comments is not null and length(comments) > 0"))

if MAX_TOKENS and "tokens_ct" in df.columns:
    df = df.where(F.col("tokens_ct") <= MAX_TOKENS)

if SAMPLE_FRAC and SAMPLE_FRAC > 0:
    df = df.sample(withReplacement=False, fraction=SAMPLE_FRAC, seed=SEED)

df = df.orderBy(F.rand(SEED)).limit(MAX_ROWS)

# Quick exit if nothing to score
if df.limit(1).count() == 0:
    print(f"[sentiment] No comments to score for ds={RUN_DS} (after sampling/filters).")
    if "dbutils" in globals():
        dbutils.notebook.exit("OK")

# ---------- Classifier ----------
SYS = (
  "You are a strict sentiment classifier. "
  'Return ONLY JSON: {"results":[{"label":"POSITIVE|NEGATIVE","confidence":0..1}, ...]}'
)

def classify_batch(texts: List[str], max_retries: int = 3, base_sleep: float = 0.6) -> List[Tuple[str, float]]:
    """Classify up to BATCH_SIZE texts with simple retries/backoff."""
    if not texts:
        return []
    numbered = "\n".join(f"{i+1}. '''{(t or '')[:MAX_CHARS]}'''" for i, t in enumerate(texts))
    prompt = f"{SYS}\n\nClassify the following reviews:\n{numbered}"
    last_err = None
    for attempt in range(max_retries):
        try:
            r = client.responses.create(model=MODEL, input=prompt, temperature=0)
            out = getattr(r, "output_text", "") or str(r)
            m = re.search(r"\{.*\}", out, re.S)
            items = (json.loads(m.group(0)).get("results", [])) if m else []
            if len(items) != len(texts):
                items = [{"label": "NEGATIVE", "confidence": 0.0} for _ in texts]
            pairs: List[Tuple[str, float]] = []
            for x in items:
                lab = str(x.get("label","NEGATIVE")).upper()
                if lab not in ("POSITIVE","NEGATIVE"):
                    lab = "NEGATIVE"
                conf = float(x.get("confidence",0.0))
                pairs.append((lab, conf))
            return pairs
        except Exception as e:
            last_err = str(e)
            time.sleep(base_sleep * (2 ** attempt) + random.uniform(0, 0.3))
    # all retries failed → conservative defaults
    return [("NEGATIVE", 0.0)] * len(texts)

# ---------- Small, driver-side batching (safe because we limited up front) ----------
def _chunked(iterable: Iterable, size: int):
    batch = []
    for x in iterable:
        batch.append(x)
        if len(batch) >= size:
            yield batch; batch = []
    if batch: 
        yield batch

rows_iter = df.toLocalIterator()

schema = T.StructType([
    T.StructField("review_id",       T.StringType()),
    T.StructField("listing_id",      T.StringType()),
    T.StructField("date_key",        T.IntegerType()),
    T.StructField("comments",        T.StringType()),
    T.StructField("tokens_ct",       T.IntegerType()),
    T.StructField("sentiment_label", T.StringType()),
    T.StructField("sentiment_score", T.DoubleType()),
])

buffer = []
for big in _chunked(rows_iter, 1000):
    texts = [r["comments"] or "" for r in big]
    labels, scores = [], []
    for i in range(0, len(texts), BATCH_SIZE):
        pairs = classify_batch(texts[i:i+BATCH_SIZE])
        if pairs:
            l, s = zip(*pairs); labels += list(l); scores += list(s)
        time.sleep(SLEEP_SEC)
    for r, lab, sc in zip(big, labels, scores):
        buffer.append((r["review_id"], r["listing_id"], r["date_key"],
                       r["comments"], r["tokens_ct"], lab, float(sc)))

scored_df = spark.createDataFrame(buffer, schema=schema).withColumn("ds", F.to_date(F.lit(RUN_DS)))

# ---------- Write output partition ----------
out_base = f"{CUR}/fact_reviews"
out_path = f"{out_base}/ds={RUN_DS}"
if "dbutils" in globals():
    dbutils.fs.rm(out_path, True)
(scored_df.write.mode("overwrite").partitionBy("ds").parquet(out_base))

print(f" Wrote: {out_path}  rows: {scored_df.count()}")
