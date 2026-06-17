# Databricks notebook source
# MAGIC %md
# MAGIC # Module 2 — Spark Structured Streaming & Stateful Processing
# MAGIC ## Participant Notebook (type along with the demos, then complete Part B)
# MAGIC
# MAGIC We now **consume the same Kafka stream from Module 1** (`topic_0`, the Datagen ORDERS feed) with
# MAGIC **Spark Structured Streaming** and build a real data-prep pipeline: parse, clean bad data, window with
# MAGIC watermarks, and land an exactly-once **Bronze → Silver → Gold** medallion on the lakehouse.
# MAGIC
# MAGIC * **Part A — Demos (D1–D8).** Run each cell with the instructor: read the stream → build a DataFrame →
# MAGIC   the familiar DataFrame API → bad-data handling → triggers & monitoring → windowing → watermarks →
# MAGIC   the Bronze/Silver/Gold pipeline. Each maps to a **▶ TRY IT TOGETHER** slide.
# MAGIC * **Part B — Your Lab + Design Exercise.** You build it: fill in every `# TODO` line, then run.
# MAGIC   Ask a facilitator if you're stuck — the goal is to reason about the mechanism, not copy code.
# MAGIC
# MAGIC Every code cell is followed by a **"Reading the output"** cell explaining the *streaming mechanism*.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC * The Module 1 Confluent cluster + Datagen ORDERS connector streaming to **`topic_0`**, with the API key
# MAGIC   in Databricks secret scope **`kafka_demo`** (see the *Confluent Cloud Setup Guide*).
# MAGIC * A **Unity Catalog** catalog/schema you can write to (set `CATALOG`/`SCHEMA` below).
# MAGIC * A cluster on a recent DBR (Structured Streaming + the Kafka source are built in).
# MAGIC
# MAGIC **Note on event-time:** Datagen's `ordertime` is a random 2017 epoch, useless for watermarks. We use the
# MAGIC **Kafka record timestamp** as event-time (it is recent); D5 uses a small controlled Delta-table stream to
# MAGIC demonstrate late data deterministically.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — run this cell once at the start

# COMMAND ----------

import json, time, uuid
from pyspark.sql.functions import col, from_json, window, expr
from pyspark.sql.functions import sum as _sum, count as _count
from pyspark.sql.types import (StructType, StructField, StringType, LongType, DoubleType)

# --- credentials (never printed) ---
BOOT   = dbutils.secrets.get("kafka_demo", "bootstrap")
KEY    = dbutils.secrets.get("kafka_demo", "api_key")
SECRET = dbutils.secrets.get("kafka_demo", "api_secret")
TOPIC  = "topic_0"

# --- Spark Kafka source options (SASL_SSL / PLAIN) ---
# NOTE: Databricks SHADES its Kafka classes, so the JAAS login module must be the
# 'kafkashaded.' class, not the plain org.apache.kafka one (otherwise: "unable to find
# LoginModule class ... PlainLoginModule").
_jaas = (f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
         f'username="{KEY}" password="{SECRET}";')
kafka_opts = {
    "kafka.bootstrap.servers": BOOT,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism":    "PLAIN",
    "kafka.sasl.jaas.config":  _jaas,
}

# --- ORDERS JSON schema (Datagen ORDERS quickstart) ---
orders_schema = StructType([
    StructField("ordertime",  LongType()),
    StructField("orderid",    LongType()),
    StructField("itemid",     StringType()),
    StructField("orderunits", DoubleType()),
    StructField("address", StructType([
        StructField("city",    StringType()),
        StructField("state",   StringType()),
        StructField("zipcode", LongType()),
    ])),
])

# --- Unity Catalog target for Delta writes (CHANGE to a catalog/schema you can write to) ---
CATALOG = "ctl_training_dev"
SCHEMA  = "bootcamp_m2"
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")   # uncomment if you have metastore-admin rights
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# --- streaming checkpoints: serverless / UC needs a Volume path (NOT /tmp) ---
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/run_{uuid.uuid4().hex[:8]}"

def orders_stream(starting="latest"):
    """Read topic_0 as a stream and parse the JSON value. event_time = Kafka record timestamp."""
    raw = (spark.readStream.format("kafka").options(**kafka_opts)
           .option("subscribe", TOPIC)
           .option("startingOffsets", starting)
           .load())
    return (raw.select(
                from_json(col("value").cast("string"), orders_schema).alias("o"),
                col("timestamp").alias("event_time"),
                col("partition"), col("offset"))
            .select("o.orderid", "o.itemid", "o.orderunits", "event_time", "partition", "offset"))

def run_to_memory(df, name, mode="append", trigger_s=2, run_s=12):
    """Run a streaming query into an in-memory table for run_s seconds, then return it."""
    q = (df.writeStream.queryName(name).outputMode(mode)
         .format("memory").trigger(processingTime=f"{trigger_s} seconds").start())
    time.sleep(run_s)
    q.stop()
    return spark.table(name)

print("Spark Structured Streaming ready. Writing Delta to", f"{CATALOG}.{SCHEMA}",
      "| checkpoints under", CKPT)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * `orders_stream()` returns a **streaming DataFrame**: a query plan over an *unbounded* table, not data yet.
# MAGIC   Nothing runs until you start a `writeStream`.
# MAGIC * Credentials come from the `kafka_demo` secret scope and are folded into the Kafka JAAS config — the same
# MAGIC   secrets pattern as M1.
# MAGIC * We will write results as **Delta** tables in Unity Catalog and keep **checkpoints** on a per-run path so
# MAGIC   restarts resume cleanly.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART A — INSTRUCTOR DEMOS  (▶ TRY IT TOGETHER)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## D1 — A Stream Is an Unbounded Table  *(≈3 min)*
# MAGIC **Predict:** what columns does the Kafka source give us before we parse anything?
# MAGIC **Observe:** `isStreaming` is True; the schema is the raw Kafka envelope.

# COMMAND ----------

raw = (spark.readStream.format("kafka").options(**kafka_opts)
       .option("subscribe", TOPIC).option("startingOffsets", "latest").load())
print("isStreaming:", raw.isStreaming)
raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * The Kafka source always hands you the same **envelope**: `key`, `value` (both binary), `topic`,
# MAGIC   `partition`, `offset`, `timestamp`, `timestampType`. Your payload is inside `value` — still bytes.
# MAGIC * `isStreaming = True` means this DataFrame is an **incremental query over an unbounded table**: as new
# MAGIC   records arrive, Spark re-runs the query on just the new rows. Same DataFrame API as batch — different engine.
# MAGIC * `partition` / `offset` are the Kafka coordinates from M1; Structured Streaming tracks offsets for you in
# MAGIC   the checkpoint, which is how it recovers exactly-once.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D2 — From Raw Stream to a Typed DataFrame  *(≈5 min)*
# MAGIC The Kafka `value` is raw bytes. We turn the stream into a normal typed DataFrame in **three steps**:
# MAGIC cast to string, parse the JSON with a schema, then flatten the struct. (The setup's `orders_stream()`
# MAGIC helper wraps exactly these steps so later demos can reuse them.)
# MAGIC **Predict:** which timestamp should be event-time — `ordertime` (inside the JSON) or the Kafka `timestamp`?

# COMMAND ----------

# Step 1 — the raw value is binary; cast it to a JSON string (keep the Kafka timestamp as event_time)
as_text = raw.select(col("value").cast("string").alias("json"),
                     col("timestamp").alias("event_time"))

# Step 2 — parse the JSON string into a typed struct using orders_schema
parsed = as_text.select(from_json(col("json"), orders_schema).alias("o"), col("event_time"))

# Step 3 — flatten the struct into flat columns: now it's an ordinary DataFrame
orders = parsed.select("o.orderid", "o.itemid", "o.orderunits", "event_time")

orders.printSchema()
run_to_memory(orders, "orders_peek", run_s=14).show(8, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * **Three steps turn the stream into a usable DataFrame:** (1) `value.cast("string")` — Kafka stores bytes,
# MAGIC   so we decode to text; (2) `from_json(..., orders_schema)` parses the text into a typed **struct** —
# MAGIC   **schema-on-read**, where the schema is *your* contract (M5 makes it enforceable); (3) `select("o.orderid", ...)`
# MAGIC   flattens the struct into flat columns.
# MAGIC * The result `orders` is a perfectly ordinary DataFrame — `printSchema()` shows typed columns. The setup's
# MAGIC   `orders_stream()` helper does these exact three steps, so later demos call it instead of repeating them.
# MAGIC * We take **`event_time` from the Kafka record timestamp** (~now). The JSON `ordertime` is a random 2017
# MAGIC   value, so windowing on it would push every window into the past — *why event-time choice matters*.
# MAGIC * The `memory` sink is only for inspection; a real query writes to Delta (D6).

# COMMAND ----------

# MAGIC %md
# MAGIC ## D2b — The Same DataFrame API on a Stream  *(≈5 min)*
# MAGIC The key idea for anyone coming from batch PySpark: a streaming DataFrame uses the **identical
# MAGIC transformations** — `select`, `filter`/`where`, `withColumn`, `drop`, `groupBy().agg()`. Only the
# MAGIC **source** (`readStream`) and **sink** (`writeStream`) change.
# MAGIC **Predict:** will `withColumn` and `filter` behave any differently here than on a batch DataFrame?

# COMMAND ----------

from pyspark.sql.functions import upper, round as _round

# Ordinary, stateless transformations — exactly as in batch PySpark — applied to a STREAM:
shaped = (orders_stream("latest")
          .filter(col("orderunits") > 5)                               # filter / where
          .withColumn("revenue", _round(col("orderunits") * 9.99, 2))  # add a calculated column
          .withColumn("itemid", upper(col("itemid")))                  # transform an existing column
          .drop("partition", "offset")                                 # remove columns
          .select("orderid", "itemid", "orderunits", "revenue", "event_time"))  # choose/reorder columns
run_to_memory(shaped, "shaped_peek", run_s=30).show(8, truncate=False)

# COMMAND ----------

# A simple (non-windowed) streaming aggregation — the same groupBy/agg you use in batch.
# (Aggregations need a committed micro-batch, so give the slow ~1/sec source enough time.)
by_item = orders_stream("latest").groupBy("itemid").agg(_count("*").alias("orders"))
run_to_memory(by_item, "by_item", mode="complete", run_s=30).orderBy(col("orders").desc()).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * Every transformation here — `filter`, `withColumn`, `drop`, `select`, `groupBy().agg()` — is the **exact
# MAGIC   same DataFrame API as batch PySpark**. A streaming DataFrame is still just a DataFrame; only the **source**
# MAGIC   (`readStream`) and **sink** (`writeStream`) differ.
# MAGIC * **Stateless** row ops (select / filter / withColumn / drop / cast) run on each micro-batch independently and
# MAGIC   work in any output mode.
# MAGIC * `groupBy().agg()` is **stateful** — Spark carries the running counts across batches, so an aggregation needs
# MAGIC   `complete` or `update` mode (or `append` *with* a watermark). That state is exactly what windows and
# MAGIC   watermarks bound in the next demos.
# MAGIC * A few batch operations don't apply to an unbounded stream as-is (a global `sort`, or `distinct`/`limit`
# MAGIC   without a watermark) because they'd need the whole dataset — Spark will raise a clear error if you try.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D2c — Handling Bad & Malformed Records  *(≈5 min)*
# MAGIC Real feeds carry corrupt rows. We read a small messy source **as a stream**; `from_json` returns **null**
# MAGIC for anything it can't parse, so we route good rows onward and bad rows to a **quarantine** table.
# MAGIC (We use a tiny controlled source because the live `topic_0` is all-valid — you build the quarantine
# MAGIC *before* you need it.)
# MAGIC **Predict:** if a record's JSON is broken, what does `from_json` put in its columns?

# COMMAND ----------

# A tiny SOURCE table standing in for a messy feed: two valid orders, two broken.
spark.sql("DROP TABLE IF EXISTS raw_json_src")
spark.createDataFrame([
    ('{"orderid":1,"itemid":"Item_5","orderunits":2.0}',),
    ('{"orderid":2,"itemid":"Item_9","orderunits":3.5}',),
    ('{"orderid":3, "itemid": BROKEN',),     # malformed JSON
    ('not json at all',),                    # not JSON
], ["json"]).write.mode("overwrite").saveAsTable("raw_json_src")

# Read it AS A STREAM, parse, and route good vs bad to two Delta tables.
parsed = (spark.readStream.table("raw_json_src")
          .withColumn("o", from_json(col("json"), orders_schema)))
good = parsed.where(col("o.orderid").isNotNull()).select("o.orderid", "o.itemid", "o.orderunits")
bad  = parsed.where(col("o.orderid").isNull()).select("json")     # -> quarantine

# availableNow drains the bounded source once, then stops on its own
(good.writeStream.format("delta").option("checkpointLocation", f"{CKPT}/good")
     .trigger(availableNow=True).toTable(f"{CATALOG}.{SCHEMA}.orders_good")).awaitTermination()
(bad.writeStream.format("delta").option("checkpointLocation", f"{CKPT}/quarantine")
     .trigger(availableNow=True).toTable(f"{CATALOG}.{SCHEMA}.orders_quarantine")).awaitTermination()

print("good rows:", spark.table("orders_good").count(),
      "| quarantined:", spark.table("orders_quarantine").count())
print("\nquarantine:")
spark.table("orders_quarantine").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * `from_json` is **lenient**: a record it can't parse becomes **all-null**, not an error — a broken feed
# MAGIC   loses rows silently unless you test for it (`o.orderid IS NULL` flags the failures).
# MAGIC * The pipeline pattern: **two `writeStream`s** off the same parsed stream — good rows to the table, bad rows
# MAGIC   to a **quarantine** Delta table you triage later. Nothing is thrown away.
# MAGIC * `trigger(availableNow=True)` drained the bounded demo source once and stopped; against Kafka the identical
# MAGIC   split runs continuously. (For the raw corrupt text inline, add a `_corrupt_record` field + `mode=PERMISSIVE`.)
# MAGIC   Formal data-quality *contracts* are M5.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D3 — Micro-Batch Execution & Triggers  *(≈4 min)*
# MAGIC **Predict:** with a 3-second trigger, how often does a new batch run, and what is in each?
# MAGIC **Observe:** a monotonic `batchId` and a row count per batch.

# COMMAND ----------

def show_batch(bdf, bid):
    print(f"micro-batch {bid}: {bdf.count()} rows")

q = (orders_stream("latest").writeStream
     .foreachBatch(show_batch)
     .trigger(processingTime="3 seconds")
     .start())
time.sleep(16)
q.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * Structured Streaming runs as a series of **micro-batches**: every trigger, Spark reads the new Kafka
# MAGIC   offsets, runs the query on that slice, commits, and advances. `batchId` increases by one each time.
# MAGIC * The **trigger** sets the rhythm: `processingTime="3 seconds"` (fixed cadence), `availableNow=True`
# MAGIC   (drain everything available, then stop — great for backfills/tests), or `continuous` (low-latency, limited ops).
# MAGIC * `foreachBatch` hands you each micro-batch as a normal **batch DataFrame** — the bridge to anything batch
# MAGIC   code can do (multi-sink writes, MERGE, etc.).

# COMMAND ----------

# MAGIC %md
# MAGIC ## D3b — Triggers in Production & Monitoring  *(≈5 min)*
# MAGIC Two things every pipeline owner controls: **how the stream is triggered** and **how to watch it**. We read
# MAGIC live **metrics** from a running query, then run one **`availableNow`** batch (drain, then stop — the
# MAGIC scheduled-job pattern). `maxOffsetsPerTrigger` caps batch size for predictable cost.
# MAGIC **Predict:** with `trigger(availableNow=True)`, does the query keep running, or stop on its own?

# COMMAND ----------

# (1) MONITORING — run a normal query briefly and read its built-in metrics
mq = (orders_stream("latest").writeStream.queryName("mon")
      .format("memory").outputMode("append")
      .trigger(processingTime="2 seconds").start())
time.sleep(14)
mq.stop()
for p in mq.recentProgress[-3:]:
    print(f"batch {p['batchId']}: in={p['numInputRows']} rows, "
          f"{p.get('inputRowsPerSecond', 0):.1f} in/s, {p.get('processedRowsPerSecond', 0):.1f} proc/s")

# (2) availableNow — drain a bounded source in micro-batches, then STOP on its own
spark.sql("DROP TABLE IF EXISTS demo_src")
spark.range(500).withColumnRenamed("id", "orderid").write.mode("overwrite").saveAsTable("demo_src")
aq = (spark.readStream.table("demo_src")
      .writeStream.queryName("an").format("memory").outputMode("append")
      .trigger(availableNow=True).start())
aq.awaitTermination()                       # availableNow finishes by itself
print("availableNow stopped on its own:", not aq.isActive, "| rows processed:", spark.table("an").count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * **`recentProgress` / `lastProgress`** are the built-in metrics: `numInputRows`, `inputRowsPerSecond`,
# MAGIC   `processedRowsPerSecond` per micro-batch — enough to chart throughput and latency (the Spark UI Streaming
# MAGIC   tab shows the same). This is how you tell whether a stream is keeping up.
# MAGIC * **`trigger(availableNow=True)`** processes everything available *now* in micro-batches and then **stops by
# MAGIC   itself** — the basis of cost-efficient **scheduled** pipelines (a job wakes, drains new data, exits; no
# MAGIC   24/7 cluster). Contrast `processingTime` (always-on, fixed cadence) and `continuous` (low-latency, limited ops).
# MAGIC * **`maxOffsetsPerTrigger`** (Kafka) / `maxFilesPerTrigger` (files) cap how much each batch ingests — predictable
# MAGIC   batch size and cost, and smoother catch-up after an outage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D4 — Windowed Aggregation: rolling revenue  *(≈5 min)*
# MAGIC **Predict:** if we never tell Spark how late data can be, how long must it keep each window's running total?
# MAGIC **Observe:** revenue grouped into 5-minute event-time windows.

# COMMAND ----------

rev = (orders_stream("latest")
       .groupBy(window(col("event_time"), "30 seconds"))   # short window so several close during the run
       .agg(_sum("orderunits").alias("revenue"), _count("*").alias("orders")))
res = run_to_memory(rev, "rev5", mode="complete", run_s=150)   # raise/lower to watch more/fewer windows
res.orderBy("window").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * `window(event_time, "30 seconds")` buckets each record by its **event-time** into a tumbling window; the
# MAGIC   aggregate is **stateful** — Spark keeps a running total per open window in the **state store**. (We use a
# MAGIC   short window only so several close while you watch; production uses minutes — just change the string.)
# MAGIC * **`complete` output mode** re-emits the *entire* result table every batch (required for an aggregation
# MAGIC   with no watermark). You will see several 30-second windows accumulate during the run.
# MAGIC * Problem: with no watermark, Spark must assume a late record could update **any** past window, so it keeps
# MAGIC   **every** window's state forever → unbounded memory. The next demo fixes that.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Challenge 1 — Output Mode  *(≈2 min)*
# MAGIC **Predict:** you write a windowed aggregation to a sink and want only **finalized** windows appended once
# MAGIC (not the whole table re-emitted). Which output mode do you need, and what must you add to make it possible?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer (reveal after attempting)**
# MAGIC * **`append`** mode — but it only works on an aggregation **with a watermark**, because Spark can only emit
# MAGIC   a window once it is sure no more data will land in it (i.e. once the watermark passes the window's end).
# MAGIC * Without a watermark, an aggregation supports only `complete` (whole table) or `update` (changed rows) —
# MAGIC   never `append`. That is the practical reason watermarks matter for sinks like Delta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D5 — Watermark & Late Data  *(≈6 min)*
# MAGIC To make the late-data drop visible **every run**, we drive a small **controlled stream** — a Delta table
# MAGIC we append to in **two phases**: three **on-time** rows (event-time = now, which advance the watermark),
# MAGIC then one **8-minute-late** row. The identical `withWatermark` logic applies to the Kafka orders stream (D2).
# MAGIC **Predict:** which rows survive into a window?

# COMMAND ----------

import datetime as dt

# A small CONTROLLED stream so the late-data drop is visible every run: a Delta table we
# append to in two phases. The same withWatermark logic applies to the Kafka orders stream.
src_tbl = f"{CATALOG}.{SCHEMA}.wm_src"
spark.sql(f"DROP TABLE IF EXISTS {src_tbl}")
spark.sql(f"CREATE TABLE {src_tbl} (event_time TIMESTAMP, label STRING)")

def append_events(rows):
    (spark.createDataFrame(rows, "event_time timestamp, label string")
          .write.mode("append").saveAsTable(src_tbl))

base = dt.datetime.now().replace(microsecond=0)

# 'update' mode (NOT 'complete') so the watermark can actually DROP the late row: in complete
# mode Spark keeps every window forever, so a late record still updates its old window.
wm = (spark.readStream.table(src_tbl)
      .withWatermark("event_time", "2 minutes")
      .groupBy(window(col("event_time"), "1 minute"))
      .agg(_count("*").alias("n")))
q = (wm.writeStream.queryName("wm").outputMode("update")
     .format("memory").trigger(processingTime="2 seconds").start())
time.sleep(8)   # let the streaming query start

# Phase 1 — three ON-TIME rows (event-time = now). These advance the watermark to ~now - 2 min.
append_events([(base, "a"), (base, "b"), (base, "c")])
print("phase 1: appended 3 on-time rows at", base)
time.sleep(12)

# Phase 2 — one LATE row (event-time = 8 min ago), arriving in a later batch -> dropped.
append_events([(base - dt.timedelta(minutes=8), "late")])
print("phase 2: appended 1 late row at", base - dt.timedelta(minutes=8))
time.sleep(12)

q.stop()
print("\nwindows that survived the watermark:")
spark.table("wm").orderBy("window").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * You should see **one window with `n = 3`** (the on-time rows). The 8-minute-late row is **dropped** and
# MAGIC   never appears.
# MAGIC * This requires **`update`/`append`** output mode. In **`complete`** mode Spark keeps every window's state
# MAGIC   forever, so the late row would still land in its old window and would *not* be dropped — output mode and
# MAGIC   watermark together decide what "too late" means.
# MAGIC * `withWatermark("event_time", "2 minutes")` is a promise: *"I won't accept data more than 2 minutes behind
# MAGIC   the max event-time I've seen."* Phase 1 (event-time = now) pushes the watermark to ~now − 2 min.
# MAGIC * The phase-2 row (event-time = 8 min ago) arrives **after** the watermark moved past its window, so it is
# MAGIC   dropped. Loosen the watermark to 10 minutes and it would be kept — the **latency vs completeness** trade-off.
# MAGIC * Key subtlety: a late record is dropped only if it arrives in a **later batch** than the one that advanced
# MAGIC   the watermark — which is exactly why we append in two phases. The watermark is also what lets Spark **evict**
# MAGIC   old window state, bounding memory.
# MAGIC * We used a Delta table as the source purely so the drop is deterministic; the same `withWatermark` /
# MAGIC   `window` code runs unchanged on the Kafka `orders` stream.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Challenge 2 — Watermark Threshold  *(≈2 min)*
# MAGIC **Predict:** with `withWatermark("event_time", "5 minutes")` and 1-minute windows, the stream's latest
# MAGIC event-time is 12:00:00. A record arrives with event-time **11:54:30**. Is it counted or dropped? What about **11:58:30**?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer (reveal after attempting)**
# MAGIC * Watermark = max event-time − 5 min = **11:55:00**. A record at **11:54:30** is *older* than the watermark →
# MAGIC   **dropped**. A record at **11:58:30** is newer than the watermark → **counted** in its window.
# MAGIC * The watermark is a moving floor: it only advances as larger event-times arrive, and it is what allows Spark
# MAGIC   to safely throw away state for windows that can no longer change.

# COMMAND ----------

# MAGIC %md
# MAGIC # ──────────────────────────────────────────────
# MAGIC # Building a Pipeline: Bronze → Silver → Gold
# MAGIC # ──────────────────────────────────────────────
# MAGIC The next demos chain into one **medallion pipeline**: raw events land in **Bronze**, are cleaned, deduped
# MAGIC and enriched into **Silver**, then aggregated into a business-ready **Gold** table — each hop a streaming
# MAGIC query writing Delta with its own checkpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D6 — Bronze: Raw Orders to Delta (exactly-once)  *(≈6 min)*
# MAGIC Land the parsed orders in a **Bronze** Delta table with a **checkpoint** — the immutable raw zone. Then stop
# MAGIC and restart to show it **resumes** from the checkpoint without reprocessing.
# MAGIC **Predict:** after a stop + restart, will the row count jump by the whole backlog, or only by what is new?

# COMMAND ----------

bronze = "orders_bronze"
ckpt   = f"{CKPT}/{bronze}"

def start_bronze():
    return (orders_stream("latest")
            .writeStream.outputMode("append").format("delta")
            .option("checkpointLocation", ckpt)
            .trigger(processingTime="5 seconds")
            .toTable(f"{CATALOG}.{SCHEMA}.{bronze}"))

q = start_bronze()
time.sleep(18)
q.stop()
n1 = spark.table(bronze).count()
print("rows in Delta after first run:", n1)
spark.table(bronze).show(5, truncate=False)

# COMMAND ----------

# Restart the SAME query (same checkpoint) — it resumes from the last committed Kafka offset.
q2 = start_bronze()
time.sleep(14)
q2.stop()
n2 = spark.table(bronze).count()
print(f"rows after restart: {n2}  (+{n2 - n1} new, no reprocessing of the first {n1})")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * The **checkpoint** stores the Kafka offsets and any state per `batchId`. On restart Spark reads the
# MAGIC   checkpoint and **resumes from the last committed offset** — so the count grows only by *new* records.
# MAGIC * **Exactly-once** comes from the pairing: the checkpoint remembers what was processed, and Delta's commit
# MAGIC   is **atomic and idempotent per batch** (a retried batch overwrites, never duplicates). Kafka offsets +
# MAGIC   Delta transaction = end-to-end exactly-once into the lakehouse.
# MAGIC * **Pitfall:** never point two *different* queries at one `checkpointLocation`, and don't delete it to "fix"
# MAGIC   a job — you lose offset/state history and risk reprocessing or loss.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Challenge 3 — Checkpoint Reuse  *(≈2 min)*
# MAGIC **Predict:** a teammate copies your streaming job, changes the aggregation, but forgets to change the
# MAGIC `checkpointLocation`. They run it alongside yours. What happens, and why?

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer (reveal after attempting)**
# MAGIC * A `checkpointLocation` belongs to **one** query. Two queries sharing it will conflict on the offset/state
# MAGIC   files — you get errors, corrupted progress, or one job silently stealing the other's offsets.
# MAGIC * Each streaming query needs its **own** checkpoint path. Changing the query logic but keeping the old
# MAGIC   checkpoint can also fail, because the stored state no longer matches the new plan.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D7 — Silver: Dedup, Enrich & MERGE-Upsert  *(≈7 min)*
# MAGIC Silver is the cleaned, business-ready layer. We read the **Bronze** table as a stream, drop duplicate
# MAGIC orders within a watermark, enrich with item category (**stream–static join**), and **MERGE-upsert** into a
# MAGIC Silver table keyed by `orderid` — the production write pattern via **`foreachBatch`**.
# MAGIC **Predict:** why upsert (MERGE) instead of a plain append?

# COMMAND ----------

# item -> category lookup (static dimension); cover the full Datagen item range so none go NULL
cats = ["Electronics", "Grocery", "Apparel"]
items = spark.createDataFrame([(f"Item_{i}", cats[i % 3]) for i in range(1, 1001)],
                              ["itemid", "category"])

silver = f"{CATALOG}.{SCHEMA}.orders_silver"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {silver}
              (orderid BIGINT, itemid STRING, category STRING, orderunits DOUBLE, event_time TIMESTAMP)""")

# read Bronze as a stream -> dedup within watermark -> enrich -> keep silver columns
clean = (spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_bronze")
         .withWatermark("event_time", "2 minutes")
         .dropDuplicatesWithinWatermark(["orderid"])           # exactly-once content dedup
         .join(items, "itemid", "left")                        # stream-static enrichment
         .select("orderid", "itemid", "category", "orderunits", "event_time"))

def upsert_to_silver(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {silver} t USING updates s ON t.orderid = s.orderid
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")

q = (clean.writeStream.foreachBatch(upsert_to_silver)
     .option("checkpointLocation", f"{CKPT}/silver")
     .trigger(processingTime="5 seconds").start())
time.sleep(25)
q.stop()
print("silver rows:", spark.table(silver).count())
spark.table(silver).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * **Why MERGE, not append:** Silver is keyed (one row per `orderid`). A correction or a reprocessed batch must
# MAGIC   **update** the existing row, not add a duplicate. `MERGE … WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT`
# MAGIC   makes the write **idempotent** — re-running the same batch is a no-op.
# MAGIC * **`foreachBatch`** hands each micro-batch to you as a normal **batch DataFrame**, so you can run *any* batch
# MAGIC   op — here a Delta `MERGE`. It is the most useful streaming sink for pipelines (upserts, SCD, multi-table writes).
# MAGIC * **`dropDuplicatesWithinWatermark(["orderid"])`** removes repeats seen within the watermark horizon — dedup
# MAGIC   that stays **bounded** in memory (plain `dropDuplicates` would track keys forever).
# MAGIC * **Stream–static join** enriches each event against the static `items` lookup (re-read each batch, **no
# MAGIC   watermark** needed). A **stream–stream** join instead needs a watermark on *both* sides to bound state.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D8 — Gold: Business Aggregates from Silver  *(≈4 min)*
# MAGIC Gold is what dashboards read. We read **Silver** as a stream and aggregate revenue by category into a Gold table.
# MAGIC **Predict:** which layer should a BI tool query — Bronze, Silver, or Gold?

# COMMAND ----------

gold = f"{CATALOG}.{SCHEMA}.revenue_by_category"
q = (spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_silver")
     .groupBy("category")
     .agg(_sum("orderunits").alias("revenue"), _count("*").alias("orders"))
     .writeStream.outputMode("complete").format("delta")
     .option("checkpointLocation", f"{CKPT}/gold")
     .trigger(processingTime="5 seconds")
     .toTable(gold))
time.sleep(20)
q.stop()
spark.table(gold).orderBy(col("revenue").desc()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * Gold reads **Silver as a stream** (`readStream.table`): layers are chained by streaming *between Delta
# MAGIC   tables*, each with its own checkpoint. Source → Bronze → Silver → Gold is the medallion pipeline.
# MAGIC * BI tools query **Gold** (small, clean, aggregated); **Silver** serves ad-hoc/joins; **Bronze** is the
# MAGIC   immutable raw landing zone for replay and audit.
# MAGIC * In production you run these hops as scheduled **`availableNow`** jobs (D3b) or as a **Lakeflow Declarative
# MAGIC   Pipeline / Delta Live Tables** — the packaging & deploy story is M3/M4.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART B — PARTICIPANT LAB + DESIGN EXERCISE
# MAGIC # ════════════════════════════════════════════════
# MAGIC You reuse `orders_stream`, `run_to_memory`, `CATALOG`/`SCHEMA` and `CKPT` from the setup cell. Give your
# MAGIC pair a unique name so tables/checkpoints don't collide.

# COMMAND ----------

PAIR = "team1"                      # TODO: change to your team name
print("Lab tables will be prefixed with:", PAIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAB 1 — Rolling Revenue Window with a Watermark  *(60 min)*
# MAGIC **Goal:** build a windowed revenue aggregation that is **bounded** by a watermark, then prove a late
# MAGIC record is handled per your threshold.
# MAGIC **Success criteria:** (1) revenue grouped into 5-minute event-time windows; (2) a watermark caps state;
# MAGIC (3) you can show an over-threshold late record being dropped.

# COMMAND ----------

# L1.1 — Build the windowed, watermarked revenue stream.
src = orders_stream("latest")
# TODO: add a watermark on event_time, then group by a tumbling window and aggregate
#       sum(orderunits) AS revenue and count(*) AS orders.
#   Use a SHORT window (e.g. 10 seconds) so append-mode windows finalize during the run;
#   in production you'd use minutes — only the duration string changes.
#   hint:
#   lab_rev = (src.withWatermark("event_time", "10 seconds")
#                 .groupBy(window(col("event_time"), "10 seconds"))
#                 .agg(_sum("orderunits").alias("revenue"), _count("*").alias("orders")))
lab_rev = None   # <- replace with your streaming aggregation

# COMMAND ----------

# L1.2 — Run it and look at the windows (append mode is now allowed because you added a watermark).
result = run_to_memory(lab_rev, f"lab_rev_{PAIR}", mode="append", run_s=40)
result.orderBy("window").show(truncate=False)
# TODO: in one sentence, explain why 'append' works here but did not in D4.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design Exercise — Convert a Daily Batch Pipeline to Near-Real-Time  *(30 min)*
# MAGIC A nightly batch job reads yesterday's orders from object storage, aggregates revenue by item, and overwrites
# MAGIC a BI table. Redesign it as a **safe** streaming pipeline. Fill in the decision table below (text only) and
# MAGIC note the **risks** you are mitigating. Defend each choice — there is no single right answer.

# COMMAND ----------

# Design decision template — edit the right-hand values and add a one-line justification each.
design = {
    "source":            "TODO  (e.g. Kafka topic_0)",
    "trigger":           "TODO  (processingTime? availableNow? why)",
    "event_time":        "TODO  (which timestamp, and why)",
    "watermark":         "TODO  (threshold + the latency/completeness trade-off)",
    "window":            "TODO  (size; tumbling vs sliding)",
    "stateful_op":       "TODO  (the aggregation; how state is bounded)",
    "sink":              "TODO  (Delta table; output mode)",
    "exactly_once":      "TODO  (checkpoint + idempotent sink)",
    "recovery":          "TODO  (what a restart does; checkpoint ownership)",
    "sla":               "TODO  (target end-to-end latency; how you'd measure lag)",
    "top_risks":         "TODO  (e.g. unbounded state, late data dropped silently, checkpoint reuse)",
}
for k, v in design.items():
    print(f"{k:14s}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # Cleanup — run between cohorts
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# Stop any lingering streaming queries from this notebook.
for q in spark.streams.active:
    print("stopping", q.name)
    q.stop()

# Optional: drop demo tables and checkpoints (skip during class).
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.orders_bronze")
# dbutils.fs.rm(CKPT, recurse=True)
print("done")
