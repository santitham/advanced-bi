# Databricks notebook source
# MAGIC %md
# MAGIC # Module 5 — Data Observability & Metric Consistency
# MAGIC ## Demo + Lab Notebook  *(tutorial pace — we build up one step at a time)*
# MAGIC
# MAGIC We work on the Unity Catalog tables created by **`M5 — UC Dataset Setup`** (no Kafka — the data was generated).
# MAGIC The story is one incident: **revenue dropped ~30% overnight — is it real, or a data bug?** We'll learn the
# MAGIC tools to answer that.
# MAGIC
# MAGIC * **Part 0 — Warm-up.** Get oriented with the tables.
# MAGIC * **Part A — Data-quality dimensions.** Measure trust, one dimension at a time (then the DQX tool).
# MAGIC * **Part B — Schema drift & data contracts.**
# MAGIC * **Part C — Metric anomalies & monitoring** (by hand first, then Databricks Data Quality Monitoring).
# MAGIC * **Part D — Metric consistency & the semantic layer** (Unity Catalog Metric Views).
# MAGIC * **Part E — Lineage & root-cause** (find the cause of the drop).
# MAGIC * **Part F — Lab** (you repeat the steps) and **Part G — the incident debate.**
# MAGIC
# MAGIC Every code cell is followed by a **"What just happened"** cell — read it; that's the learning.
# MAGIC **New to these tools?** Good — each one is introduced in plain language before we use it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — install the tools, point at the dataset, then go top to bottom
# MAGIC We install **DQX** and upgrade the **SDK** (needed for Data Quality Monitoring on this runtime). This cell
# MAGIC restarts Python, so run it **first**, then continue below.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx databricks-sdk --upgrade -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Use the SHARED schema for the lecture demos; in the lab you'll switch to your own.
CATALOG = "ctl_training_dev"
SCHEMA  = "m5_obs_shared"          # lab: change to your personal schema, e.g. m5_obs_<you>
FQ = f"`{CATALOG}`.`{SCHEMA}`"
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
print("reading from:", FQ)
display(spark.sql(f"SHOW TABLES IN {FQ}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART 0 — WARM-UP: GET ORIENTED
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.1 — How data is addressed in Unity Catalog
# MAGIC Every table has a **three-level name**: `catalog.schema.table` (e.g. `ctl_training_dev.m5_obs_shared.orders_bronze`).
# MAGIC We set the catalog + schema above, so we can now use short names like `orders_bronze`.

# COMMAND ----------

# Look at the raw orders — the source data for everything else.
display(spark.sql("SELECT * FROM orders_bronze LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * `orders_bronze` is our **raw** table: one row per order — `order_id`, `item_id`, `order_units`, `order_ts`, `ingest_date`.
# MAGIC * "Bronze" = as-ingested, **not yet cleaned**. Look closely and you'll already spot suspicious values — that's the point.

# COMMAND ----------

# What columns and types does the table have? DESCRIBE shows the schema.
display(spark.sql("DESCRIBE orders_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * `DESCRIBE` prints the **schema** — the contract of column names and types. We'll come back to this in Part B.
# MAGIC * `order_id` is a `bigint`, `order_ts` a `timestamp`, `ingest_date` a `date`. Knowing the types is the first step to checking them.

# COMMAND ----------

# The "normal" business picture: total revenue per day for the last two weeks.
display(spark.sql("""
  SELECT event_date, revenue
  FROM revenue_daily
  ORDER BY event_date DESC
  LIMIT 14
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * `revenue_daily` is the **gold** metric the business watches. Most days look similar…
# MAGIC * …but one recent day is much lower. **The job that built this table succeeded** — no error anywhere.
# MAGIC * So here's the whole module in one question: *how would we know this number is wrong, and find out why?* That is **data observability**.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART A — DATA-QUALITY DIMENSIONS
# MAGIC # ════════════════════════════════════════════════
# MAGIC **Data quality** isn't one number — it's several **dimensions**. We'll measure them one at a time on
# MAGIC `orders_bronze`, starting with a fully worked example, then reuse the same pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.1 — Completeness (worked example)
# MAGIC **Completeness** = are required values present? Missing IDs break joins and counts. We'll measure it step by step.

# COMMAND ----------

# Step 1: how many rows in total?
total = spark.sql("SELECT COUNT(*) AS n FROM orders_bronze").first()["n"]
print("total rows:", total)

# COMMAND ----------

# Step 2: how many have a NULL order_id or item_id (the keys we depend on)?
nulls = spark.sql("""
  SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN item_id  IS NULL THEN 1 ELSE 0 END) AS null_item_id
  FROM orders_bronze
""")
display(nulls)

# COMMAND ----------

# Step 3: turn the counts into a completeness % for order_id.
row = nulls.first()
completeness = round(100 * (total - row["null_order_id"]) / total, 2)
print(f"order_id completeness: {completeness}%   (nulls: {row['null_order_id']} of {total})")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * We built a quality metric from scratch: **count rows → count nulls → turn into a %**.
# MAGIC * A `NULL order_id` means an order we can't identify or join. Completeness puts a *number* on "how bad."
# MAGIC * This three-step pattern (total → violations → %) is exactly how we'll measure every other dimension.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.2 — Validity
# MAGIC **Validity** = do values obey the rules? Order units can't be negative. Same pattern: count the violations.

# COMMAND ----------

display(spark.sql("""
  SELECT COUNT(*) AS total,
         SUM(CASE WHEN order_units < 0 THEN 1 ELSE 0 END) AS invalid_units
  FROM orders_bronze
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * A negative `order_units` is **valid as a number but invalid as a business fact** — it would distort revenue.
# MAGIC * This is why the gold table filters `order_units >= 0`. Validity rules encode the business meaning.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.3 — Uniqueness
# MAGIC **Uniqueness** = is each entity represented once? Duplicate orders double-count revenue.

# COMMAND ----------

display(spark.sql("""
  SELECT COUNT(*) AS duplicate_groups
  FROM (SELECT order_id FROM orders_bronze
        WHERE order_id IS NOT NULL
        GROUP BY order_id HAVING COUNT(*) > 1)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * We grouped by `order_id` and kept groups appearing more than once — those are **duplicates**.
# MAGIC * Duplicates silently inflate sums (revenue, order counts). Uniqueness checks catch double-counting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.4 — Freshness
# MAGIC **Freshness** = how recent is the data? A stale table means dashboards show yesterday's world.

# COMMAND ----------

display(spark.sql("""
  SELECT MAX(ingest_date) AS latest_load,
         CURRENT_DATE() AS today,
         DATEDIFF(CURRENT_DATE(), MAX(ingest_date)) AS days_behind
  FROM orders_bronze
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * Freshness compares the **latest data timestamp** to **now**. `days_behind` is the lag.
# MAGIC * A freshness SLA ("loaded within 1 day") turns this into a pass/fail signal — the basis of automated monitoring later.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.5 — Put it together: a data-quality scorecard
# MAGIC Real teams track all dimensions in one place. Here's a tiny scorecard with thresholds.

# COMMAND ----------

from pyspark.sql import functions as F

m = spark.sql("""
  SELECT
    COUNT(*)                                                  AS total_rows,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)         AS null_order_id,
    SUM(CASE WHEN order_units < 0 THEN 1 ELSE 0 END)          AS negative_units,
    DATEDIFF(CURRENT_DATE(), MAX(ingest_date))                AS days_behind
  FROM orders_bronze
""").first()

scorecard = [
    ("completeness (order_id)", round(100*(m.total_rows-m.null_order_id)/m.total_rows,2), ">= 99%",  m.null_order_id == 0 or 100*(m.total_rows-m.null_order_id)/m.total_rows >= 99),
    ("validity (units >= 0)",   m.negative_units,                                         "== 0",    m.negative_units == 0),
    ("freshness (days behind)", m.days_behind,                                            "<= 1 day",m.days_behind <= 1),
]
print(f"{'dimension':28} {'value':>10}   {'threshold':10} result")
for name, val, thr, ok in scorecard:
    print(f"{name:28} {str(val):>10}   {thr:10} {'PASS' if ok else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * One pass over the table produces a **scorecard**: value + threshold + PASS/FAIL per dimension.
# MAGIC * This is the core of every data-quality tool. Next we see the tool that does this for you at scale.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.6 — The tool: Databricks Labs DQX  *(introduce, then use)*
# MAGIC **DQX** is a framework that lets you declare quality **rules** once and apply them to split **good** rows from a
# MAGIC **quarantine** set — instead of hand-writing every check. It is the productionised version of what we just did.
# MAGIC The cell is guarded: if DQX isn't installed, we fall back to the same idea in plain Spark so the demo still runs.

# COMMAND ----------

def quarantine_split(df):
    """Hand-rolled equivalent of a DQX rule set: good rows vs quarantined rows."""
    good_cond = "order_id IS NOT NULL AND item_id IS NOT NULL AND order_units >= 0"
    return df.where(good_cond), df.where(f"NOT ({good_cond})")

# Real DQX rules — run if databricks-labs-dqx is installed (see "M5 — Enable Native Tools").
dqx_checks = [
    {"name": "order_id_not_null", "criticality": "error",
     "check": {"function": "is_not_null", "arguments": {"column": "order_id"}}},
    {"name": "item_id_not_null",  "criticality": "error",
     "check": {"function": "is_not_null", "arguments": {"column": "item_id"}}},
]
try:
    from databricks.labs.dqx.engine import DQEngine
    from databricks.sdk import WorkspaceClient
    dq = DQEngine(WorkspaceClient())
    good, bad = dq.apply_checks_by_metadata_and_split(spark.table("orders_bronze"), dqx_checks)
    print("DQX applied its rules and split the data (not-null keys; validity handled in A.2):")
except Exception as e:
    print(f"DQX not usable here ({type(e).__name__}) — using the plain-Spark equivalent (same idea).")
    good, bad = quarantine_split(spark.table("orders_bronze"))

print(f"GOOD rows kept:   {good.count():>6}")
print(f"QUARANTINED rows: {bad.count():>6}   (set aside + alerted, not silently dropped)")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * A quality tool **splits**, it doesn't just count: good rows flow on; bad rows are **quarantined** for review.
# MAGIC * `revenue_fact` was built from the GOOD rows only — that's why the gold number is trustworthy *as far as quality goes*.
# MAGIC * But quality being fine doesn't mean the **metric** is right. That's the next problem.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART B — SCHEMA DRIFT & DATA CONTRACTS
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.1 — What is a schema, and how do we read it?
# MAGIC The **schema** is the set of column names + types. Code downstream depends on it. We can grab it programmatically.

# COMMAND ----------

current_cols = {f.name: f.dataType.simpleString() for f in spark.table("orders_bronze").schema.fields}
print("current orders_bronze schema:")
for c, t in current_cols.items():
    print(f"  {c:14} {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * We read the live schema into a dictionary `{column: type}`. Now we can *compare* it to what we expect.
# MAGIC * **Schema drift** is when this set changes unexpectedly — a renamed, removed, added, or retyped column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.2 — Detecting drift
# MAGIC Imagine an upstream team renames `order_units` to `qty`. Let's simulate that schema and detect the change.

# COMMAND ----------

drifted = {c: t for c, t in current_cols.items()}
drifted["qty"] = drifted.pop("order_units")        # the rename

added   = set(drifted) - set(current_cols)
removed = set(current_cols) - set(drifted)
print("added columns  :", added or "none")
print("removed columns:", removed or "none")
print("=> a rename shows up as one removed + one added column — a breaking change for anything reading 'order_units'.")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * Comparing two schemas localizes drift precisely: `order_units` **removed**, `qty` **added**.
# MAGIC * This is the M3 incident (`revenue → amount`) seen from the data side: the code may run, but the **contract** broke.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.3 — A data contract
# MAGIC A **data contract** writes the expectation down: required columns, types, and quality rules a producer promises a
# MAGIC consumer. We validate the live table against it — if reality and contract disagree, that's a failed build.

# COMMAND ----------

CONTRACT = {
    "required_columns": {
        "order_id": "bigint", "item_id": "string", "order_units": "double",
        "order_ts": "timestamp", "ingest_date": "date",
    },
    "rules": {"order_units": ">= 0", "freshness_days": "<= 1"},
}

def validate_contract(table, contract):
    cols = {f.name: f.dataType.simpleString() for f in spark.table(table).schema.fields}
    problems = []
    for c, t in contract["required_columns"].items():
        if c not in cols:        problems.append(f"missing column '{c}'")
        elif cols[c] != t:       problems.append(f"'{c}' type {cols[c]} != expected {t}")
    return problems

issues = validate_contract("orders_bronze", CONTRACT)
print("CONTRACT CHECK:", "PASS — schema matches" if not issues else "FAIL")
for p in issues:
    print("  -", p)

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * The contract is just **expectations as data**. Validation = compare live schema to the contract.
# MAGIC * Put this in CI (Module 3) and in production monitoring (here), and a breaking schema change becomes a caught,
# MAGIC   explainable failure instead of a 2 a.m. dashboard outage.
# MAGIC * **Watch out:** Delta's `mergeSchema` option will happily *evolve* a table's schema on write — convenient, but it's
# MAGIC   how silent drift sneaks in. A contract check is your guardrail.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART C — METRIC ANOMALIES & MONITORING
# MAGIC # ════════════════════════════════════════════════
# MAGIC Row-level quality can be perfect and the **metric** still be wrong. Now we watch the *number over time*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.1 — See it by hand first
# MAGIC Let's pull the daily revenue series and just look at it. (We detect, then automate.)

# COMMAND ----------

import pandas as pd
pdf = spark.table("revenue_daily").orderBy("event_date").toPandas()
display(spark.table("revenue_daily").orderBy("event_date"))   # use the chart toggle: line, x=event_date, y=revenue

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * Toggle the result to a **line chart** (x = `event_date`, y = `revenue`). One recent day clearly dips.
# MAGIC * Eyeballing works for one chart — but you can't watch thousands of metrics by eye. We need a *rule*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.2 — Turn "looks low" into a rule: a rolling baseline
# MAGIC Compare each day to the median of the **previous 7 days**; flag a day that falls more than 20% below it.

# COMMAND ----------

pdf = pdf.sort_values("event_date").reset_index(drop=True)
pdf["baseline"] = pdf["revenue"].shift(1).rolling(7, min_periods=3).median()
pdf["pct_vs_baseline"] = (100 * (pdf["revenue"] / pdf["baseline"] - 1)).round(1)
pdf["anomaly"] = pdf["pct_vs_baseline"] < -20
display(pdf.tail(10)[["event_date", "revenue", "baseline", "pct_vs_baseline", "anomaly"]])

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * `baseline` = median of the prior 7 days (robust to noise). `pct_vs_baseline` is how far today sits from normal.
# MAGIC * The incident day trips the rule (**anomaly = True**, ~30% below). We turned "looks wrong" into a **reproducible signal**.
# MAGIC * A z-score (deviations from the rolling mean) is the same idea with statistics instead of a fixed %.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.3 — The tool: Databricks Data Quality Monitoring  *(set-up shown; results need serverless network access)*
# MAGIC Databricks **Data Quality Monitoring** (formerly Lakehouse Monitoring) automates C.2: attach a monitor to a table and
# MAGIC it builds **profile** + **drift** metric tables over time. We **register** the monitor here (that works). The
# MAGIC **refresh runs on Databricks-managed serverless**, which on this locked-down workspace can't reach the storage
# MAGIC (Private Link / firewall), so the metric tables won't populate — the **by-hand baseline (C.2) is our runnable detection**.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorTimeSeries

w    = WorkspaceClient()
MON  = f"{CATALOG}.{SCHEMA}.revenue_fact"
user = spark.sql("SELECT current_user()").first()[0]

# 1) Register the monitor — this part works on this workspace.
try:
    info = w.quality_monitors.get(table_name=MON)                       # reuse if it already exists
    print("monitor already exists on", MON)
except Exception:
    info = w.quality_monitors.create(                                   # else create it
        table_name=MON,
        assets_dir=f"/Workspace/Users/{user}/databricks_lakehouse_monitoring/{MON}",
        output_schema_name=f"{CATALOG}.{SCHEMA}",
        time_series=MonitorTimeSeries(timestamp_col="event_date", granularities=["1 day"]),
    )
    print("monitor created on", MON)
print("profile metrics table:", info.profile_metrics_table_name)
print("drift metrics table  :", info.drift_metrics_table_name)

# 2) Refresh computes the metrics ON SERVERLESS. Here that fails (AZURE_AUTHENTICATION_FAILURE) because
#    serverless can't reach storage behind Private Link/firewall — expected on this workspace.
try:
    run = w.quality_monitors.run_refresh(table_name=MON)
    print("refresh started:", run.refresh_id, "- (in an unrestricted workspace this populates the tables)")
except Exception as e:
    print("refresh can't run here:", type(e).__name__, str(e)[:140])
print("\nNote: even if the refresh starts, the serverless job may fail with AZURE_AUTHENTICATION_FAILURE on this")
print("network-restricted workspace, so the metric tables stay empty. Our runnable detection is the C.2 baseline.")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * `quality_monitors.create` registered a **TimeSeries** monitor on `revenue_fact` (event_date, daily) — the setup is real.
# MAGIC * The **refresh** is a serverless job; on this Private-Link/firewalled workspace it can't reach storage, so the
# MAGIC   profile/drift tables don't populate. In an open workspace this is the managed version of C.2.
# MAGIC * **Takeaway:** you've seen how to *turn on* native monitoring; our **runnable** anomaly detection is the C.2 baseline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.4 — What the monitor *would* show
# MAGIC If the refresh could run, the **profile-metrics table** holds one row per day per column (the stats), and the
# MAGIC **drift-metrics table** flags day-over-day change — exactly our by-hand baseline, managed. The cell below tries to
# MAGIC read it; on this workspace it's empty (refresh blocked), so we point back to C.2.

# COMMAND ----------

try:
    df = spark.sql(f"SELECT * FROM {info.profile_metrics_table_name} ORDER BY window.start DESC LIMIT 20")
    if df.count() == 0:
        print("profile-metrics table exists but is empty — the serverless refresh couldn't run on this workspace.")
        print("Our runnable anomaly detection is the C.2 rolling baseline.")
    else:
        display(df)
except Exception as e:
    print("profile metrics unavailable here:", type(e).__name__, str(e)[:120])
    print("Expected on this network-restricted workspace — use the C.2 baseline as the runnable detection.")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * On an open workspace this table is the automated equivalent of our scorecard + baseline, ready to **alert** on (Module 6).
# MAGIC * Here it's empty because serverless can't reach storage — so the **C.2 baseline remains our live detection**. Same concept, two delivery options.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART D — METRIC CONSISTENCY & THE SEMANTIC LAYER
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## D.1 — The problem: two reports, two numbers
# MAGIC Two analysts both compute "total revenue" — but define it differently. Watch them disagree.

# COMMAND ----------

report_a = spark.sql(f"""
  SELECT ROUND(SUM(order_units) * 9.99, 2) AS revenue   -- Analyst A: straight from RAW orders
  FROM orders_bronze
""").first()["revenue"]

report_b = spark.sql("SELECT ROUND(SUM(revenue), 2) AS revenue FROM revenue_fact").first()["revenue"]  # Analyst B: from gold

print(f"Report A (raw orders_bronze):  {report_a:,.2f}")
print(f"Report B (clean revenue_fact): {report_b:,.2f}")
print(f"difference: {abs(report_a - report_b):,.2f}  -> two 'revenue' numbers in the same company")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * Same word, two definitions: A includes negative/unjoined rows; B uses the cleaned gold. Neither is "lying" — they
# MAGIC   just **defined the metric differently**. This is how dashboards end up disagreeing and trust erodes.
# MAGIC * The fix isn't "argue which query is right" — it's to define the metric **once**, where everyone uses the same one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D.2 — The fix: one shared definition (a metric view)
# MAGIC The cure for "two numbers" is to define the metric **once**, where everyone reads it. Databricks' native tool is a
# MAGIC **Unity Catalog Metric View**: you declare *measures* (e.g. `total_revenue = SUM(revenue)`) separately from
# MAGIC *dimensions* (date, category) in YAML, and everyone queries the same definition with `MEASURE()`:
# MAGIC ```
# MAGIC CREATE VIEW revenue_metrics WITH METRICS LANGUAGE YAML AS $$
# MAGIC   source: ...revenue_fact
# MAGIC   dimensions: [ event_date, category ]
# MAGIC   measures:   [ total_revenue = SUM(revenue) ]
# MAGIC $$;
# MAGIC SELECT category, MEASURE(total_revenue) FROM revenue_metrics GROUP BY category;
# MAGIC ```
# MAGIC Metric views need **DBR 16.4+** (this cluster is 15.4), so we encode the *same single definition* as a plain view.
# MAGIC The concept is identical: **one place, one definition, everyone agrees.**

# COMMAND ----------

# One agreed definition of the revenue metric, in one place.
spark.sql(f"""
  CREATE OR REPLACE VIEW {FQ}.revenue_metrics AS
  SELECT event_date, category, revenue AS total_revenue, order_count AS total_orders
  FROM {FQ}.revenue_fact
""")
print("created revenue_metrics — the single source of truth for the revenue metric.")

# COMMAND ----------

# Now BOTH the total and the by-category breakdown read the SAME definition -> they reconcile by construction.
print("Total revenue (one definition):")
display(spark.sql("SELECT ROUND(SUM(total_revenue), 2) AS revenue FROM revenue_metrics"))
print("Same definition, sliced by category:")
display(spark.sql("SELECT category, ROUND(SUM(total_revenue), 2) AS revenue FROM revenue_metrics GROUP BY category ORDER BY category"))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * The total and the by-category breakdown come from **one definition** — they reconcile by construction.
# MAGIC * Define `total_revenue` once in the metric view; every dashboard, notebook, and BI tool gets the same number.
# MAGIC * **Data contract** (Part B) governs the *inputs*; the **semantic layer** governs the *outputs*. Together = trustworthy metrics.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART E — LINEAGE & ROOT-CAUSE
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## E.1 — What is lineage?
# MAGIC **Lineage** is the automatic map of how data flows: which tables (and columns) feed which. Unity Catalog records it
# MAGIC whenever a query reads one table to build another — no setup. **In the UI:** open `revenue_daily` in *Catalog Explorer*
# MAGIC → the **Lineage** tab → *Upstream* shows `revenue_fact`, then `orders_bronze` + `items`. Let's also query it.

# COMMAND ----------

# Programmatic lineage via system tables (guarded: needs system.access enabled + access).
try:
    up = spark.sql(f"""
      SELECT DISTINCT source_table_full_name
      FROM system.access.table_lineage
      WHERE target_table_full_name = '{CATALOG}.{SCHEMA}.revenue_daily'
        AND source_table_full_name IS NOT NULL
    """)
    print("Upstream of revenue_daily (from system.access.table_lineage):")
    display(up)
except Exception as e:
    print("lineage system tables not available here:", type(e).__name__, str(e)[:120])
    print("Use the Catalog Explorer → Lineage tab instead (always available in the UI).")
    print("Known chain: orders_bronze + items -> revenue_fact -> revenue_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * Lineage answers "**where did this number come from?**" Following it upstream from `revenue_daily` points us at
# MAGIC   `revenue_fact`, and then at the raw source, `orders_bronze`. That tells us *where to look* for the drop's cause.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E.2 — Follow lineage to the cause
# MAGIC Lineage says the drop must originate upstream. Let's check the **volume** of `orders_bronze` per day and per
# MAGIC category around the incident — completeness at the *source*.

# COMMAND ----------

display(spark.sql("""
  SELECT o.ingest_date, i.category, COUNT(*) AS orders
  FROM orders_bronze o JOIN items i ON o.item_id = i.item_id
  WHERE o.ingest_date >= DATE_SUB(CURRENT_DATE(), 4)
  GROUP BY o.ingest_date, i.category
  ORDER BY o.ingest_date, i.category
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * On the incident day, **category A has (almost) no orders** while B/C/D look normal — a *source volume* problem.
# MAGIC * Root cause found: an upstream **category-A feed failure**, not a code bug. The revenue drop is **real in the data,
# MAGIC   caused by missing inputs.** The incident loop: **detect** (anomaly) → **localize** (lineage) → **diagnose** (volume by source) → fix & communicate.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART F — LAB  (you build it: fill in each # TODO)
# MAGIC # ════════════════════════════════════════════════
# MAGIC Switch `SCHEMA` at the top to your **personal** schema first (e.g. `m5_obs_<you>`), then complete these.

# COMMAND ----------

# L1 — Completeness for item_id. Compute its completeness % using the total -> nulls -> % pattern from A.1.
# TODO:
#   t = spark.sql("SELECT COUNT(*) n, SUM(CASE WHEN item_id IS NULL THEN 1 ELSE 0 END) nulls FROM orders_bronze").first()
#   pct = round(100 * (t.n - t.nulls) / t.n, 2)
#   print("item_id completeness:", pct, "%")
pass

# COMMAND ----------

# L2 — A freshness rule. Print PASS if orders_bronze is <= 1 day behind, else FAIL.
# TODO:
#   days = spark.sql("SELECT DATEDIFF(CURRENT_DATE(), MAX(ingest_date)) d FROM orders_bronze").first().d
#   print("freshness:", "PASS" if days <= 1 else "FAIL", f"({days} days behind)")
pass

# COMMAND ----------

# L3 — Metric consistency. Query revenue_metrics two ways and confirm the totals reconcile.
# TODO: select the total, then the sum of the by-category breakdown; they should match.
pass

# COMMAND ----------

# L4 — Root-cause practice. Using the per-day, per-category volume query from E.2, identify which
#      category and which date caused the drop, and write one sentence stating the cause.
# TODO:
pass

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART G — INCIDENT DEBATE
# MAGIC # ════════════════════════════════════════════════
# MAGIC ## "Revenue dropped 30% overnight — real, or a data bug?"  *(45 min)*
# MAGIC In pairs, build the case with evidence from this notebook:
# MAGIC * **Detect:** show the anomaly (C.2) — how far below baseline, on which date?
# MAGIC * **Rule out a quality bug:** are the DQ dimensions (Part A) healthy on the incident day, or not?
# MAGIC * **Localize:** use lineage (E.1) to say *where* to look.
# MAGIC * **Diagnose:** use the per-source volume (E.2) to name the cause.
# MAGIC * **Verdict:** real or artifact? **Deliverable:** a 3-line incident note (impact → cause → the guard that would
# MAGIC   have caught it first: which DQ check or monitor, and where it runs).

# COMMAND ----------

# MAGIC %md
# MAGIC # Wrap-up
# MAGIC * Quality has **dimensions** (completeness, validity, uniqueness, freshness) — measure them, score them, quarantine bad rows.
# MAGIC * **Contracts** guard inputs (schema + rules); **metric views** guard outputs (one definition); **monitoring** watches metrics over time.
# MAGIC * **Lineage** turns "the number looks wrong" into "here's exactly which upstream table caused it."
# MAGIC * Next: **Module 6 — Platform Observability & Failure Analysis** (metrics/logs/traces, golden signals, alerting, root-cause on *jobs*).