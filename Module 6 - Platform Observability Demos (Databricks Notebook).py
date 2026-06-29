# Databricks notebook source
# MAGIC %md
# MAGIC # Module 6 — Platform Observability & Failure Analysis
# MAGIC ## Demo + Lab Notebook  *(tutorial pace — concept, then measure, then fix)*
# MAGIC
# MAGIC M5 asked "is the **data** right?" — M6 asks "is the **system** healthy, and when a job is slow or fails, **why**?"
# MAGIC
# MAGIC We use the skewed dataset from **`M6 — Skewed Dataset Setup`**. The story: a join is **slow**; we read the **Spark UI**,
# MAGIC find the cause (**data skew → one giant task → spill**), and fix it three ways — then compare the times.
# MAGIC
# MAGIC * **Part A — Reproduce the slow job** (and learn to read the Spark UI).
# MAGIC * **Part B — Diagnose** (skew, spill, shuffle — with the numbers to look at).
# MAGIC * **Part C — Fix it** (broadcast · AQE skew-join · salting) and re-time.
# MAGIC * **Part D — Metrics at scale** (Jobs UI + system tables; admin-gated → walkthrough).
# MAGIC * **Part E — Lab** and **Part F — the root-cause playbook.**
# MAGIC
# MAGIC **The Spark UI is the star.** Open it from **Compute → your cluster → Spark UI** (or the "Spark UI" link under a cell's
# MAGIC job). Each timed cell below is something you then *go look at* in that UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — point at the dataset

# COMMAND ----------

import time
from pyspark.sql import functions as F

CATALOG = "ctl_training_dev"
SCHEMA  = "m6_obs_shared"          # lab: switch to your personal schema, e.g. m6_obs_<you>
FQ = f"`{CATALOG}`.`{SCHEMA}`"
spark.sql(f"USE CATALOG `{CATALOG}`"); spark.sql(f"USE SCHEMA `{SCHEMA}`")

events = spark.table("events")
users  = spark.table("users")
print("events:", f"{events.count():,}", "rows |  users:", users.count(), "rows")

def run_timed(label, df):
    """Trigger the full job with an action and report wall-clock seconds."""
    t = time.time()
    n = df.count()
    dt = time.time() - t
    print(f"{label:36}  ->  {dt:6.1f} s   ({n} result rows)")
    return dt

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART A — REPRODUCE THE SLOW JOB
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.1 — Turn OFF the autopilot so we can SEE the problem
# MAGIC Databricks normally hides skew for us: **AQE** (Adaptive Query Execution) splits skewed partitions, and small tables
# MAGIC are **broadcast** (no shuffle). We switch both off first so the raw skew is visible — then we'll turn them back on as fixes.

# COMMAND ----------

def set_conf(k, v):
    try:
        spark.conf.set(k, v); print(f"set {k} = {spark.conf.get(k)}")
    except Exception as e:
        print(f"could NOT set {k}: {type(e).__name__} {str(e)[:80]}")

set_conf("spark.sql.adaptive.enabled", "false")            # off: don't auto-fix skew
set_conf("spark.sql.autoBroadcastJoinThreshold", "-1")     # off: force a shuffle (sort-merge) join

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * `adaptive.enabled=false` stops AQE from splitting the skewed partition for us.
# MAGIC * `autoBroadcastJoinThreshold=-1` forces a **sort-merge join**, which **shuffles** all 50M event rows by `user_id`.
# MAGIC * Now the hot key `U00000` (~80% of rows) will all land on **one** reduce task — the skew we want to observe.
# MAGIC * (If a conf "could NOT set" on this shared cluster, the demo still works — the skew is just smaller.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.2 — Run the slow join  *(then open the Spark UI)*
# MAGIC Join 50M events to the users dimension on the skewed `user_id`, then total revenue by region.

# COMMAND ----------

joined = (events.join(users, "user_id")
                .groupBy("region")
                .agg(F.sum("amount").alias("revenue")))
t_slow = run_timed("skewed sort-merge join", joined)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the Spark UI — do this together now**
# MAGIC 1. Open **Compute → your cluster → Spark UI → Stages** (or the **Spark Jobs** link just under the cell above).
# MAGIC 2. **Sort stages by Duration.** The longest stage is the join's shuffle-read stage — that's where the time goes.
# MAGIC 3. Open it and read **Summary Metrics for tasks**. The tell-tale of **skew**:
# MAGIC    * **Max** task **Duration** is *far* bigger than the **75th percentile** (rule of thumb: Max > ~1.5× p75 = skew; 10×+ = severe).
# MAGIC    * **Shuffle Read Size** shows the same imbalance — one task reads most of the data.
# MAGIC 4. Look for **Spill (Memory)/(Disk)** columns > 0 — that task ran out of memory and wrote to disk (slow).
# MAGIC * One task doing ~80% of the work while 199 others finish fast = **data skew**. That single task is your whole runtime.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.3 — Confirm it in the query plan

# COMMAND ----------

joined.explain("formatted")   # look for: SortMergeJoin, and Exchange hashpartitioning(user_id, 200)

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * The plan shows a **SortMergeJoin** fed by an **Exchange hashpartitioning(user_id)** — that exchange is the shuffle.
# MAGIC * Hash-partitioning by `user_id` sends every `U00000` row to the same partition → one overloaded task.
# MAGIC * The plan tells you *what* Spark will do; the Spark UI tells you *where it hurt*. Read both.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART B — DIAGNOSE (read the stage, then act)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.1 — Quantify the skew
# MAGIC The Spark UI shows it visually; here's the **metric** behind it. Skew is a ratio of the worst task to a typical task.
# MAGIC
# MAGIC ```
# MAGIC skew ratio  =  max(task time)  /  median(task time)
# MAGIC      > ~5×   →  meaningful skew      10×+  →  severe
# MAGIC ```
# MAGIC
# MAGIC We can see the imbalance directly in the **data** — the row counts the shuffle has to move per key:

# COMMAND ----------

display(spark.sql(f"""
  SELECT user_id, COUNT(*) AS rows,
         ROUND(100.0*COUNT(*)/(SELECT COUNT(*) FROM events), 1) AS pct_of_all
  FROM events GROUP BY user_id ORDER BY rows DESC LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * One key holds ~80% of the rows. In a shuffle-join, those rows can't be split across tasks — they must meet their
# MAGIC   match **on one task**. That's why max-task-time ≫ median-task-time.
# MAGIC * **Classify before you fix** (the discipline): is the slow stage **CPU-bound** (heavy compute), **I/O-bound**
# MAGIC   (big shuffle read/write, spill), or **skewed** (one task ≫ the rest)? Here it's **skew → spill (I/O)**. Each has a different fix.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART C — FIX IT (and prove it with the clock)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.1 — Fix 1: broadcast the small table  *(removes the shuffle entirely)*
# MAGIC `users` is tiny. **Broadcast** it to every executor so each event row finds its match **locally** — no shuffle, no skew.
# MAGIC This is the best fix *when one side is small*.

# COMMAND ----------

from pyspark.sql.functions import broadcast
joined_bc = (events.join(broadcast(users), "user_id")
                   .groupBy("region").agg(F.sum("amount").alias("revenue")))
t_bc = run_timed("broadcast join (no shuffle)", joined_bc)
print(f"speedup vs skewed: {t_slow / max(t_bc, 0.1):.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * `broadcast(users)` switches the plan to a **BroadcastHashJoin** — the small table is shipped whole; the big table is **not shuffled**.
# MAGIC * No shuffle ⇒ no hot partition ⇒ no skew. Usually the biggest win when a dimension is broadcast-sized.
# MAGIC * Check the Spark UI: the long shuffle stage is gone; tasks are now evenly short.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.2 — Fix 2: let AQE handle the skew  *(when you can't broadcast)*
# MAGIC If both sides are large, AQE's **skew-join** splits the oversized partition into several. Turn it on and keep broadcast off.

# COMMAND ----------

set_conf("spark.sql.adaptive.enabled", "true")
set_conf("spark.sql.adaptive.skewJoin.enabled", "true")
set_conf("spark.sql.autoBroadcastJoinThreshold", "-1")   # isolate AQE's effect (no broadcast)

joined_aqe = (events.join(users, "user_id")
                    .groupBy("region").agg(F.sum("amount").alias("revenue")))
t_aqe = run_timed("sort-merge + AQE skew-join", joined_aqe)
print(f"speedup vs skewed: {t_slow / max(t_aqe, 0.1):.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * AQE detects the skewed partition at runtime and **splits it** into several smaller tasks, so no single task carries 80%.
# MAGIC * It's automatic — often the "free 80%" fix. In the Spark UI you'll see the skewed stage replaced by an AQE sub-stage with more, smaller tasks.
# MAGIC * AQE is **on by default** on Databricks; we only turned it off to *show* the problem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.3 — Fix 3: salting  *(the manual technique, for when neither fits)*
# MAGIC Add a random **salt** to the hot key so its rows spread across N partitions; replicate the small side N times to match.
# MAGIC More code, but it works when you can't broadcast and AQE isn't enough. Concept + sketch:

# COMMAND ----------

SALT = 16
# big side: give every row a random salt 0..SALT-1
ev_salted = events.withColumn("_salt", (F.rand() * SALT).cast("int"))
# small side: replicate each user row once per salt value, so matches still line up
salts = spark.range(SALT).withColumnRenamed("id", "_salt")
us_salted = users.crossJoin(salts)
joined_salt = (ev_salted.join(us_salted, ["user_id", "_salt"])
                        .groupBy("region").agg(F.sum("amount").alias("revenue")))
t_salt = run_timed("salted join (hot key spread over 16)", joined_salt)

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * Salting turns one hot partition into `SALT` normal ones: `U00000` rows now carry `_salt` 0..15 and spread across tasks.
# MAGIC * The small side is replicated per salt so every match still occurs. Cost: extra rows on the small side and more code.
# MAGIC * Use it as a last resort — prefer **broadcast** (small side) or **AQE** (both large) first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.4 — Scoreboard

# COMMAND ----------

print("Runtime comparison (lower is better):")
for label, dt in [("skewed (no fix)", t_slow), ("broadcast", t_bc), ("AQE skew-join", t_aqe), ("salting", t_salt)]:
    bar = "#" * max(1, int(dt / max(t_slow, 0.1) * 40))
    print(f"  {label:20} {dt:6.1f}s  {bar}")
print("\nRule of thumb: small dimension -> broadcast; both large -> AQE; neither -> salt.")

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART D — METRICS AT SCALE: JOBS UI & SYSTEM TABLES
# MAGIC # ════════════════════════════════════════════════
# MAGIC The Spark UI diagnoses **one** run. To watch **many** jobs over time you use the **Jobs UI run history** and the
# MAGIC **system tables**. The system tables are **account-admin-gated** (like lineage in M5), so the queries below are guarded —
# MAGIC if they're blocked, use the **Jobs UI** (always available): a job → Runs → per-run duration, status, and the Spark UI link.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D.1 — Job run history (system.lakeflow)  *(walkthrough)*
# MAGIC `job_run_timeline` records every run's start/end/result — your latency & error-rate source over time.

# COMMAND ----------

try:
    display(spark.sql("""
        SELECT job_id, run_id, period_start_time, period_end_time, result_state
        FROM system.lakeflow.job_run_timeline
        ORDER BY period_start_time DESC
        LIMIT 10
    """))
except Exception as e:
    print("system.lakeflow not accessible here:", type(e).__name__, str(e)[:120])
    print("Use the Jobs UI → a job → Runs tab for the same per-run duration & status.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D.2 — Cluster utilization (system.compute.node_timeline)  *(walkthrough)*
# MAGIC Minute-by-minute CPU/memory per node — your **saturation** signal (are we starved, or over-provisioned?).

# COMMAND ----------

try:
    display(spark.sql("""
        SELECT start_time, cpu_user_percent, cpu_system_percent, mem_used_percent
        FROM system.compute.node_timeline
        ORDER BY start_time DESC
        LIMIT 10
    """))
except Exception as e:
    print("system.compute not accessible here:", type(e).__name__, str(e)[:120])
    print("Use the cluster's Metrics tab (Compute → cluster → Metrics) for CPU/memory over time.")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * System tables turn one-off Spark-UI reads into **trends**: latency per run, error rate, CPU/memory saturation.
# MAGIC * On those trends you set **alerts** (Databricks SQL Alerts: a query + a condition + a schedule + a destination).
# MAGIC * Alert on **symptoms** users feel (run took > SLA, error rate up), not on every low-level cause — that's how you avoid alert fatigue.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART E — LAB  (you diagnose & fix — fill each # TODO)
# MAGIC # ════════════════════════════════════════════════
# MAGIC Switch `SCHEMA` to your personal schema first, then work through these.

# COMMAND ----------

# L1 — Reproduce the skew on YOUR data: turn AQE + broadcast off, run the skewed join, and TIME it.
# TODO:
#   spark.conf.set("spark.sql.adaptive.enabled", "false")
#   spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
#   df = events.join(users, "user_id").groupBy("region").agg(F.sum("amount").alias("revenue"))
#   t = run_timed("my skewed join", df)
pass

# COMMAND ----------

# L2 — Open the Spark UI for that run. In one sentence, record the evidence of skew
#      (max task duration vs the 75th-percentile, and whether any task spilled).
# TODO (write your finding as a comment or print):
pass

# COMMAND ----------

# L3 — Fix it the best way for a small dimension, and TIME it. Compute your speedup.
# TODO:
#   from pyspark.sql.functions import broadcast
#   df2 = events.join(broadcast(users), "user_id").groupBy("region").agg(F.sum("amount").alias("revenue"))
#   t2 = run_timed("my broadcast join", df2)
#   print("speedup:", round(t/t2, 1), "x")
pass

# COMMAND ----------

# L4 — Classify: was the slow stage CPU-bound, I/O-bound, or skewed? Justify from the Spark UI evidence.
# TODO:
pass

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART F — ROOT-CAUSE PLAYBOOK  (debrief)
# MAGIC # ════════════════════════════════════════════════
# MAGIC A reusable runbook for "a job is slow / failing." Fill the blanks for *this* incident, then keep it for next time.
# MAGIC
# MAGIC 1. **Detect** — what fired? (SLA miss / alert / failed run). What's the symptom users feel?
# MAGIC 2. **Localize** — Spark UI → longest stage. CPU-bound, I/O-bound (shuffle/spill), or **skew** (max ≫ p75)?
# MAGIC 3. **Diagnose** — name the cause with evidence (e.g., "hot key `U00000` = 80% of rows → one task + spill").
# MAGIC 4. **Fix** — match the cause: broadcast (small side) · AQE skew-join (both large) · salting · more partitions · cache.
# MAGIC 5. **Verify** — re-run; compare the clock and the Spark UI; confirm the SLA is met.
# MAGIC 6. **Prevent** — the guard: an **alert** on run duration / error rate, or a fix made permanent (AQE on, broadcast hint).
# MAGIC
# MAGIC **Discipline:** *read the stage, then act.* Don't add workers or change code before you know whether it's CPU, I/O, or skew.

# COMMAND ----------

# MAGIC %md
# MAGIC # Wrap-up
# MAGIC * The **Spark UI** is your microscope: longest stage → Summary Metrics → max-vs-p75 = skew, spill = I/O pressure.
# MAGIC * Fixes match the cause: **broadcast**, **AQE**, **salting**. Prove every fix with the clock and the UI.
# MAGIC * **System tables / Jobs UI** turn single runs into trends; **alerts** watch the symptoms so you don't have to.
# MAGIC * Next: **Module 7 — Security Integration in the BI Platform.**