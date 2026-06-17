# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3 — CI/CD for Data Engineering
# MAGIC ## Demo + Lab Notebook (one file to upload & test on Databricks)
# MAGIC
# MAGIC We take the **orders → revenue-by-category** transform from M1/M2 and make it **safe to ship**:
# MAGIC pure testable functions, unit + schema + data-quality tests, a CI pipeline of gates, and deployment with
# MAGIC **Databricks Asset Bundles**. The full project lives in the **Repo Skeleton** folder — this notebook runs
# MAGIC the testable parts; the facilitator demos the dev loop in a **Databricks Git folder**, a real GitHub
# MAGIC Actions run, and `bundle deploy`.
# MAGIC
# MAGIC * **Part A — Foundations.** A CI/CD recap, basic **Git** hands-on (`%sh`), and **Git on Databricks** (Git folders).
# MAGIC * **Part B — The automated safety net.** Pure transforms → unit/schema/data-quality tests → CI gates → Asset Bundles.
# MAGIC * **Part C — Lab + Incident Simulation.** Add tests + a CI gate, then watch a schema change break prod and get caught.
# MAGIC
# MAGIC Every code cell is followed by a **"Reading the output"** cell explaining the CI/CD idea.
# MAGIC
# MAGIC **Prerequisite:** any Databricks cluster (no Kafka needed). We `%pip install chispa pytest` —
# MAGIC `chispa` for DataFrame test assertions and `pytest` to run the cloned repo's tests in C3.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — run these two cells once at the start

# COMMAND ----------

# MAGIC %pip install chispa pytest

# COMMAND ----------

# The PURE TRANSFORMS — mirror src/orders_pipeline/transforms.py in the Repo Skeleton.
# DataFrame in -> DataFrame out, no I/O: that is what makes them unit-testable.
from pyspark.sql import functions as F

PRICE = 9.99

def add_revenue(df, price=PRICE):
    return df.withColumn("revenue", F.round(F.col("orderunits") * price, 2))

def enrich_category(orders, items):
    return orders.join(items, "itemid", "left")

def revenue_by_category(orders, items, price=PRICE):
    return (enrich_category(add_revenue(orders, price), items)
            .groupBy("category")
            .agg(F.sum("revenue").alias("revenue"), F.count("*").alias("orders")))

# DATA-QUALITY RULES — mirror src/orders_pipeline/quality.py. A row is good when all hold.
DQ_RULES = {
    "orderid_not_null":    "orderid IS NOT NULL",
    "itemid_not_null":     "itemid IS NOT NULL",
    "orderunits_positive": "orderunits >= 0",
}

def dq_split(df):
    cond = F.expr(" AND ".join(f"({c})" for c in DQ_RULES.values()))
    t = df.withColumn("_g", cond)
    return t.where("_g").drop("_g"), t.where("NOT _g OR _g IS NULL").drop("_g")

def dq_summary(df):
    return {n: df.where(f"NOT ({c}) OR ({c}) IS NULL").count() for n, c in DQ_RULES.items()}

ORDERS_SCHEMA = "orderid long, itemid string, orderunits double"
print("transforms + DQ rules loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the CI/CD idea**
# MAGIC * The logic is **pure functions** (DataFrame in → DataFrame out). No reading Kafka/Delta, no writing tables —
# MAGIC   that I/O lives in the job entrypoint (`notebooks/build_gold.py`).
# MAGIC * Because they are pure, they run in **milliseconds on tiny made-up data, no cluster state** — so a CI server
# MAGIC   can test them on every change. Untestable code is code that mixes logic with I/O.

# COMMAND ----------

# MAGIC %md
# MAGIC # ══════════════════════════════════════════════
# MAGIC # PART A — FOUNDATIONS: CI/CD, Git, and Git on Databricks
# MAGIC # ══════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## C1 — CI/CD: a Quick Recap  *(slides lead this)*
# MAGIC * **CI (Continuous Integration):** every change is automatically **built and tested** on a server, so bugs
# MAGIC   are caught in minutes on a pull request — not in production.
# MAGIC * **CD (Continuous Delivery/Deployment):** a passing build is automatically **packaged and deployed** to an
# MAGIC   environment (**dev → prod**), repeatably.
# MAGIC * The backbone is **Git** — the shared history CI watches and CD ships. So we start with Git, then add the
# MAGIC   **gates** (tests) and the **deployer** (Asset Bundles).

# COMMAND ----------

# MAGIC %md
# MAGIC ## C2 — Basic Git, Hands-On (step by step)  *(≈10 min)*
# MAGIC The core loop: **init → edit → `add` → `commit` → `branch` → `diff`**. Each cell below is **one step** —
# MAGIC run it and read the output before moving on. We use `%sh` on a throwaway repo (real git, no Databricks).
# MAGIC *(If your cluster blocks `%sh`, do the same steps in the GitHub web UI — see the participant guide.)*

# COMMAND ----------

# MAGIC %md
# MAGIC **Per-student isolation.** A git repo needs a real filesystem — the `/Workspace` and Volume mounts reject
# MAGIC `git init` ("Operation not supported"). So we use a **per-user folder on local disk** (`/tmp/<you>/…`): unique
# MAGIC per student (no collision) and git-capable. This cell exports `$GITDIR` for the `%sh` steps below.

# COMMAND ----------

import os
USER = spark.sql("SELECT current_user()").first()[0]
GITDIR = f"/tmp/{USER}/m3_gitdemo"     # per-user LOCAL dir: unique (no collision) AND git-capable
# Note: git can't init on /Workspace or /Volumes (FUSE mounts -> "Operation not supported"),
# so the repo lives on the driver's local disk under your username. It's transient — fine for a demo.
os.environ["GITDIR"] = GITDIR
print("git demo dir:", GITDIR)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 — create a repo.** `git init` turns a folder into a repository; we set an identity for commits.

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf "$GITDIR" && mkdir -p "$GITDIR" && cd "$GITDIR"
# MAGIC git init -q && git branch -m main
# MAGIC git config user.email "you@example.com" && git config user.name "You"
# MAGIC echo "repo ready on branch:"; git branch --show-current

# COMMAND ----------

# MAGIC %md
# MAGIC * `git init` creates a hidden **`.git/`** folder — that *is* the history/database.
# MAGIC * Every commit is stamped with the **identity** you set; we renamed the default branch to **`main`**.
# MAGIC * Each `%sh` cell is a fresh shell, so every step starts with `cd "$GITDIR"` — the folder persists across cells.

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 — stage & commit.** Edit a file, **`git add`** it to the staging area, then **`git commit`** a snapshot.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd "$GITDIR"
# MAGIC echo "price = 9.99" > pricing.txt
# MAGIC git add pricing.txt
# MAGIC echo "--- staged (git status) ---"; git status -s
# MAGIC git commit -qm "Add pricing rule"
# MAGIC echo "--- history (git log) ---"; git log --oneline

# COMMAND ----------

# MAGIC %md
# MAGIC * **`add`** moves your edit into the **staging area** (`A pricing.txt` = added/staged).
# MAGIC * **`commit`** records a permanent **snapshot** with a message — a point you can always return to.
# MAGIC * `git log` shows the history: one commit so far, on `main`.

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3 — branch & change.** Isolate new work on a **feature branch** so `main` stays safe.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd "$GITDIR"
# MAGIC git switch -c feature/raise-price
# MAGIC echo "price = 12.99" > pricing.txt
# MAGIC git commit -aqm "Raise price to 12.99"
# MAGIC echo "--- history on this branch ---"; git log --oneline

# COMMAND ----------

# MAGIC %md
# MAGIC * **`git switch -c`** creates and moves onto a new branch — a movable pointer to your work.
# MAGIC * You committed on the branch **without touching `main`**. This branch is one PR's worth of change.
# MAGIC * `-aqm` = stage tracked changes (`-a`), quiet (`-q`), message (`-m`) in one command.

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4 — compare (the pull-request view).** `git diff` shows what changed between `main` and the branch.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd "$GITDIR"
# MAGIC echo "=== branches ==="; git branch
# MAGIC echo "=== graph (all commits) ==="; git log --oneline --graph --all
# MAGIC echo "=== diff: main vs feature ==="; git diff main feature/raise-price -- pricing.txt

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the CI/CD idea**
# MAGIC * The **diff** (`9.99` → `12.99`) is exactly what a reviewer reads on a **pull request**.
# MAGIC * In a team you'd **push** this branch and open the PR — that push is what **triggers CI** (the next sections).
# MAGIC * You merge into `main` only after the **gates** (tests) pass. You never edit `main` directly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C3 — Git on Databricks: Git Folders (best practice)  *(≈6 min)*
# MAGIC On Databricks you don't run git in a terminal — you use **Git folders (Repos)**: clone a branch into the
# MAGIC workspace, edit/run against a cluster, and commit/push from the UI.
# MAGIC **The loop:** clone fork → branch → edit & run tests → commit & push → PR → CI → merge → deploy.
# MAGIC **Best practices:**
# MAGIC * Keep **logic in `src/`** (testable) and notebooks **thin**; commit notebooks as **source format** (`.py`), not `.ipynb` with outputs.
# MAGIC * **Never commit secrets or data** — use secret scopes + `.gitignore` (ours excludes `__pycache__` and caches).
# MAGIC * One **short-lived branch per change**, merged via PR; let CI gate it.
# MAGIC * `dev`/`prod` are **environments** (catalogs), not branches — you promote by **deploying**, not long-lived branches.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3 · Step 1 — Clone your fork as a Git folder *(do this in the UI, once)*
# MAGIC Cloning is a **workspace UI action**, not a shell command — there's no code cell for it. Follow these clicks:
# MAGIC
# MAGIC 1. **Fork** `github.com/santitham/orders_pipeline` to your own GitHub account (GitHub web UI → **Fork**).
# MAGIC 2. In Databricks, left sidebar → **Workspace** → open your user folder (**Users / your-email**).
# MAGIC 3. Click the blue **Create** button (top-right) → **Git folder**.
# MAGIC 4. **Git repository URL:** paste *your fork's* URL, e.g. `https://github.com/<your-gh-user>/orders_pipeline.git`
# MAGIC    **Git provider:** GitHub. (First time only: add a GitHub **personal access token** under *Settings → Linked accounts → Git integration*.)
# MAGIC 5. Click **Create Git folder.** It clones into `/Workspace/Users/<your-email>/orders_pipeline`.
# MAGIC
# MAGIC That folder is now a live clone you can edit, run, and commit/push from — the small **branch button** at the top of the
# MAGIC folder is where you switch branches, commit, and push (the equivalent of the `git` commands you ran by hand in C2).
# MAGIC
# MAGIC > **Demo without forking:** if you're just watching, skip the clone — **Part B below runs the same tests inline**, so
# MAGIC > nothing downstream depends on this step. The clone is what *students* do in the lab.

# COMMAND ----------

# C3 · Step 2 — Run YOUR cloned Git folder's tests, on this cluster.
# After Step 1, this path is already correct (it auto-fills your username). If you cloned
# somewhere else, edit REPO_PATH. If you didn't clone, this prints a note and Part B covers the tests.
#
# We run pytest IN-PROCESS (pytest.main), not in a subprocess. Why: a shared / Spark-Connect
# cluster won't let a child process build its own Spark (Py4J whitelisting). conftest.py is written
# to REUSE the cluster's already-running Spark when one exists — so it only works in this same process.
import os, sys
USER = spark.sql("SELECT current_user()").first()[0]
REPO_PATH = f"/Workspace/Users/{USER}/orders_pipeline"   # default clone location from Step 1

if os.path.isdir(REPO_PATH):
    import pytest
    sys.dont_write_bytecode = True                  # /Workspace (FUSE) rejects __pycache__ writes
    if os.path.join(REPO_PATH, "src") not in sys.path:
        sys.path.insert(0, os.path.join(REPO_PATH, "src"))   # make `import orders_pipeline` resolve
    code = pytest.main(["-q", "-p", "no:cacheprovider", "--rootdir", REPO_PATH,
                        os.path.join(REPO_PATH, "tests")])
    print("\nexit code:", int(code), " (0 = green build — these same tests run again in GitHub CI)")
else:
    print(f"No Git folder found at {REPO_PATH}.")
    print("Do C3 Step 1 (clone your fork) to run your repo's tests here —")
    print("or just continue: Part B below runs the equivalent tests inline, so the demo works without the clone.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the CI/CD idea**
# MAGIC * **Git folder = develop**; commit/push from the UI **triggers CI**. The workspace is your dev sandbox, not prod.
# MAGIC * The same `pytest` that passes in your Git folder runs again on the GitHub runner before merge — defense in depth.
# MAGIC * **Git folder vs Asset Bundle:** the folder is how you *develop*; the bundle (`bundle deploy`) is how you *ship*.

# COMMAND ----------

# MAGIC %md
# MAGIC # ══════════════════════════════════════════════
# MAGIC # PART B — BUILD THE AUTOMATED SAFETY NET
# MAGIC # ══════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## D1 — Pure, Testable Transforms  *(≈4 min)*
# MAGIC **Predict:** what does `add_revenue` need from a cluster or production data to run? (nothing)
# MAGIC **Observe:** a transform runs on three hand-made rows.

# COMMAND ----------

sample = spark.createDataFrame(
    [(1, "Item_1", 2.0), (2, "Item_2", 1.5), (3, "Item_3", 4.0)], ORDERS_SCHEMA)
add_revenue(sample).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the CI/CD idea**
# MAGIC * The function turned `orderunits` into `revenue` with **no setup** — exactly the property a test needs.
# MAGIC * Keep transforms pure and the job thin: the job reads/writes; the functions decide. Everything below tests
# MAGIC   these functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D2 — Unit Testing with pytest + chispa  *(≈5 min)*
# MAGIC `chispa.assert_df_equality` compares two DataFrames (schema **and** rows). We assert **inline** here; the
# MAGIC **real `pytest`** (in `tests/`) runs in **GitHub Actions** on every push — no shell or files needed in the notebook.
# MAGIC **Predict:** if a transform silently changed a value, would the test catch it? (yes)

# COMMAND ----------

from chispa import assert_df_equality

got = add_revenue(sample.limit(1), price=10.0)           # orderunits 2.0 -> revenue 20.0
exp = spark.createDataFrame([(1, "Item_1", 2.0, 20.0)],
                            "orderid long, itemid string, orderunits double, revenue double")
assert_df_equality(got, exp)
print("inline unit assertion passed")

# COMMAND ----------

# This workspace blocks files + shell, so we assert INLINE. Show the test CATCHING a bug:
# a transform that returns the wrong value fails the assertion.
# Note: chispa raises its own DataFramesNotEqualError (NOT a subclass of AssertionError), so catch it.
from chispa.dataframe_comparer import DataFramesNotEqualError

def add_revenue_BUGGY(df, price=PRICE):
    return df.withColumn("revenue", F.col("orderunits") * price + 1)   # off-by-one bug

try:
    assert_df_equality(add_revenue_BUGGY(sample.limit(1), price=10.0), exp)   # exp expects 20.0
    print("no failure (unexpected)")
except DataFramesNotEqualError:
    print("caught the bug: assert_df_equality flagged the wrong revenue value (21.0 != 20.0)")

print("\nThe REAL pytest in tests/ runs in GitHub Actions on every push — you saw it go green.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the CI/CD idea**
# MAGIC * `assert_df_equality` fails loudly on **any** difference in schema or data — the safety net for refactors.
# MAGIC * Here we ran the **logic** inline (no files, no shell). The same assertions live in `tests/` and run under
# MAGIC   **`pytest`** in CI; a non-zero exit code = a **failed build** that blocks the merge — that gate is the whole point.
# MAGIC * You already saw GitHub Actions run the real `pytest` and go **green** on your repo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D3 — Schema-Validation Test  *(≈4 min)*
# MAGIC A **schema test** asserts the output columns/types — it catches a breaking change *before* it reaches a
# MAGIC dashboard. **Predict:** which is more dangerous in prod — a wrong number, or a renamed column? (often the rename)

# COMMAND ----------

def assert_schema(df, expected_fields):
    actual = [f.name for f in df.schema.fields]
    assert actual == expected_fields, f"SCHEMA DRIFT: got {actual}"

# passes for the current transform
assert_schema(add_revenue(sample), ["orderid", "itemid", "orderunits", "revenue"])
print("schema test passed")

# a 'drifted' transform that renames a column -> the test fails and catches it
def add_revenue_BROKEN(df):
    return df.withColumn("rev", F.col("orderunits") * PRICE)     # wrong column name!
try:
    assert_schema(add_revenue_BROKEN(sample), ["orderid", "itemid", "orderunits", "revenue"])
except AssertionError as e:
    print("caught:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the CI/CD idea**
# MAGIC * A schema test is cheap insurance: a renamed/missing/retyped column **fails the build** instead of silently
# MAGIC   breaking everything downstream that reads the table.
# MAGIC * This is exactly the failure we reproduce in the **Incident Simulation** at the end.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D4 — Data-Quality Gate  *(≈5 min)*
# MAGIC Tests check the **code**; a data-quality gate checks the **data**. We split good vs quarantine and count
# MAGIC violations the build can fail on. **Predict:** should bad rows block the pipeline, or be set aside?

# COMMAND ----------

dirty = spark.createDataFrame([
    (1, "Item_1", 2.0),       # good
    (2, "Item_2", 1.5),       # good
    (None, "Item_3", 1.0),    # bad: null orderid
    (4, "Item_4", -3.0),      # bad: negative units
], ORDERS_SCHEMA)

valid, quarantine = dq_split(dirty)
print("valid:", valid.count(), "| quarantined:", quarantine.count())
print("violations per rule:", dq_summary(dirty))

# the gate: fail the build if any rule is violated above a threshold
THRESHOLD = 0
failed = sum(dq_summary(dirty).values()) > THRESHOLD
print("DATA-QUALITY GATE:", "FAIL (block)" if failed else "PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the CI/CD idea**
# MAGIC * Each rule is a condition that should hold for every row; `dq_summary` counts the failures. A gate **fails the
# MAGIC   build** (or quarantines + alerts) when the data is wrong — code being correct is not enough.
# MAGIC * In production you express these as a framework: **Databricks Labs DQX** (rules + checks at scale) — see the
# MAGIC   optional cell below. Formal **data contracts** and lineage are Module 5.

# COMMAND ----------

# OPTIONAL — Databricks Labs DQX: the productionized way to declare & apply quality checks.
# Guarded so it never breaks the run if DQX isn't installed/available.
try:
    from databricks.labs.dqx.engine import DQEngine
    from databricks.sdk import WorkspaceClient
    print("DQX available — define checks as YAML/rules and apply with DQEngine(WorkspaceClient()).apply_checks(df).")
    print("(See the DQX docs; we keep the hand-rolled rules above for a dependency-free demo.)")
except Exception as e:
    print("DQX not installed here — that's fine; the hand-rolled rules above teach the same idea.")
    print("To use it for real:  %pip install databricks-labs-dqx   (then define checks + DQEngine).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D5 — The CI Pipeline as a Sequence of Gates  *(≈5 min)*
# MAGIC A CI pipeline is just **ordered gates**: lint → unit tests → schema test → data-quality. The first red gate
# MAGIC stops everything. We run them here exactly as `.github/workflows/ci.yml` does.
# MAGIC **Predict:** if the unit tests fail, does deployment still happen? (no)

# COMMAND ----------

def run_ci():
    stages, ok = [], True
    # 1) lint (placeholder for `ruff check`)
    stages.append(("lint", True))
    # 2) unit tests (inline; the real pytest runs in GitHub Actions)
    try:
        got = add_revenue(sample.limit(1), price=10.0)
        exp = spark.createDataFrame([(1, "Item_1", 2.0, 20.0)],
                                    "orderid long, itemid string, orderunits double, revenue double")
        assert_df_equality(got, exp)
        stages.append(("unit-tests", True))
    except Exception:
        stages.append(("unit-tests", False)); ok = False
    # 3) schema test
    try:
        assert_schema(add_revenue(sample), ["orderid", "itemid", "orderunits", "revenue"])
        stages.append(("schema-test", True))
    except AssertionError:
        stages.append(("schema-test", False)); ok = False
    # 4) data-quality gate (on clean sample)
    dq_ok = sum(dq_summary(sample).values()) == 0
    stages.append(("data-quality", dq_ok)); ok &= dq_ok

    print("CI PIPELINE")
    for name, passed in stages:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}")
    print("BUILD:", "GREEN -> deploy" if ok else "RED -> blocked")
    return ok

run_ci()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the CI/CD idea**
# MAGIC * The pipeline is a chain of **gates**; a single failure makes the build **red** and blocks deploy. Fast,
# MAGIC   deterministic gates (run on tiny data) give quick feedback on every PR.
# MAGIC * On a green `main`, the next job runs `databricks bundle deploy -t prod` (next demo). Green build → ship;
# MAGIC   red build → nothing leaves the branch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D6 — Asset Bundles & Deployment  *(≈6 min · walkthrough)*
# MAGIC Code passes the gates — now ship it. A **Databricks Asset Bundle** (`databricks.yml`) packages the code + the
# MAGIC Job and deploys it per **environment**. The facilitator runs the real deploy; here we read the bundle.
# MAGIC **Predict:** why have separate `dev` and `prod` *targets* instead of one?

# COMMAND ----------

print(r"""
# databricks.yml  (in the Repo Skeleton)
bundle:
  name: orders_pipeline
targets:
  dev:                      # isolated sandbox, your own schema, schedules paused
    mode: development
    default: true
    variables: {catalog: ctl_training_dev}
  prod:                     # governed target, deployed by CI only
    mode: production
    variables: {catalog: ctl_training_prod}
resources:
  jobs:
    orders_gold:
      tasks:
        - task_key: build_gold
          notebook_task: {notebook_path: ./notebooks/build_gold.py}

# Deploy from the CLI (facilitator demo):
#   databricks bundle validate -t dev
#   databricks bundle deploy   -t dev      # creates a username-prefixed job in dev
#   databricks bundle run orders_gold -t dev
# CI deploys to prod on merge to main (see .github/workflows/ci.yml).
""")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the CI/CD idea**
# MAGIC * **Dev/Test/Prod isolation:** each target writes to its own **catalog** (`ctl_training_dev` vs `_prod`), so a
# MAGIC   dev run can never touch production data. `mode: development` even prefixes resources with your username.
# MAGIC * A **bundle** is one versioned unit (code + jobs + config); `bundle deploy -t <env>` is repeatable and
# MAGIC   reviewable — no clicking in the UI.
# MAGIC * **Deployment strategies:** *Blue/Green* keeps the old job live until the new one is verified, then switches;
# MAGIC   *Canary* sends a small slice first. Both let you **roll back** instantly to the previous bundle version.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART C — LAB + INCIDENT SIMULATION
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAB — Add a Test and a CI Gate  *(60 min)*
# MAGIC **Goal:** extend the safety net. Write a unit test for `revenue_by_category` and a schema test for its output,
# MAGIC then confirm they pass.
# MAGIC **Success criteria:** both assertions pass; you can explain what each protects.
# MAGIC **In the real dev loop:** add these to `tests/` in your cloned **Git folder** and push — CI runs the same
# MAGIC `pytest` (re-run the D0 cell with `REPO_PATH` set to see it). Below you build them inline.

# COMMAND ----------

# L1 — Unit test revenue_by_category. Two orders in category 'A'; with price=10 the revenue should be 30.0.
items = spark.createDataFrame([("Item_1", "A"), ("Item_2", "A")], "itemid string, category string")
orders = spark.createDataFrame([(1, "Item_1", 2.0), (2, "Item_2", 1.0)], ORDERS_SCHEMA)

# TODO: build the expected one-row result (category 'A', revenue 30.0, orders 2) and assert equality.
#   hint:
#   got = revenue_by_category(orders, items, price=10.0)
#   exp = spark.createDataFrame([("A", 30.0, 2)], "category string, revenue double, orders long")
#   assert_df_equality(got, exp)
pass

# COMMAND ----------

# L2 — Schema test for the gold output: assert its columns are exactly [category, revenue, orders].
gold = revenue_by_category(orders, items)
# TODO: call assert_schema(gold, [...]) with the expected column names.
pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## INCIDENT SIMULATION — A Schema Change Breaks Production  *(45 min)*
# MAGIC A teammate "improves" the transform by renaming `revenue` → `amount`. It merges without a schema test. The
# MAGIC dashboard query `SELECT revenue FROM gold` breaks. Watch CI catch it, then **roll back**.
# MAGIC **Deliverable:** a 3-line incident timeline + the guard test that would have blocked the merge.

# COMMAND ----------

# The "bad deploy": a v2 transform that renames the output column.
def add_revenue_v2(df, price=PRICE):
    return df.withColumn("amount", F.round(F.col("orderunits") * price, 2))   # renamed!

# Run the schema gate against v2 -> it FAILS (this is what CI would have reported on the PR).
print("--- running the schema gate against v2 (the proposed change) ---")
try:
    assert_schema(add_revenue_v2(sample), ["orderid", "itemid", "orderunits", "revenue"])
    print("schema gate PASSED (uh oh — no protection)")
except AssertionError as e:
    print("schema gate FAILED -> PR blocked:", e)

# Roll back: keep using v1. The gate is green again.
print("\n--- rolled back to v1 ---")
assert_schema(add_revenue(sample), ["orderid", "itemid", "orderunits", "revenue"])
print("schema gate PASSED -> safe to ship")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the CI/CD idea**
# MAGIC * The rename is a classic **breaking change**: the code "works" but the **contract** (column names) changed, so
# MAGIC   every downstream reader breaks. A **schema test in CI** turns that into a red build on the PR — caught before
# MAGIC   merge, not at 2 a.m. in production.
# MAGIC * **Roll back** = redeploy the previous bundle version (Blue/Green makes this instant); **prevent** = the guard
# MAGIC   test now in the suite. Incident response = diagnose → roll back → add the test that would have caught it.

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleanup
# MAGIC A throwaway git repo lives under your user folder (`$GITDIR`); nothing else persists. The Repo Skeleton holds the full,
# MAGIC deployable project: `databricks.yml`, `src/`, `tests/`, and `.github/workflows/ci.yml`.

# COMMAND ----------

