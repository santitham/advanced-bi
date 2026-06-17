# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4 — Platform Engineering
# MAGIC ## Instructor Demo Notebook (light — this module is concept-heavy)
# MAGIC
# MAGIC Module 4 is mostly lecture. This notebook is the **one short instructor demo**: we *inspect* the
# MAGIC compute objects you normally click, then *read* infrastructure-as-code and run a **safe `terraform plan`**.
# MAGIC There is **no participant notebook** — the lab is a structured **discussion** (see the worksheet).
# MAGIC
# MAGIC * **Part A — Inspect the compute objects.** A cluster's JSON config and a cluster (compute) policy —
# MAGIC   the "as-code" view of what the UI shows.
# MAGIC * **Part B — IaC, two ways.** The same job declared by an **Asset Bundle** (deploy-safe to dev) and by
# MAGIC   **Terraform** (we `plan` only — it creates nothing).
# MAGIC
# MAGIC Tag on each section: **[I] inspect together** · **[D] instructor demo**.
# MAGIC
# MAGIC **Prerequisite:** any cluster. The inspect cells use the pre-installed `databricks-sdk` and are guarded —
# MAGIC if a read is blocked by policy, they fall back to a representative example so the demo never breaks.
# MAGIC The Terraform cell needs outbound internet (to download the binary + provider) and a PAT in a secret
# MAGIC scope; if either is missing it prints guidance and skips. **Have a screen recording as backup.**

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART A — INSPECT THE COMPUTE OBJECTS  [I]
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## A1 — A Cluster's JSON Config  *(the "as-code" view of what you clicked)*
# MAGIC Every cluster is, underneath, a JSON spec. We read *this* cluster's config via the Databricks SDK.
# MAGIC **Predict:** which fields here would a cost-conscious platform engineer care about most?

# COMMAND ----------

import json
EXAMPLE_CLUSTER = {
    "cluster_name": "etl-nightly",
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "m5d.large",
    "autoscale": {"min_workers": 2, "max_workers": 8},
    "autotermination_minutes": 20,
    "data_security_mode": "USER_ISOLATION",
    "policy_id": "etl-standard",
}

def show_this_cluster():
    """Read THIS cluster's real config; fall back to the example if blocked by policy."""
    try:
        from databricks.sdk import WorkspaceClient
        cid = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        c = WorkspaceClient().clusters.get(cid).as_dict()
        keep = ["cluster_name", "spark_version", "node_type_id", "autoscale",
                "autotermination_minutes", "data_security_mode", "policy_id", "num_workers"]
        return {k: c[k] for k in keep if k in c}, "live (this cluster)"
    except Exception as e:
        return EXAMPLE_CLUSTER, f"example (live read unavailable: {type(e).__name__})"

cfg, source = show_this_cluster()
print("source:", source)
print(json.dumps(cfg, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the platform-engineering idea**
# MAGIC * This JSON is exactly what a **bundle** or a **Terraform** file declares — the cluster you "click" is just data.
# MAGIC * Cost/safety fields to watch: `autoscale` (the floor/ceiling), `autotermination_minutes` (idle shutoff),
# MAGIC   `node_type_id` (size), and `policy_id` — the **policy that constrains all of the above**.
# MAGIC * Platform engineering = moving this object out of the UI and into versioned, reviewable code.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A2 — A Cluster (Compute) Policy  *(governance as JSON)*
# MAGIC A **policy** is a JSON rule set that limits what compute users can create. We list the policies on this
# MAGIC workspace and print one definition. **Predict:** which attribute disabled your Web Terminal back in M3?

# COMMAND ----------

EXAMPLE_POLICY = {
    "spark_version": {"type": "fixed", "value": "15.4.x-scala2.12"},
    "autotermination_minutes": {"type": "range", "maxValue": 60},
    "autoscale.max_workers": {"type": "range", "maxValue": 8},
    "enable_web_terminal": {"type": "fixed", "value": False},
}

def show_a_policy():
    """List compute policies; print the first definition. Fall back to the example if blocked."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        pols = list(w.cluster_policies.list())
        if not pols:
            return EXAMPLE_POLICY, "example (no policies visible to you)"
        names = [p.name for p in pols]
        defn = json.loads(pols[0].definition) if pols[0].definition else {}
        print("policies on this workspace:", names)
        return defn, f"live (policy: {pols[0].name})"
    except Exception as e:
        return EXAMPLE_POLICY, f"example (live read unavailable: {type(e).__name__})"

defn, source = show_a_policy()
print("source:", source)
print(json.dumps(defn, indent=2)[:1200])

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the platform-engineering idea**
# MAGIC * `"type": "fixed"` locks a value; `"type": "range"` bounds it. A policy is **guardrails as configuration**.
# MAGIC * `"enable_web_terminal": {"type": "fixed", "value": false}` is precisely what disabled the Web Terminal in M3.
# MAGIC * Policies enforce **cost** (max workers, auto-terminate) and **security** (features, node types) for *everyone*,
# MAGIC   without blocking self-service. This is the platform engineer's main governance lever for compute.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART B — INFRASTRUCTURE AS CODE, TWO WAYS  [D]
# MAGIC # ════════════════════════════════════════════════
# MAGIC The same idea — declare resources as code — at two layers:
# MAGIC **Asset Bundles** ship your *workloads*; **Terraform** provisions the *platform*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B1 — Layer 1: the Asset Bundle (deploy-safe to dev)
# MAGIC The bundle in `IaC Sample/databricks.yml` declares a **job + job cluster**. `bundle validate` checks it;
# MAGIC `bundle deploy -t dev` would create a username-prefixed, paused job (safe). We validate here.
# MAGIC **Predict:** why is deploying *this* safe, but applying the Terraform file is not?

# COMMAND ----------

# MAGIC %sh
# MAGIC # Show the bundle, then validate it if the Databricks CLI is available.
# MAGIC echo "=== databricks.yml (the workload, as code) ==="
# MAGIC sed -n '1,40p' "/Workspace/Users/santitham.pro@kmutt.ac.th/IaC samples/databricks.yml" 2>/dev/null || echo "(open /Workspace/Users/santitham.pro@kmutt.ac.th/IaC samples/databricks.yml in the repo to read it)"
# MAGIC echo
# MAGIC if command -v databricks >/dev/null 2>&1; then
# MAGIC   echo "=== databricks bundle validate ==="
# MAGIC   databricks bundle validate 2>&1 | head -20 || echo "(validate needs the bundle root + auth; read-only step)"
# MAGIC else
# MAGIC   echo "Databricks CLI not on this cluster — show 'bundle validate' from the recording, or run it from your laptop."
# MAGIC fi

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the platform-engineering idea**
# MAGIC * `bundle validate` parses the YAML and checks the resources — the same gate CI runs (Module 3).
# MAGIC * `bundle deploy -t dev` is **safe**: `mode: development` prefixes the job with your username and pauses it.
# MAGIC * You own this layer. It ships *workloads* — jobs, pipelines, their clusters — not the platform itself.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B2 — Layer 2: Terraform (we PLAN only — it creates nothing)
# MAGIC `terraform plan` shows what *would* be created. We never `apply` in class — provisioning the platform is a
# MAGIC platform-team action needing admin + a sandbox. This cell installs Terraform to `/tmp`, writes a tiny `.tf`,
# MAGIC and runs `init` + `plan`. **If outbound internet or the PAT secret is missing, it skips with guidance —
# MAGIC fall back to the recording.**

# COMMAND ----------

# Provide auth to Terraform from a secret scope (never hard-code a token).
# Create a scope named "platform_demo" with keys "host" and "token" beforehand, OR this cell skips.
import os
try:
    os.environ["TF_VAR_databricks_host"]  = dbutils.secrets.get("platform_demo", "host")
    os.environ["TF_VAR_databricks_token"] = dbutils.secrets.get("platform_demo", "token")
    os.environ["TF_READY"] = "1"
    print("Terraform auth loaded from secret scope 'platform_demo' (host + token).")
except Exception as e:
    os.environ["TF_READY"] = "0"
    print("No 'platform_demo' secret scope found — the next cell will SKIP the live plan and explain.")
    print("That's fine for a concept demo: walk the .tf and play the recording instead.")

# COMMAND ----------

# MAGIC %sh
# MAGIC set +e
# MAGIC if [ "$TF_READY" != "1" ]; then
# MAGIC   echo "SKIPPING live terraform plan (no PAT secret). Walk IaC Sample/main.tf and play the recording."
# MAGIC   exit 0
# MAGIC fi
# MAGIC cd /tmp && rm -rf tfdemo && mkdir tfdemo && cd tfdemo
# MAGIC TFVER=1.9.8
# MAGIC if ! command -v terraform >/dev/null 2>&1 && [ ! -x ./terraform ]; then
# MAGIC   curl -fsS -o tf.zip "https://releases.hashicorp.com/terraform/${TFVER}/terraform_${TFVER}_linux_amd64.zip" \
# MAGIC     && unzip -oq tf.zip || { echo "Could not download Terraform (egress blocked) — use the recording."; exit 0; }
# MAGIC fi
# MAGIC TF=terraform; command -v terraform >/dev/null 2>&1 || TF=./terraform
# MAGIC cat > main.tf <<'HCL'
# MAGIC terraform { required_providers { databricks = { source = "databricks/databricks" } } }
# MAGIC variable "databricks_host" {}
# MAGIC variable "databricks_token" {}
# MAGIC provider "databricks" { host = var.databricks_host  token = var.databricks_token }
# MAGIC resource "databricks_cluster_policy" "etl" {
# MAGIC   name = "etl-standard-demo"
# MAGIC   definition = jsonencode({ "autotermination_minutes" = { type = "range", maxValue = 60 } })
# MAGIC }
# MAGIC HCL
# MAGIC echo "=== terraform init ===";  $TF init  -input=false -no-color 2>&1 | tail -6
# MAGIC echo "=== terraform plan (PLAN ONLY — creates nothing) ===";  $TF plan -input=false -no-color 2>&1 | tail -25

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the platform-engineering idea**
# MAGIC * `plan` ends with something like **`Plan: 1 to add, 0 to change, 0 to destroy`** — and it **created nothing**.
# MAGIC * Same declarative idea as the bundle, but Terraform governs the **platform** (policies, metastores, networking, users).
# MAGIC * In a real org this runs in **CI**: `plan` on the PR for review, `apply` on merge (often with a manual prod approval).
# MAGIC   You only `apply` through the pipeline as a service principal — never from a laptop against prod.

# COMMAND ----------

# MAGIC %md
# MAGIC # Wrap-up
# MAGIC * **Inspect** showed every clicked object is really JSON; **IaC** puts that JSON under version control.
# MAGIC * **Two layers:** Asset Bundles (workloads, you deploy) vs Terraform (platform, the platform team applies).
# MAGIC * Nothing here created or changed infrastructure — the bundle was only *validated* and Terraform only *planned*.
# MAGIC * Now move to the **discussion lab**: a platform-decision debate (see the participant worksheet).