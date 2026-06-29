# Databricks notebook source
# MAGIC %md
# MAGIC # Module 7 — Security Integration in the BI Platform
# MAGIC ## Participant Notebook  *(follow the demos, then complete the lab)*
# MAGIC
# MAGIC We protect the **PII** in `customers` (from `M7 — PII Dataset Setup`) with the real Unity Catalog controls:
# MAGIC **least-privilege grants**, a **row filter** (region), and **column masks** (email / phone / national_id) —
# MAGIC then we try to **break** them in a threat scenario.
# MAGIC
# MAGIC * **A — Least privilege** (grants) · **B — Row-level security** · **C — Column masking**
# MAGIC * **D — Dynamic views** (the alternative) · **E — Secrets** · **F — Audit & sharing** (walkthrough)
# MAGIC * **G — Threat scenario** (masking bypassed) · **H — Lab** · **I — Cleanup**
# MAGIC
# MAGIC These run on a table **you own**. After `SET MASK`, even *your own* queries are masked (unless you're in the
# MAGIC privileged group) — that's the control working. Each code cell has a **"What just happened"** note.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — point at the dataset

# COMMAND ----------

CATALOG = "ctl_training_dev"
SCHEMA  = "m7_sec_shared"          # lab: switch to your personal schema, e.g. m7_sec_<you>
FQ = f"`{CATALOG}`.`{SCHEMA}`"
spark.sql(f"USE CATALOG `{CATALOG}`"); spark.sql(f"USE SCHEMA `{SCHEMA}`")

ADMIN_GROUP = "pii_admins"         # the group allowed to see raw PII (likely doesn't exist -> you'll see masked)
print("user      :", spark.sql("SELECT current_user()").first()[0])
print("in", ADMIN_GROUP, ":", spark.sql(f"SELECT is_account_group_member('{ADMIN_GROUP}')").first()[0])
print("=> if False, you'll correctly see FILTERED + MASKED data after we apply the policies.")

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART A — LEAST PRIVILEGE (grants)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.1 — Who can see this table today?
# MAGIC `SHOW GRANTS` lists the privileges on an object. The setup granted SELECT to **all account users** — that's
# MAGIC broad. Least privilege says: grant the **minimum** needed, to **groups**, not individuals.

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON TABLE {FQ}.customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * The privilege model: `USE CATALOG` → `USE SCHEMA` → `SELECT` / `MODIFY` on objects. Privileges **inherit** downward.
# MAGIC * `account users` with `SELECT` = everyone can read raw PII. The "broad grant to unblock someone" anti-pattern.
# MAGIC * Fix in two moves: tighten the grant (next), then add **row filters + masks** so even readers see only what they should.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A.2 — Tighten the grant (least privilege)
# MAGIC Grant to a **group** that maps to a need, not to "everyone". (Use a group that exists in your workspace; here we
# MAGIC show the statements — adjust the group name to one you manage.)

# COMMAND ----------

# Example least-privilege statements (edit the group name to one in your workspace):
print("REVOKE SELECT ON TABLE customers FROM `account users`;   -- stop the blanket access")
print("GRANT  SELECT ON TABLE customers TO `analysts`;          -- grant to a role-based group instead")
print("\nWe keep `account users` for the class demo so everyone can follow along,")
print("but in production you'd scope SELECT to the smallest group that needs it.")

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * `GRANT`/`REVOKE` are how you implement least privilege. Grant to **groups** (scale, survives staff changes).
# MAGIC * Privilege creep is real — audit grants periodically and revoke what's unused (Part F).
# MAGIC * Grants control **who can query**; row filters & masks control **what they see** when they do. Both, together.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART B — ROW-LEVEL SECURITY (row filter)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.1 — Before: everyone sees every region

# COMMAND ----------

display(spark.sql(f"SELECT region, COUNT(*) AS rows FROM {FQ}.customers GROUP BY region ORDER BY region"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.2 — Apply a row filter
# MAGIC A **row filter** is a SQL function returning TRUE (keep the row) or FALSE (hide it), evaluated per row at query time.
# MAGIC Here: admins see all regions; everyone else sees only **APAC**. (In production you'd map each user to their region.)

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {FQ}.rf_region(region STRING)
  RETURN is_account_group_member('{ADMIN_GROUP}') OR region = 'APAC'
""")
spark.sql(f"ALTER TABLE {FQ}.customers SET ROW FILTER {FQ}.rf_region ON (region)")
print("row filter applied on customers(region)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B.3 — After: the same query now returns only your allowed rows

# COMMAND ----------

display(spark.sql(f"SELECT region, COUNT(*) AS rows FROM {FQ}.customers GROUP BY region ORDER BY region"))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * The query didn't change — the **table** did. UC applies the filter for every reader, on any compute.
# MAGIC * You (not in `pii_admins`) now see only **APAC**; an admin in that group still sees all four regions.
# MAGIC * Row-level security = one table, many tailored views, enforced centrally — no per-dashboard `WHERE` clauses to forget.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART C — COLUMN MASKING (protect the PII values)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.1 — Apply column masks to email, phone, national_id
# MAGIC A **column mask** is a SQL function that transforms a value at query time. Admins see the raw value; everyone
# MAGIC else sees a masked form. We bind one mask per sensitive column with `ALTER COLUMN … SET MASK`.

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {FQ}.mask_email(v STRING)
  RETURN CASE WHEN is_account_group_member('{ADMIN_GROUP}') THEN v
              ELSE concat('***@', split(v, '@')[1]) END
""")
spark.sql(f"""
  CREATE OR REPLACE FUNCTION {FQ}.mask_tail4(v STRING)
  RETURN CASE WHEN is_account_group_member('{ADMIN_GROUP}') THEN v
              ELSE concat('****', right(v, 4)) END
""")
spark.sql(f"ALTER TABLE {FQ}.customers ALTER COLUMN email       SET MASK {FQ}.mask_email")
spark.sql(f"ALTER TABLE {FQ}.customers ALTER COLUMN phone       SET MASK {FQ}.mask_tail4")
spark.sql(f"ALTER TABLE {FQ}.customers ALTER COLUMN national_id SET MASK {FQ}.mask_tail4")
print("column masks applied on email, phone, national_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C.2 — Query the PII — it's masked now

# COMMAND ----------

display(spark.sql(f"SELECT customer_id, full_name, email, phone, national_id, region FROM {FQ}.customers LIMIT 8"))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * Email shows `***@example.com`; phone/national_id show only the last 4. The raw values never leave UC for you.
# MAGIC * The mask is on the **table**, so it applies no matter how the data is read — notebook, SQL, BI tool, or export.
# MAGIC * Combine with the row filter: you now see **only APAC rows** AND **masked PII** — defense in depth, one definition.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART D — DYNAMIC VIEWS (the alternative)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## D.1 — The same protection as a view
# MAGIC Before table-level filters/masks existed, teams used **dynamic views**: a view with `CASE` (masking) and a
# MAGIC `WHERE` (row filter), then grant on the **view**, not the base table.

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE VIEW {FQ}.customers_secure AS
  SELECT customer_id, full_name,
         CASE WHEN is_account_group_member('{ADMIN_GROUP}') THEN email
              ELSE concat('***@', split(email,'@')[1]) END AS email,
         region, segment
  FROM {FQ}.customers
  WHERE is_account_group_member('{ADMIN_GROUP}') OR region = 'APAC'
""")
print("created dynamic view customers_secure")
display(spark.sql(f"SELECT * FROM {FQ}.customers_secure LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * The view encodes the same logic. You'd `GRANT SELECT ON VIEW customers_secure` and **never grant the base table**.
# MAGIC * Table-level masks vs views: **masks protect the base table itself** (harder to bypass); **views** are flexible but
# MAGIC   only safe if the base table is **not** also granted — which is exactly the trap in the threat scenario (Part G).

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART E — SECRETS (never hardcode credentials)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## E.1 — Read a secret from a scope
# MAGIC Credentials live in **secret scopes**, referenced at runtime — never typed into a notebook. Reading a secret
# MAGIC returns a **redacted** value in output, so it can't leak to logs.

# COMMAND ----------

try:
    scopes = [s.name for s in dbutils.secrets.listScopes()]
    print("secret scopes you can see:", scopes)
    if "kafka_demo" in scopes:
        val = dbutils.secrets.get("kafka_demo", "bootstrap")   # from Module 1
        print("read kafka_demo/bootstrap ->", val, "  (Databricks redacts it in output)")
    else:
        print("(no kafka_demo scope here — the pattern is: dbutils.secrets.get(scope, key))")
except Exception as e:
    print("secrets:", type(e).__name__, str(e)[:120])

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened**
# MAGIC * The value printed as `[REDACTED]` — secrets are masked in cell output and logs by design.
# MAGIC * **Anti-pattern:** `TOKEN = "dapi123…"` hard-coded in a notebook — it leaks into git, logs, and exports. Never do it.
# MAGIC * Best practice: a scope per role/app/environment; automation authenticates as a **service principal** (OAuth), not a person.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART F — AUDIT & SECURE SHARING  (walkthrough)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## F.1 — Audit logs: who did what, when
# MAGIC Every grant, query, and access attempt is recorded. `system.access.audit` is the table — **admin-gated** (blocked
# MAGIC here, like other system tables), so this is a walkthrough; account admins read it or ship it to your SIEM.

# COMMAND ----------

try:
    display(spark.sql("""
        SELECT event_time, user_identity.email AS who, action_name, request_params
        FROM system.access.audit
        WHERE action_name IN ('getTable','generateTemporaryTableCredential')
        ORDER BY event_time DESC LIMIT 10
    """))
except Exception as e:
    print("system.access.audit not accessible here:", type(e).__name__, str(e)[:120])
    print("Use the Account console → Audit logs, or have an admin grant access to system.access.audit.")
    print("Audit answers: who queried PII? who changed a grant? any failed-auth spikes? (alert on these — M6).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## F.2 — Secure sharing: Delta Sharing  *(concept)*
# MAGIC To share data **across** organizations without copying it, use **Delta Sharing**: the provider creates a *share*
# MAGIC and adds tables; a *recipient* reads it live, governed and audited — no SFTP of CSVs, no credentials in email.
# MAGIC Both sides get audit logs of every access. (Creating a share needs metastore-admin rights.)

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART G — THREAT SCENARIO: "masking bypassed via export"
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## G.1 — The attack
# MAGIC An analyst is shown `customers_secure` (masked email, APAC only). They try to get the raw PII anyway by going
# MAGIC **around** the view — querying the base table directly, then exporting it.

# COMMAND ----------

# The analyst tries the base table directly (the "bypass"):
print("--- analyst queries the BASE table directly ---")
display(spark.sql(f"SELECT customer_id, email, phone, national_id, region FROM {FQ}.customers LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened — the lesson**
# MAGIC * Because we put the mask + row filter **on the base table** (Parts B–C), the direct query is **still masked & filtered**.
# MAGIC   The export would carry only masked values. The bypass fails. ✅
# MAGIC * Contrast: if we had **only** the dynamic view (Part D) and left `SELECT` on the base table, the analyst would read
# MAGIC   raw PII directly and export it. ❌ The view protected nothing because the base was still exposed.
# MAGIC * **Defense in depth wins:** least privilege (revoke base) **+** table-level masks/filters **+** audit (catch the attempt).
# MAGIC   A mask alone, or a view alone with a loose grant, is not enough.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART H — LAB  (you secure it — fill each # TODO)
# MAGIC # ════════════════════════════════════════════════
# MAGIC Switch `SCHEMA` to your personal schema (rebuild it with the setup notebook in MODE="lab"), then:

# COMMAND ----------

# L1 — Apply a column mask to `phone` that shows only the last 4 digits.
# TODO: bind the mask_tail4 function to the phone column, then SELECT phone to confirm it's masked.
#   spark.sql(f"ALTER TABLE {FQ}.customers ALTER COLUMN ____ SET MASK {FQ}.____")
pass

# COMMAND ----------

# L2 — Apply a row filter so only the 'EMEA' region is visible to non-admins.
# TODO: create rf_region returning is_account_group_member('pii_admins') OR region = '____',
#       then ALTER TABLE ... SET ROW FILTER, then verify the GROUP BY region shows only EMEA.
pass

# COMMAND ----------

# L3 — Show the grants on your customers table and write one least-privilege improvement you'd make.
# TODO:
#   display(spark.sql(f"SHOW GRANTS ON TABLE {FQ}.customers"))
pass

# COMMAND ----------

# L4 — Threat check: with your mask on the base table, query the base table directly. Is the PII protected? Why?
# TODO (run the query, write your one-line conclusion):
pass

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART I — CLEANUP  (remove the policies so the demo is re-runnable)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

for col in ["email", "phone", "national_id"]:
    try: spark.sql(f"ALTER TABLE {FQ}.customers ALTER COLUMN {col} DROP MASK")
    except Exception as e: print("drop mask", col, ":", str(e)[:60])
try: spark.sql(f"ALTER TABLE {FQ}.customers DROP ROW FILTER")
except Exception as e: print("drop row filter:", str(e)[:60])
spark.sql(f"DROP VIEW IF EXISTS {FQ}.customers_secure")
print("cleaned up masks, row filter, and the dynamic view — customers is back to raw.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Wrap-up
# MAGIC * **Authn vs authz:** who you are vs what you can do. Least-privilege **grants** to **groups**.
# MAGIC * **Row filters** tailor rows; **column masks** hide values — on the **table**, enforced everywhere.
# MAGIC * **Secrets** in scopes, never hardcoded; automation uses **service principals**.
# MAGIC * **Audit** proves it; **Delta Sharing** shares safely. **Defense in depth** beats any single control.
# MAGIC * Next: **Module 8 — Cloud Cost Calculation.**