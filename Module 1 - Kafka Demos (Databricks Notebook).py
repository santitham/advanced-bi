# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1 — Kafka Architecture & Reliability
# MAGIC ## Demo + Lab Notebook (one file to upload & test on Databricks)
# MAGIC
# MAGIC This notebook has **two parts**:
# MAGIC
# MAGIC * **Part A — Instructor Demos (D1–D7).** Run top to bottom during the lecture. Each demo maps
# MAGIC   to a **▶ TRY IT TOGETHER** slide. Prompts in **bold** are the student engagement cue.
# MAGIC * **Part B — Participant Lab (LAB 1, LAB 2, Scenario SC1).** What learners do hands-on after
# MAGIC   the demos. Cells are runnable as written (this is the facilitator/answer build); the participant
# MAGIC   version blanks the `# TODO` lines.
# MAGIC
# MAGIC Every code cell is followed by a **"Reading the output"** cell that explains what the printout
# MAGIC means *in terms of the Kafka mechanism* — read it aloud; that interpretation is the actual learning.
# MAGIC
# MAGIC **Prerequisite:** a Confluent Cloud cluster with a **Datagen Source Connector** (ORDERS quickstart,
# MAGIC JSON) streaming to topic **`topic_0`**, and an API key stored as Databricks secrets in scope
# MAGIC `kafka_demo` (see the *Confluent Cloud Setup Guide*). The connector emits ~1 order/sec continuously.
# MAGIC A **second** Datagen connector (USERS quickstart) streams to topic **`sample_data_users`**, used in demo D0e.
# MAGIC
# MAGIC **How to test:** attach to a small cluster, run the two setup cells, then Run All. Every cell should
# MAGIC complete without error.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — run these two cells once at the start

# COMMAND ----------

# MAGIC %pip install confluent-kafka python-dotenv
# MAGIC # %pip restarts the Python interpreter, so it MUST be the first cell you run.

# COMMAND ----------

# Shared connection config — reused by every demo and lab cell.
import json, random, time, os
from dotenv import load_dotenv

load_dotenv()
BROKER = os.getenv("BROKER")
KEY = os.getenv("KEY")
SECRET = os.getenv("SECRET")

common = {
    "bootstrap.servers": BROKER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     KEY,
    "sasl.password":     SECRET,
    "enable.metrics.push": False,    # quiet librdkafka client-telemetry log lines
}

TOPIC = "topic_0"     # the Datagen Source Connector streams JSON ORDERS records here
GROUP = "demo"        # tip: set GROUP = "demo_<name>" so each pair watches its own offsets
USERS_TOPIC = "sample_data_users"   # 2nd Datagen stream: USERS (userid, regionid, gender, registertime)

def make_order(i):
    """Build one JSON record matching the Datagen ORDERS schema already on topic_0."""
    return json.dumps({
        "ordertime":  int(time.time() * 1000),       # event time (epoch ms)
        "orderid":    i,
        "itemid":     f"Item_{random.randint(1, 500)}",
        "orderunits": round(random.uniform(0.1, 10.0), 3),
        "address": {
            "city":    f"City_{random.randint(1, 90)}",
            "state":   f"State_{random.randint(1, 50)}",
            "zipcode": random.randint(10000, 99999),
        },
    })

print("Connected to:", BROKER)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * `Connected to: pkc-….confluent.cloud:9092` is the **bootstrap server**. The client connects
# MAGIC   here only to fetch cluster *metadata* (which brokers exist, which broker leads which partition);
# MAGIC   the actual produce/consume traffic then goes **directly to the leader broker** of each partition.
# MAGIC * Nothing secret prints — `BROKER/KEY/SECRET` came from the `kafka_demo` secret scope, so
# MAGIC   credentials never appear in the notebook. This is the secrets-management pattern we formalise in M7.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART A — INSTRUCTOR DEMOS  (▶ TRY IT TOGETHER)
# MAGIC # ════════════════════════════════════════════════

# COMMAND ----------

# MAGIC %md
# MAGIC ## D0 — Kafka 101: the building blocks  *(≈8 min)*
# MAGIC Before the reliability story, let's *see* the pieces every later demo relies on: the **cluster of
# MAGIC brokers**, an **event/record**, how a **key** picks a **partition**, **offsets** on an append-only
# MAGIC log, and **replay**. Run each cell and read the explanation under it.

# COMMAND ----------

# D0a — Explore the cluster: brokers + controller
from confluent_kafka.admin import AdminClient
admin = AdminClient(common)
md = admin.list_topics(timeout=10)
print(f"cluster: {len(md.brokers)} broker(s), controller id = {md.controller_id}")
for bid, b in sorted(md.brokers.items()):
    print(f"  broker {bid}: {b.host}:{b.port}")
print(f"topics visible in this cluster: {len(md.topics)}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * A **broker** is one Kafka server; the **cluster** is the set of brokers. You see each broker's id and
# MAGIC   `host:port`. Confluent Cloud is multi-tenant, so you may see a dozen or more brokers; the point is
# MAGIC * The **controller** is the broker that manages cluster metadata (who leads which partition, membership).
# MAGIC   Modern Kafka elects it via **KRaft** — no ZooKeeper.
# MAGIC * You learned all of this through the single **bootstrap server**: the client asked it "who is in the
# MAGIC   cluster?" and got the full broker list back. That one address is all a client needs to start.

# COMMAND ----------

# D0b — Anatomy of a record: one event, all of its fields
from confluent_kafka import Consumer
c = Consumer({**common, "group.id": "kafka101_peek", "auto.offset.reset": "earliest"})
c.subscribe([TOPIC])
rec = None
for _ in range(20):
    m = c.poll(2.0)
    if m and not m.error():
        rec = m; break
if rec:
    print("topic     :", rec.topic())
    print("partition :", rec.partition())
    print("offset    :", rec.offset())
    print("timestamp :", rec.timestamp())     # (timestamp_type, epoch_ms)
    print("key       :", rec.key())
    print("headers   :", rec.headers())
    print("value     :", rec.value()[:140])
c.close()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * A Kafka **record (event)** is more than its payload:
# MAGIC   * **value** — the payload (here, the JSON order).
# MAGIC   * **key** — Datagen ORDERS records carry one (the orderid) plus headers; some producers omit the key, then it is `None`.
# MAGIC   * **timestamp** — `(type, epoch_ms)`; event-time vs append-time becomes central in M2.
# MAGIC   * **headers** — optional metadata (schema id, trace id, ...).
# MAGIC * **`(topic, partition, offset)` is the record's permanent address** — nothing else identifies it, and
# MAGIC   that triple is exactly how a consumer bookmarks and how we located this record.

# COMMAND ----------

# D0c — A key picks a partition; offsets only ever go up (append-only)
from confluent_kafka import Producer
pr = Producer(common)
placed = []
def d0c_cb(err, msg):
    if not err:
        placed.append((msg.partition(), msg.offset()))
for i in range(3):
    pr.produce(TOPIC, key="alpha", value=make_order(8500 + i), on_delivery=d0c_cb)
pr.flush()
print("3 records, same key 'alpha' -> (partition, offset):", placed)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * All three share **one partition** — `partition = hash(key) % num_partitions`, and `'alpha'` always
# MAGIC   hashes the same way. That is how a key keeps related records **together and in order**.
# MAGIC * Their **offsets increase by exactly 1** and never repeat: a partition is an **append-only log**, so
# MAGIC   records are only ever added at the end. That monotonic offset is the backbone of replay and lag.

# COMMAND ----------

# D0d — Replay: reading does not consume; seek back to the start of a partition
from confluent_kafka import Consumer, TopicPartition
c = Consumer({**common, "group.id": "kafka101_replay", "enable.auto.commit": False})
tp = TopicPartition(TOPIC, 0, 0)             # partition 0, start at offset 0
c.assign([tp])
print("re-reading partition 0 from offset 0:")
for _ in range(5):
    m = c.poll(2.0)
    if m and not m.error():
        v = m.value()
        try:
            oid = json.loads(v)["orderid"]
        except Exception:
            oid = "(non-JSON record)"
        print(f"  offset={m.offset()}  orderid={oid}")
c.close()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * You re-read records from **offset 0** even though other consumers already read them — in Kafka,
# MAGIC   **reading does not delete**. The log keeps every record until **retention** (time/size) expires.
# MAGIC * That is why Kafka is a *replayable log*, not a queue: a new consumer, a reprocessing job, or a bug-fix
# MAGIC   backfill can all re-read history by seeking to an earlier offset. We lean on this for recovery in M2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D0e — A second topic: `sample_data_users`  *(≈4 min)*
# MAGIC The cluster now carries a second Datagen stream — **USERS** (`userid`, `regionid`, `gender`,
# MAGIC `registertime`). Topics are **independent named streams**; the same broker / partition / offset
# MAGIC mechanics apply to each.

# COMMAND ----------

# D0e — one cluster, many topics
md = admin.list_topics(timeout=10)
print("topics in this cluster:")
for name in sorted(md.topics):
    if not name.startswith("_"):                 # skip Kafka's internal topics
        print(f"  {name}  ({len(md.topics[name].partitions)} partitions)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * A cluster holds **many topics** side by side — here `topic_0` (orders) and `sample_data_users` (users).
# MAGIC * Each topic has its **own** partitions, leaders and offsets; topics share the brokers but nothing else.
# MAGIC * Names starting with `_` (e.g. `__consumer_offsets`) are Kafka's **internal** topics that store group
# MAGIC   offsets — even Kafka's own bookkeeping is just another log.

# COMMAND ----------

# D0e — read one USERS record (a different schema than orders)
from confluent_kafka import Consumer
cu = Consumer({**common, "group.id": "users_peek", "auto.offset.reset": "earliest"})
cu.subscribe([USERS_TOPIC])
m = None
for _ in range(20):
    m = cu.poll(2.0)
    if m and not m.error():
        break
if m and not m.error():
    raw = m.value()
    print("raw bytes:", raw[:80])
    try:
        # Confluent Schema Registry frames the payload with a 5-byte header (0x00 + 4-byte schema id)
        u = json.loads(raw[5:] if raw[:1] == b"\x00" else raw)
        print("userid:", u.get("userid"), "| regionid:", u.get("regionid"),
              "| gender:", u.get("gender"), "| registertime:", u.get("registertime"))
    except Exception as e:
        print("not plain JSON (likely Avro via Schema Registry):", e)
cu.close()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * The **value is just bytes** the producer wrote — Kafka never looks inside it. Here it decodes to a
# MAGIC   USERS record: `userid`, `regionid`, `gender`, `registertime`.
# MAGIC * The **schema is a separate contract**, not part of Kafka. This topic's schema lives in **Confluent
# MAGIC   Schema Registry** (`schema-sample_data_users-value-v1.json`). A leading `0x00` + 4 bytes is the
# MAGIC   Schema-Registry framing (magic byte + schema id) before the payload.
# MAGIC * Same partitions, offsets and consumer-group rules as orders — only the **shape of the value** differs.
# MAGIC   Enforcing that shape is the job of schema / data contracts, which we formalise in M5.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D1 — Inspect the Topic  *(≈3 min · Terminal-free, all in the notebook)*
# MAGIC `topic_0` already exists because the Datagen connector created it, so the create call
# MAGIC returns **"already exists"** — that message is expected, not an error.
# MAGIC **Predict:** ask the class "how many partition lines will `--describe` print?" (one per partition).
# MAGIC **Observe & report:** one pair reads out the Leader broker id for partition 0.

# COMMAND ----------

from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient(common)

# Try to create it (6 partitions, replication factor 3). It already exists -> caught below.
futures = admin.create_topics([NewTopic(TOPIC, num_partitions=6, replication_factor=3)])
for t, f in futures.items():
    try:
        f.result()                      # block until done
        print("created topic:", t)
    except Exception as e:
        print(f"{t}: {e}")              # expected: 'already exists' (connector made it)

# Describe it: one line per partition (the architecture slide, live).
meta = admin.list_topics(timeout=10).topics[TOPIC]
for pid, pm in sorted(meta.partitions.items()):
    print(f"partition {pid}: leader=broker {pm.leader}  replicas={pm.replicas}  isr={pm.isrs}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * **One line per partition.** A topic is not a single queue — it is split into **partitions**, and the
# MAGIC   partition is Kafka's **unit of parallelism, ordering and storage**. Six lines = six independent
# MAGIC   append-only logs that can be written and read in parallel.
# MAGIC * **`leader=broker N`.** Every partition has exactly **one leader broker** at a time. *All* reads and
# MAGIC   writes for that partition go through its leader — that is what makes per-partition ordering possible.
# MAGIC   Different partitions usually have different leaders, which is how load spreads across the cluster.
# MAGIC * **`replicas=[…]`.** The partition is copied to this many brokers (replication factor 3 = 3 copies).
# MAGIC   If the leader dies, one replica is promoted — this is the durability/fault-tolerance story.
# MAGIC * **`isr=[…]`** are the **in-sync replicas**: the copies currently caught up to the leader. A write with
# MAGIC   `acks=all` is only acknowledged once all ISR have it. If ISR shrinks below `min.insync.replicas` (2),
# MAGIC   the partition stops accepting `acks=all` writes rather than risk data loss. Watch ISR = replicas here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D2 — Produce Events  *(≈3 min)*
# MAGIC We publish keyed JSON orders and read the **delivery report** to see which partition each
# MAGIC landed on — deterministic regardless of how much live Datagen traffic is on the topic.
# MAGIC **Predict:** "will all of `cust-0`'s events land on the same partition?" (yes — same key → same partition).
# MAGIC **Pair-discuss:** what changes with no key? (round-robin spread).

# COMMAND ----------

from confluent_kafka import Producer

p = Producer(common)

def report(err, msg):
    if err:
        print("delivery error:", err)
    else:
        print(f"key={msg.key().decode()} -> partition {msg.partition()}  offset {msg.offset()}")

for i in range(9):
    p.produce(TOPIC, key=f"cust-{i % 3}", value=make_order(i), on_delivery=report)
p.flush()                               # produce() is async — flush() sends + fires the callbacks
print("sent 9 JSON orders")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * **Same key → same partition, every time.** The producer chooses the partition as
# MAGIC   `hash(key) % num_partitions` (murmur2 hash). So `cust-0`, `cust-1`, `cust-2` each pin to one
# MAGIC   partition for the whole run. That is the *only* ordering guarantee Kafka gives you: **records with
# MAGIC   the same key are ordered, because they share a partition.** Use a key when per-entity order matters
# MAGIC   (e.g. all events for one account).
# MAGIC * **No key → spread out.** Remove the key and the client load-balances across partitions (sticky
# MAGIC   batching), maximising throughput but giving up cross-record ordering.
# MAGIC * **The delivery report is the broker's acknowledgement.** `produce()` is asynchronous — it only
# MAGIC   queues the record. The callback fires after `flush()` when the **leader (and ISR)** confirm the write.
# MAGIC * **`offset N`** is the record's permanent position in that partition's log — monotonically increasing,
# MAGIC   never reused. `(partition, offset)` uniquely identifies a record forever; that is what a consumer
# MAGIC   later "bookmarks".

# COMMAND ----------

# MAGIC %md
# MAGIC ## D3 — Consume Events  *(≈4 min)*
# MAGIC Read records back and parse the JSON. You'll see a **mix**: some orders you just produced
# MAGIC plus live records from the **Datagen connector** already flowing on `topic_0`.
# MAGIC **Predict:** "if we re-run this cell, what offset does it start from?" (the committed offset, not 0).

# COMMAND ----------

from confluent_kafka import Consumer

c = Consumer({**common, "group.id": GROUP, "auto.offset.reset": "earliest"})
c.subscribe([TOPIC])
try:
    for _ in range(10):
        m = c.poll(2.0)
        if m and not m.error():
            v = json.loads(m.value().decode())
            print(f"partition={m.partition()}  offset={m.offset()}  "
                  f"orderid={v['orderid']}  item={v['itemid']}  units={v['orderunits']}")
finally:
    c.close()                           # commits final offsets and leaves the group

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * **`partition` + `offset` on every line.** The consumer pulls (polls) from partition logs and tells
# MAGIC   you exactly where each record sat. Offsets climb **within a partition** but you'll see them interleaved
# MAGIC   across partitions — there is no global order, only per-partition order.
# MAGIC * **Why a mix of orders.** Your D2 records and the connector's records live on the *same* topic, so one
# MAGIC   consumer reads both. Kafka doesn't care who produced a record — consumers read a partition log, full stop.
# MAGIC * **`auto.offset.reset="earliest"`** only applies the **first** time this `group.id` reads — it has no
# MAGIC   committed offset yet, so it starts at the beginning. Re-run the cell and it resumes from the **committed
# MAGIC   offset** (where `c.close()` left the bookmark), not from 0. That bookmark is what makes consumption
# MAGIC   resumable after a restart.
# MAGIC * **Reading does not delete.** Unlike a queue, the log is retained; other groups can read the same records
# MAGIC   independently with their own offsets (you'll see that in D4).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Challenge 1 — Keys & Partitioning  *(≈2 min)*
# MAGIC **Predict, then run.** You produce 6 records that all carry the **same key** `'vip'`.
# MAGIC Before running: will they land on **1**, **3**, or **6** partitions?

# COMMAND ----------

# Quick Challenge 1 — six records, one shared key
from confluent_kafka import Producer
qp = Producer(common)
qc1_partitions = set()
def qc1(err, msg):
    if not err:
        qc1_partitions.add(msg.partition())
KEY_FOR_ALL = "vip"                       # TODO: later, change to f"vip-{i}" and re-run
for i in range(6):
    qp.produce(TOPIC, key=KEY_FOR_ALL, value=make_order(9000 + i), on_delivery=qc1)
qp.flush()
print("distinct partitions these 6 records used:", sorted(qc1_partitions))

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer (reveal after attempting)**
# MAGIC * **One partition.** The producer chooses the partition with `hash(key) % num_partitions`, and an
# MAGIC   identical key always hashes the same way — so all six `vip` records share one partition and stay
# MAGIC   strictly ordered.
# MAGIC * Change `KEY_FOR_ALL` to `f"vip-{i}"` and re-run: now they scatter across partitions and you lose
# MAGIC   cross-record ordering. **The key is your ordering knob.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## D4 — Scale & Inspect a Group: assignment + rebalance  *(≈4 min)*
# MAGIC In a notebook we run consumers in background **threads** so they coexist. Each consumer
# MAGIC prints the partitions it is assigned via an `on_assign` callback. Because Datagen keeps
# MAGIC producing, the consumers actually receive live orders while you watch the rebalance.
# MAGIC
# MAGIC **Predict:** "with 6 partitions and a 7th consumer, what happens to the 7th?" (it sits idle).
# MAGIC **Observe & report:** how did the assignment change when the 2nd consumer joined / left?

# COMMAND ----------

import threading, time
from confluent_kafka import Consumer

def run_consumer(name, stop_evt):
    def on_assign(consumer, partitions):
        print(f"[{name}] ASSIGNED partitions: {sorted(p.partition for p in partitions)}")
    cons = Consumer({**common, "group.id": GROUP, "auto.offset.reset": "earliest"})
    cons.subscribe([TOPIC], on_assign=on_assign)
    while not stop_evt.is_set():
        cons.poll(0.3)
    cons.close()
    print(f"[{name}] left the group")

stopA, stopB = threading.Event(), threading.Event()

# 1) Consumer A joins alone -> should own all partitions (6 here)
threading.Thread(target=run_consumer, args=("A", stopA), daemon=True).start()
time.sleep(5)

# 2) Consumer B joins -> rebalance: partitions split across A and B
print("\n--- Consumer B joins ---")
threading.Thread(target=run_consumer, args=("B", stopB), daemon=True).start()
time.sleep(6)

# 3) Consumer B leaves -> rebalance: A reclaims all partitions
print("\n--- Consumer B leaves ---")
stopB.set()
time.sleep(6)

stopA.set()
time.sleep(2)
print("\ndone")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * **A alone owns `[0, 1, 2]`.** A **consumer group** shares the partitions of a topic among its members.
# MAGIC   With one member, that member gets every partition.
# MAGIC * **B joins → a `rebalance` fires** and the assignment splits (e.g. A `[0,1]`, B `[2]`). The **group
# MAGIC   coordinator** (a broker) detects the membership change and re-distributes partitions so each is owned
# MAGIC   by exactly **one** consumer in the group. That one-owner rule is what preserves per-partition order
# MAGIC   while still scaling out.
# MAGIC * **B leaves → another rebalance** and A reclaims `[0, 1, 2]`. Leaving is detected either cleanly
# MAGIC   (`close()`) or by a missed **heartbeat** past `session.timeout.ms`.
# MAGIC * **Parallelism is capped by partition count.** A 4th consumer in this group would get an **empty**
# MAGIC   assignment and sit idle — you can never have more *working* consumers than partitions. This is the
# MAGIC   single most important sizing rule: **#partitions sets your maximum consumer parallelism** (drives D-/SC1 sizing).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Challenge 2 — Consumer-Group Parallelism  *(≈2 min)*
# MAGIC `topic_0` has **6 partitions**. You launch a group with **7 consumers** (one more than partitions).
# MAGIC Predict: how many actively receive partitions, and how many sit **idle**? (a few seconds to rebalance)

# COMMAND ----------

# Quick Challenge 2 — one more consumer than partitions -> exactly one idle
import threading, time
from confluent_kafka import Consumer
nparts = len(admin.list_topics(timeout=10).topics[TOPIC].partitions)
nconsumers = nparts + 1
print(f"{TOPIC} has {nparts} partitions; launching {nconsumers} consumers...")
qc2_assign = {}
def qc2(name, stop):
    def on_assign(_, parts):
        qc2_assign[name] = sorted(p.partition for p in parts)
    cons = Consumer({**common, "group.id": "qc2_group", "auto.offset.reset": "latest"})
    cons.subscribe([TOPIC], on_assign=on_assign)
    while not stop.is_set():
        cons.poll(0.3)
    cons.close()
stops = [threading.Event() for _ in range(nconsumers)]
for i in range(nconsumers):
    threading.Thread(target=qc2, args=(f"C{i}", stops[i]), daemon=True).start()
time.sleep(8)
for s in stops:
    s.set()
time.sleep(2)
print("assignments:", qc2_assign)
print("idle consumers (no partitions):", [n for n, parts in qc2_assign.items() if not parts])

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer (reveal after attempting)**
# MAGIC * **6 consume, exactly 1 idle.** A partition is owned by **at most one** consumer in a group, so the
# MAGIC   number of working consumers can never exceed the partition count — the extra consumer gets nothing.
# MAGIC * This is the hard ceiling on consumer parallelism: to use one more consumer you must **add a partition**.
# MAGIC   (If two consumers briefly show the same partition, you caught a rebalance mid-flight — let it settle.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D5 — Watch Offsets & Lag Move  *(≈4 min)*
# MAGIC First a reusable **lag helper** (replaces the `kafka-consumer-groups --describe` CLI).
# MAGIC **Predict:** "what lag will we see right after a 200-event burst?" (≈200, on top of whatever
# MAGIC the live connector has added).

# COMMAND ----------

from confluent_kafka import Consumer, TopicPartition

def group_lag(topic=TOPIC, group=GROUP):
    """Print committed vs end offset per partition, and total lag (CLI-free)."""
    c = Consumer({**common, "group.id": group, "enable.auto.commit": False})
    md = c.list_topics(topic, timeout=10).topics[topic]
    tps = [TopicPartition(topic, pid) for pid in md.partitions]
    committed = c.committed(tps, timeout=10)
    total = 0
    for tp in committed:
        _, end = c.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10, cached=False)
        cur = tp.offset if tp.offset and tp.offset >= 0 else 0
        lag = end - cur
        total += lag
        print(f"p{tp.partition}: committed={tp.offset}  end={end}  lag={lag}")
    c.close()
    print("TOTAL LAG:", total)
    return total

# COMMAND ----------

# Produce a burst, then watch lag fall as a consumer drains it.
for i in range(200):
    p.produce(TOPIC, value=make_order(1000 + i))
p.flush()
print("burst of 200 JSON orders sent — now check lag:")
group_lag()                              # run D3 again, then re-run this cell; watch lag drop

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * **`end`** is the partition's **log-end offset (high-watermark)** — where the next produced record will
# MAGIC   go. The burst (plus the live connector) pushed `end` up.
# MAGIC * **`committed`** is your group's **bookmark** — the last offset it confirmed it has processed.
# MAGIC * **`lag = end − committed`** is therefore literally *"how many records have been produced that this
# MAGIC   group hasn't consumed yet."* Right after the burst, lag jumps by ≈200.
# MAGIC * **Lag is per-partition first.** Total lag hides skew: one hot partition can lag while others are at zero.
# MAGIC   Always read the per-partition lines, not just the total.
# MAGIC * **Why this is the key health metric:** lag turns "are we keeping up?" into one number. Re-run D3 to
# MAGIC   consume + commit, then re-run this cell — committed advances toward end and lag drops. Lag that *falls
# MAGIC   or holds steady* = healthy; lag that *climbs* = consumers are slower than producers (backpressure).

# COMMAND ----------

# MAGIC %md
# MAGIC ## D6 — Turn On Idempotence  *(≈3 min)*
# MAGIC **Pair-discuss:** "which earlier failure does `enable.idempotence` remove?" (producer-retry duplicates).
# MAGIC **Predict:** "does the consumer output look any different?" (no — the guarantee is behind the scenes).

# COMMAND ----------

from confluent_kafka import Producer

p2 = Producer({
    **common,
    "enable.idempotence": True,          # de-duplicate retries (also preserves order)
    "acks": "all",                       # wait for all in-sync replicas
})
for i in range(5):
    p2.produce(TOPIC, key="cust-0", value=make_order(2000 + i))
p2.flush()
print("sent 5 JSON orders with idempotence + acks=all")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * **The output looks identical to D2 — and that's the point.** Idempotence fixes a failure you can't see
# MAGIC   in the happy path. Without it, a producer whose write *succeeds but whose ack is lost* will **retry**
# MAGIC   and write the record **twice** (a duplicate, i.e. at-least-once at the producer).
# MAGIC * **How the broker dedupes:** with `enable.idempotence=True` the producer gets a **producer id (PID)** and
# MAGIC   stamps each record with a **monotonic sequence number** per partition. The leader remembers the last
# MAGIC   sequence it accepted and **silently drops a re-sent duplicate** — so a retry is safe.
# MAGIC * **`acks=all`** ties in the durability half: the leader waits for all **in-sync replicas** before
# MAGIC   acknowledging, so an acknowledged record survives a broker failure. Idempotence (no dupes) + `acks=all`
# MAGIC   (no loss) is **effectively-once *into* Kafka**.
# MAGIC * **End-to-end exactly-once** additionally needs **transactions** on the producer and a transaction-aware
# MAGIC   consumer/sink (`read_committed`) — that's the next slide, and why Structured Streaming + Delta matters in M2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D7 — Read Lag as a Health Signal  *(≈3 min)*
# MAGIC One number you could alert on. Because the Datagen connector keeps producing, expect a
# MAGIC **small, steady** lag rather than exactly 0.
# MAGIC **Pair-discuss:** "lag = 5 steady vs lag = 5 and rising — which is a problem?" (the rising one).

# COMMAND ----------

total = group_lag()
print("ALERT!" if total > 100 else "healthy", "— total lag =", total)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * **A non-zero "healthy" lag is normal here.** The connector adds ~1 record/sec, so between your polls
# MAGIC   there are always a few un-consumed records. Absolute lag matters less than its **trend** and whether it
# MAGIC   stays inside your latency budget.
# MAGIC * **Lag is a derivative of two rates:** it grows when *production rate > consumption rate* and shrinks
# MAGIC   when consumers catch up. A steadily climbing lag means you are under-provisioned — add consumers (up to
# MAGIC   the partition count) or speed up processing.
# MAGIC * **This is the bridge to an SLA.** "Lag in records ÷ consumption rate ≈ time-behind-real-time." Alert on
# MAGIC   *rising* lag (a symptom), not on every blip — the alert-fatigue lesson we develop in M6.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Challenge 3 — Lag vs SLA  *(≈2 min)*
# MAGIC Producers send **120 records/sec**; your consumers process **100 records/sec**; the SLA is **5 s**.
# MAGIC Predict: is lag **stable, rising, or falling**? Then compute how long until you breach the SLA from lag 0.

# COMMAND ----------

# Quick Challenge 3 — is the pipeline keeping up?
prod_rate, cons_rate, sla_s = 120, 100, 5
net    = prod_rate - cons_rate            # lag growth per second
budget = cons_rate * sla_s                # max tolerable lag for a 5s SLA
print(f"lag changes by {net:+d} records/sec")
print(f"SLA lag budget = {budget} records (= cons_rate x SLA)")
if net > 0:
    print(f"time to breach SLA from lag 0: ~{budget/net:.0f} sec")
else:
    print("lag stable or falling -> within SLA")

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer (reveal after attempting)**
# MAGIC * **Lag rises** at 20 records/sec because production (120) > consumption (100). Lag is the difference
# MAGIC   of the two rates accumulating over time.
# MAGIC * The 5 s SLA tolerates ~500 records of lag, so you breach in **~25 s**. Fixes: add consumers (up to
# MAGIC   the 6-partition cap) or speed up per-record processing — the health signal from D5/D7.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # PART B — PARTICIPANT LAB
# MAGIC # ════════════════════════════════════════════════
# MAGIC Work in pairs. You reuse `common`, `make_order` and `group_lag` from the setup/demo cells above,
# MAGIC so run Part A first (or at least the two setup cells). **Use your own group id** so your offsets
# MAGIC don't collide with anyone else's. After each step, say *out loud* which Kafka mechanism produced
# MAGIC the result you see — that explanation is the graded part, not the code.

# COMMAND ----------

# Give your pair a unique name so your consumer group + delivery prints are yours alone.
PAIR      = "team1"                       # TODO: change to your team name
LAB_GROUP = f"lab_{PAIR}"
print("Lab group:", LAB_GROUP)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAB 0 — Explore the Cluster  *(warm-up, ≈15 min)*
# MAGIC Get your bearings before building. Fill the `# TODO`s and run.
# MAGIC **Success criteria:** you can state how many brokers exist, how many partitions `topic_0` has, and
# MAGIC read 5 records reporting each one's partition / offset / key.

# COMMAND ----------

# L0.1 — How many brokers are in your cluster?
from confluent_kafka.admin import AdminClient
admin = AdminClient(common)
md = admin.list_topics(timeout=10)
print("broker ids:", sorted(md.brokers))      # TODO: how many brokers? note their host:port below
for bid, b in sorted(md.brokers.items()):
    print(f"  broker {bid}: {b.host}:{b.port}")

# COMMAND ----------

# L0.2 — Describe topic_0: how many partitions, and who leads each?
t = md.topics[TOPIC]
print(f"{TOPIC}: {len(t.partitions)} partitions")
for pid, pm in sorted(t.partitions.items()):
    print(f"  partition {pid}: leader=broker {pm.leader}  replicas={pm.replicas}  isr={pm.isrs}")
# TODO: is every partition's ISR equal to its replica list? what would it mean if not?

# COMMAND ----------

# L0.3 — Read 5 records and report each one's address + key
from confluent_kafka import Consumer
c = Consumer({**common, "group.id": f"lab0_{PAIR}", "auto.offset.reset": "earliest"})
c.subscribe([TOPIC])
read = 0
while read < 5:
    m = c.poll(2.0)
    if m and not m.error():
        print(f"  partition={m.partition()}  offset={m.offset()}  key={m.key()}")
        read += 1
c.close()
# TODO: which partitions did your 5 records come from? did any two share a key?

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAB 1 — Build a Producer & Consumer  *(90 min)*
# MAGIC **Goal:** produce and consume your own orders; see how the **key** controls partitioning, run a
# MAGIC two-consumer group, and read lag.
# MAGIC **Success criteria:** (1) same key always maps to the same partition; (2) a 2-consumer group
# MAGIC splits the partitions across two consumers; (3) you can report your group's total lag.

# COMMAND ----------

# L1.1 — Produce 30 orders keyed by REGION. Same region key -> same partition (read the delivery report).
from confluent_kafka import Producer
lp = Producer(common)

regions = ["north", "south", "east"]
seen = {}
def lab_report(err, msg):
    if err:
        print("delivery error:", err); return
    region = msg.key().decode()
    seen.setdefault(region, set()).add(msg.partition())

for i in range(30):
    region = regions[i % 3]               # TODO: try keying by something else and re-observe
    lp.produce(TOPIC, key=region, value=make_order(5000 + i), on_delivery=lab_report)
lp.flush()
for region, parts in sorted(seen.items()):
    print(f"key={region!r} -> partition(s) {sorted(parts)}   (expect exactly one)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * Each region key maps to **exactly one partition** across all 30 sends — confirming the
# MAGIC   `hash(key) % num_partitions` rule from D2. Three keys may land on two or three partitions (a hash can
# MAGIC   collide two keys onto the same partition); that is expected and worth pointing out.
# MAGIC * **Implication:** if you needed *all* regions strictly ordered together you'd need a single partition
# MAGIC   (no parallelism). Keying by region buys you **order within a region** while still spreading load — the
# MAGIC   real-world trade-off between ordering and throughput.

# COMMAND ----------

# L1.2 — Consume YOUR group and count how many records you read per partition.
from confluent_kafka import Consumer
from collections import Counter

c = Consumer({**common, "group.id": LAB_GROUP, "auto.offset.reset": "earliest"})
c.subscribe([TOPIC])
counts = Counter()
try:
    for _ in range(40):                   # read ~40 records then stop
        m = c.poll(2.0)
        if m and not m.error():
            counts[m.partition()] += 1
finally:
    c.close()
print("records read per partition:", dict(sorted(counts.items())))

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * Your single consumer owns **all** of the topic's partitions, so records may come from any of them.
# MAGIC   Which partitions appear (and how many from each) depends on what was produced during your poll
# MAGIC   window — you may see one busy partition or several.
# MAGIC * Because you used a **fresh `group.id`** with `earliest`, you started from the beginning of each
# MAGIC   partition. Re-running resumes from your committed offset (the D3 lesson, now in your own group).

# COMMAND ----------

# L1.3 — Run TWO consumers in the same group and watch the partitions split between them.
import threading, time
from confluent_kafka import Consumer

def lab_consumer(name, stop_evt):
    def on_assign(_, partitions):
        print(f"[{name}] assigned {sorted(p.partition for p in partitions)}")
    cons = Consumer({**common, "group.id": LAB_GROUP, "auto.offset.reset": "earliest"})
    cons.subscribe([TOPIC], on_assign=on_assign)
    while not stop_evt.is_set():
        cons.poll(0.3)
    cons.close()

s1, s2 = threading.Event(), threading.Event()
threading.Thread(target=lab_consumer, args=("L1", s1), daemon=True).start(); time.sleep(4)
print("--- second consumer joins ---")
threading.Thread(target=lab_consumer, args=("L2", s2), daemon=True).start(); time.sleep(6)
s1.set(); s2.set(); time.sleep(2)
print("done — you should have seen the partitions shared across L1 and L2")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * L1 starts owning `[0,1,2]`; when L2 joins, the **group coordinator triggers a rebalance** and the
# MAGIC   the topic's partitions are divided so each is owned by exactly one of L1/L2 (e.g. `[0,1,2]` and `[3,4,5]`).
# MAGIC * This is horizontal scale-out in action: adding a consumer to the **same group** adds throughput, up to
# MAGIC   the partition-count ceiling. With 6 partitions, a 7th consumer would sit idle.
# MAGIC * Note both consumers share `LAB_GROUP` — *same group = split the work*. If they had **different** group
# MAGIC   ids they would each read **all** partitions independently (fan-out, like D3 vs another tool).

# COMMAND ----------

# L1.4 — Report your group's lag (your success-criteria checkpoint).
group_lag(group=LAB_GROUP)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * This is `end − committed` per partition for **your** group. If you consumed in L1.2/L1.3, committed has
# MAGIC   advanced and lag is small; the connector keeps `end` creeping up, so a small steady lag is correct.
# MAGIC * Checkpoint question to answer aloud: *which partition has the most lag, and why?* (Hint: whichever your
# MAGIC   keyed burst or the connector favoured.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAB 2 — Simulate Failure Recovery  *(45 min)*
# MAGIC **Goal:** see how **commit timing** decides loss vs duplicates, and that a consumer-group
# MAGIC rebalance recovers with no loss.
# MAGIC **Success criteria:** explain, from your output, why commit-before-processing risks **loss** and
# MAGIC commit-after-processing risks **duplicates**.

# COMMAND ----------

# L2.1 — Manual commit AFTER processing = at-least-once. A crash before commit -> reprocess (duplicate), never loss.
from confluent_kafka import Consumer

def drain_n(n, commit_after_process):
    c = Consumer({**common, "group.id": f"{LAB_GROUP}_recovery",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    c.subscribe([TOPIC])
    read = 0
    try:
        while read < n:
            m = c.poll(2.0)
            if not (m and not m.error()):
                continue
            if not commit_after_process:
                c.commit(m, asynchronous=False)     # commit BEFORE processing -> crash here loses it
            # ---- "process" the record ----
            read += 1
            if commit_after_process:
                c.commit(m, asynchronous=False)     # commit AFTER processing -> crash here reprocesses it
    finally:
        c.close()
    return read

print("processed (commit-after):", drain_n(5, commit_after_process=True))
print("Re-run this cell: at-least-once may reprocess the last record on recovery — duplicates, never loss.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * The **commit** is what moves the group's bookmark. The only question that decides your delivery
# MAGIC   semantic is **when** you commit relative to processing:
# MAGIC   * **Commit AFTER processing (used here) = at-least-once.** If the consumer crashes between processing
# MAGIC     and committing, the record is re-read on restart → a **duplicate**, but **never lost**.
# MAGIC   * **Commit BEFORE processing (`commit_after_process=False`) = at-most-once.** A crash after the commit
# MAGIC     but before processing means the offset moved past a record you never handled → **silent loss**.
# MAGIC * **`enable.auto.commit=False`** is what gives you this control. Auto-commit ticks on a timer regardless of
# MAGIC   whether your processing succeeded — convenient, but it hides exactly this loss/dup window.
# MAGIC * Flip the flag to `False` and reason about which record would vanish. The lesson: **the offset commit is
# MAGIC   the unit of delivery-semantic control**, and "exactly-once" needs the sink to dedupe (idempotent/Delta), not Kafka alone.

# COMMAND ----------

# L2.2 — Kill one consumer of a 2-consumer group mid-stream; the partitions rebalance, consumption continues.
import threading, time
from confluent_kafka import Consumer

def survivor(name, stop_evt, got):
    def on_assign(_, parts):
        print(f"[{name}] now owns {sorted(p.partition for p in parts)}")
    cons = Consumer({**common, "group.id": f"{LAB_GROUP}_failover", "auto.offset.reset": "latest"})
    cons.subscribe([TOPIC], on_assign=on_assign)
    while not stop_evt.is_set():
        m = cons.poll(0.5)
        if m and not m.error():
            got[name] = got.get(name, 0) + 1
    cons.close()

sA, sB = threading.Event(), threading.Event(); got = {}
threading.Thread(target=survivor, args=("A", sA, got), daemon=True).start()
threading.Thread(target=survivor, args=("B", sB, got), daemon=True).start()
time.sleep(6)
print("--- KILL consumer B (simulated crash) ---")
sB.set(); time.sleep(6)                  # A should rebalance to all partitions and keep reading
sA.set(); time.sleep(2)
print("records read after failover:", got, "-> A kept consuming = no loss")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it**
# MAGIC * When B is killed, the coordinator notices the missing member (failed **heartbeat** / clean leave) and
# MAGIC   **rebalances B's partitions onto A** — you'll see A's `now owns …` line grow to all of the topic's partitions.
# MAGIC * A's record count **keeps climbing after** the kill: no records were lost because B's partitions are
# MAGIC   simply reassigned and consumed from the **last committed offset**. This is why Kafka consumers are
# MAGIC   fault-tolerant *as a group* — failure recovery is just a rebalance plus offset resume.
# MAGIC * Trade-off to name: during the brief rebalance window, those partitions pause (a few seconds). That pause
# MAGIC   is part of your latency budget — relevant to the SLA sizing next.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario SC1 — Size an Ingestion Tier  *(design exercise)*
# MAGIC **Brief:** design a Kafka ingestion tier for **20,000 events/sec** with an end-to-end **SLA < 5s**.
# MAGIC Decide partition count, replication, and consumer count, then justify it. Edit the assumptions
# MAGIC below and defend your numbers — there is no single right answer, only a defensible one.

# COMMAND ----------

import math

target_eps        = 20_000     # required throughput (events/sec)
per_partition_eps = 5_000      # TODO: sustained events/sec one partition+consumer can handle (benchmark!)
sla_seconds       = 5
headroom          = 1.5        # TODO: safety margin for spikes / rebalances

partitions = math.ceil(target_eps / per_partition_eps * headroom)
consumers  = partitions        # one consumer per partition is the max useful parallelism

print(f"target            : {target_eps:,} events/sec, SLA < {sla_seconds}s")
print(f"per-partition rate : {per_partition_eps:,} events/sec  (with {headroom}x headroom)")
print(f"=> partitions      : {partitions}")
print(f"=> consumers       : {consumers}  (>{partitions} would sit idle)")
print(f"=> replication     : 3, min.insync.replicas=2  (durability)")
print(f"=> producer        : acks=all, enable.idempotence=True")
print("Latency check: keep per-partition lag well under "
      f"{per_partition_eps * sla_seconds:,} events to stay within the {sla_seconds}s SLA.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the output — the mechanism behind it (defend these numbers)**
# MAGIC * **Partitions = parallelism.** `ceil(target ÷ per-partition rate × headroom)` falls straight out of D4:
# MAGIC   you can't consume faster than you have partitions, so partitions are sized to the throughput target plus
# MAGIC   headroom for spikes and the rebalance pauses you saw in L2.2.
# MAGIC * **Consumers ≤ partitions.** More consumers than partitions just sit idle (D4), so the useful consumer
# MAGIC   count equals the partition count.
# MAGIC * **Replication 3 / `min.insync.replicas` 2** comes from D1 + D6: enough copies to survive one broker loss
# MAGIC   while still accepting `acks=all` writes (no loss).
# MAGIC * **The SLA is a lag budget.** `per_partition_rate × SLA` is the maximum tolerable per-partition lag before
# MAGIC   you breach 5s; the operational job (M6) is to alert when lag trends toward it.
# MAGIC * **Now stress-test your assumption:** the whole design hinges on `per_partition_eps`. Lower it (a slow
# MAGIC   consumer / fat records) and partitions rise; that is the number you must justify with a real benchmark.

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════════════════════════════════════════════
# MAGIC # Cleanup — run between cohorts
# MAGIC # ════════════════════════════════════════════════
# MAGIC **Do not delete `topic_0` while the Datagen connector is still running** — the connector
# MAGIC targets it. To reset cleanly: pause/delete the connector first, then optionally delete the
# MAGIC topic. Skip all of this during class.

# COMMAND ----------

# Uncomment only after pausing the Datagen connector:
# admin.delete_topics([TOPIC])
# print("deleted", TOPIC)