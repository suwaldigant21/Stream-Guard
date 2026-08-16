# Stream Guard — Real-Time Fraud Detection Pipeline (PaySim)

An end-to-end fraud-detection pipeline that takes a modeling approach
**validated on a 24K-row academic sample** (BSc Machine Learning coursework,
R/caret — 5-fold CV, ROSE balancing, Random Forest at 0.9275 AUC), **scales it
to the full 6.3M-row PaySim dataset**, and serves it as a genuinely real-time
scoring system: a FastAPI vendor feed streams synthetic payments into Redpanda, PySpark
Structured Streaming lands them in a partitioned Bronze lakehouse on S3, dbt
builds Silver/Gold on Athena, an XGBoost model is trained on the Gold feature
store, and a second streaming consumer scores every event in flight, emits
CloudWatch metrics, and raises SNS fraud alerts — all against a documented
**GDPR Article 17 right-to-erasure** layer.

This README documents what was actually built, why each decision was made, and
the real bugs — including **Bronze data that silently vanished from S3 twice**
and **a green model build that would have shipped a degenerate threshold** —
that shaped the final design. Every number below was verified against real
pipeline output, not estimated.

> **Data source:** the public, academic **PaySim** mobile-money simulator
> (Lopez-Rojas, Elmir & Axelsson). 100% synthetic — no real customer data is
> used or was ever accessible anywhere in this pipeline.

---

## Table of Contents
1. [The Problem](#1-the-problem)
2. [Architecture](#2-architecture)
3. [Key Results](#3-key-results)
4. [Tech Stack](#4-tech-stack)
5. [Repository Structure](#5-repository-structure)
6. [The Data](#6-the-data)
7. [Streaming Ingestion — Redpanda + PySpark](#7-streaming-ingestion--redpanda--pyspark)
8. [Bronze → Gold — Terraform + Athena + dbt](#8-bronze--gold--terraform--athena--dbt)
9. [The Model — XGBoost](#9-the-model--xgboost)
10. [Real-Time Scoring, Alerts & Dashboard](#10-real-time-scoring-alerts--dashboard)
11. [GDPR Article 17 — Right to Erasure](#11-gdpr-article-17--right-to-erasure)
12. [Validation & Results](#12-validation--results)
13. [Engineering War Stories](#13-engineering-war-stories)
14. [Security & Cost Notes](#14-security--cost-notes)
15. [How to Reproduce](#15-how-to-reproduce)
16. [Roadmap](#16-roadmap)

---

## 1. The Problem

Fraud in mobile-money networks is extremely rare (~0.13% of PaySim
transactions) and **time-concentrated** — which makes it the ideal domain for
an honest end-to-end engineering story, because every naive shortcut is
visible:

- A **random** train/test split is optimistic: fraud clusters in time, and the
  late test window holds **52% of all fraud**.
- A **default 0.5 decision threshold** on a rare-positive class produces
  garbage precision (30,545 false positives on the baseline model).
- **Class imbalance** (1:560) makes raw accuracy meaningless and demands
  `scale_pos_weight` + PR-curve thresholding rather than accuracy tuning.
- Detecting fraud **at streaming speed** — not in a batch report — requires
  the scoring model to run inside the ingestion path with a bounded,
  past-only feature window, which is where almost all of the interesting bugs
  in this project lived.

The pipeline's design goal: a model that catches the majority of fraudulent
transactions with near-zero false positives, served live on the streaming
path, with erasure and observability treated as first-class requirements — not
afterthoughts.

---

## 2. Architecture

> 📸 **Diagram placement:** the draw.io architecture diagram lives at
> [`docs/architecture-diagram.png`](docs/architecture-diagram.png) and should be
> shown here, before the ASCII version below (the ASCII diagram is accurate and
> can stand alone if the image is ever missing). The live CloudWatch dashboard
> panels — workload, heartbeat, and alarm status — are referenced in §10.

```
 FEED                  STREAMING                    BRONZE                SILVER / GOLD               ML / OBSERVABILITY
┌────────────┐   ┌─────────────────────┐   ┌────────────────────┐  ┌───────────────────────┐  ┌────────────────────────┐
│ FastAPI    │   │ Redpanda (Docker)    │   │ S3 Bronze parquet   │  │ AWS Glue Data Catalog │  │ XGBoost fraud model     │
│ mock vendor│──▶│transactions-raw topic│──▶│ partitioned by type │─▶│ Athena + dbt Core      │─▶│ (Gold feature store)    │
│ feed (:8000)│  │ producer.py replay   │   │ + DQ quarantine     │  │ stg → gold CTAS        │  │                         │
└────────────┘   └─────────────────────┘   └────────────────────┘  └───────────────────────┘  │ FastAPI scoring :8001   │
                        │                                                                      │  POST /v1/predict        │
                        │ gdpr-deletion-requests (purge)                                      │                         │
                        ▼                                                                      │ PySpark consumer        │
                 ┌─────────────┐   fraud-alerts ──▶ CloudWatch metrics ──▶ SNS alerts          │  (score + fan-in state)  │
                 │ erasure sink │   ConsumerHeartbeat ──▶ dead-man's alarm                     └────────────────────────┘
                 └─────────────┘   CloudWatch dashboard (5 panels)
```

**Data flow in one sentence:** a FastAPI mock feed replays the PaySim dataset
into a Redpanda `transactions-raw` topic; a PySpark Structured Streaming job
consumes it, validates each row against a data-quality gate (rejecting to a
`transactions_dq_rejected` quarantine), and writes partitioned Bronze parquet;
dbt + Athena build Silver and Gold (window aggregates + GDPR masking); an
XGBoost model is trained on the Gold feature store and frozen to JSON; and a
second streaming consumer scores every live event through the model's FastAPI
service, feeding fan-in state back into scoring, emitting CloudWatch metrics,
and raising SNS alerts — with a parallel GDPR erasure topic.

---

## 3. Key Results

- **Production-grade bug found and fixed:** diagnosed **two unexplained
  disappearances of Bronze data from S3** using full AWS forensics
  (EventBridge, Lambda, CloudTrail, IAM) that traced the deletion to
  dbt-athena's relation-replacement semantics, not external actors — and
  redesigned the layer boundaries so it cannot happen again (§13.1).
- **Model:** PR-AUC **0.8550**, ROC-AUC **0.9956** on 8,213 fraud / 2.77M
  transactions; tuned operating point cuts false positives from **30,545 to
  77** at 73.3% recall — and the threshold is *derived*, never hardcoded
  (§9, §13.2).
- **Real-time:** 6.3M transactions streamed through Redpanda → PySpark →
  Bronze with **0 duplicates** after rebuild; a 5,000-score live alert test
  produced **4,999/5,000 correct alerts** through the real scoring service.
- **Observability:** 3 CloudWatch alarms + a 5-panel dashboard + SNS email
  alerts, including a **consumer dead-man's switch** with verified-live death
  detection (~10 min, documented latency — not assumed).
- **Privacy:** GDPR Article 17 erasure implemented and **verified on AWS**:
  erasing one account produced `ANON_fbad4ba4428e3311` at the Silver layer
  with **zero raw-identifier leaks** anywhere in the lakehouse.
- **Gates:** **66 tests pass**, `ruff check .` clean, `dbt test` PASS=8.

---

## 4. Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Streaming broker | Redpanda + Console (Docker) | Kafka-compatible, single binary, no ZooKeeper |
| Producer / mock feed | FastAPI + kafka-python | Replays PaySim into `transactions` with a persisted watermark |
| Stream processing | PySpark Structured Streaming (Spark 4) | Partitioned Parquet sink, checkpoints, DQ fork |
| Storage | Amazon S3 (Bronze/Silver/Gold) | Decoupled storage/compute, partition-friendly, cheap |
| Catalog | AWS Glue Data Catalog | Single metadata layer across all layers |
| Query engine | AWS Athena (Trino) | Serverless SQL over Parquet, pay-per-scan |
| Transform | dbt Core + dbt-athena | Versioned SQL, window aggregates, source/mart hygiene, tests |
| ML | XGBoost + scikit-learn | Tabular, `scale_pos_weight` for 1:560 imbalance |
| Serving | FastAPI + frozen `xgb.Booster` | Feature-order contract via `feature_names`, type guard |
| Observability | CloudWatch + SNS + dashboard | Metrics, alarms (incl. dead-man's switch), email alerts |
| IaC | Terraform | Reproducible S3/Glue/Athena/IAM/CloudWatch/SNS |
| Privacy | HMAC-SHA256 aliasing + append-only registry | Article 17 erasure with verifiable cascade |

---

## 5. Repository Structure

```
stream-guard/
├── .env.example                      # documented env contract — no real secrets
├── docker-compose.yml                # Redpanda broker + Console
├── producer.py                       # PaySim replay → transactions topic (persisted watermark)
├── mock_vendor_api.py                # FastAPI mock payment feed (:8000)
├── pyspark_consumer.py               # PySpark Structured Streaming → Bronze + DQ quarantine
├── streamguard_serializers.py        # shared Kafka JSON (de)serializers
├── src/
│   ├── api/main.py                   # scoring service (:8001, POST /v1/predict, OOD type guard)
│   ├── consumer/main.py              # live scoring loop, fan-in state, GDPR purge, CloudWatch
│   ├── dq.py                         # Bronze data-quality rules (required fields + balance checks)
│   └── api/gdpr.py                   # erasure helpers + HMAC-SHA256 account aliasing
├── dbt/
│   ├── profiles.yml                  # Athena connection (default credential chain)
│   └── models/
│       ├── staging/  stg_transactions.sql · sources.yml · schema.yml
│       └── marts/    gold_transactions.sql · schema.yml
├── train/
│   ├── extract_gold.py               # fraud-bearing subset pull from Athena
│   ├── temporal_split.py             # 80th-percentile time split (not random)
│   ├── train_baseline.py · train_enriched.py · tune_threshold.py · diagnostics.py
├── scripts/
│   ├── export_phase5a.py · export_phase5b.py   # freeze model + metadata + plots
│   ├── anonymize_batch_gdpr.py       # batch erasure with pepper-keyed HMAC aliasing
│   ├── publish_gdpr_erasure.py       # streaming purge publisher
│   ├── seed_gdpr_requests.py         # append-only erasure registry (Athena table)
│   └── setup_sns_subscription.ps1 · teardown.ps1
├── terraform/                        # S3, Glue, Athena, IAM, CloudWatch, SNS, dashboard
├── tests/                            # 66 tests across feed, streaming, DQ, API, GDPR, dbt, training
├── docs/                             # dashboard screenshots (see §10)
└── data/  (gitignored)               # bronze parquet, checkpoints, model artifacts, gold extracts
```

Model artifacts (`model.json`, `metadata.json`, PR/ROC/confusion/importance
PNGs) and all parquet stay **local** — `data/` is gitignored by design; the
repo contains the code, tests, and Terraform that reproduce them.

---

## 6. The Data

### 6.1 Where this started — validated on a 24K-row academic sample first

Before any of the infrastructure below existed, the modeling approach was
proven on a 24,192-row PaySim subset as university coursework (BSc Machine
Learning, R/caret): full EDA, mode/median imputation, IQR-based outlier
retention (kept as fraud signal, not removed), ROSE balancing on the training
fold only, and five models compared under 5-fold CV. Two findings from that
project carried forward directly into StreamGuard's production design:

- **Fraud only occurs in `TRANSFER` and `CASH_OUT` transaction types** — this
  is exactly why the Gold training subset here is filtered to those two types
  (§9), not a fresh discovery re-derived from scratch.
- **PCA showed heavy class overlap** — an early signal that a simple decision
  boundary wouldn't separate fraud cleanly, which is part of why the eventual
  threshold here is *derived* from the PR curve rather than assumed (§9, §13.2).

The best R model (Random Forest, mtry=4, ntree=500) reached **0.9275 AUC** on
the 24K balanced-ish sample (34% fraud). StreamGuard scales the same underlying
problem to the full, genuinely hard version: **6.3M rows at ~0.13% fraud** —
the coursework proved the modeling fundamentals; this project proves they hold
up under real class imbalance and real streaming ingestion, not just a
classroom-sized CSV.

### 6.2 The production dataset

**PaySim** simulates a mobile-money service: a user-to-user P2P transfer network
with both genuine and fraudulently-initiated transactions. It is the standard
academic benchmark for transaction fraud detection, with a real catch: all
fraud sits in only two of the five transaction types.

- **6,362,620 transactions** across `CASH_IN / CASH_OUT / DEBIT / PAYMENT /
  TRANSFER`, fully replayed end-to-end into Bronze with **0 duplicates**.
- **8,213 fraudulent transactions (~0.13%)** — all of them `CASH_OUT` or
  `TRANSFER` with a **stolen-originator** pattern (the account is typically
  drained and the destination is not the initiator).
- Real data-quality problems this pipeline had to solve, not hypothetical ones:
  - Malformed rows (missing required fields, negative amounts) must be
    **quarantined, not silently dropped** — so the data-quality gate forks them
    to a `transactions_dq_rejected` topic with a visible reason code.
  - Fraud is **time-concentrated**: the late 20% of the dataset holds 52% of
    all fraud, which is why the split is temporal, not random (§9).
  - PaySim's account identifiers are 10-digit strings — trivially guessable,
    which is exactly why erasure aliasing (§11) uses a keyed HMAC rather than
    a reversible hash.

---

## 7. Streaming Ingestion — Redpanda + PySpark

> 📸 **Screenshot placement:** the Redpanda Console (`localhost:8080`) showing
> the `transactions-raw`, `fraud-alerts`, `gdpr-deletion-requests`, and
> `transactions_dq_rejected` topics with live message counts →
> `docs/redpanda_console_topics.png`, right after this heading.

- **Producer** (`producer.py`): replays the PaySim CSV into the
  `transactions-raw` topic. A **persisted watermark** (`producer_state.json`) plus
  `maxOffsetsPerTrigger` make restart idempotent — this fixed a real incident
  where a restart from offset 0 created **~532K duplicate Bronze rows** (§13.5).
- **Consumer** (`pyspark_consumer.py`): PySpark Structured Streaming with a
  30 s processing trigger, so each partition lands as ~450–520 rows of parquet
  instead of hundreds of tiny files (S3 PUT/LIST cost + Athena metadata scan).
  Bronze is partitioned by `type`; checkpoints are on local disk.
- **Data-quality gate** (`src/dq.py`): before anything is scored or stored,
  each row is checked for the five required fields (`step, type, amount,
  nameOrig, nameDest`) and balance sanity (`amount ≥ 0`, four non-negative
  balances). Rejects fork to a **quarantine stream** with a reason-code list —
  nothing is dropped silently. DQ rules are pure Python shared by both the
  Spark job and the Python tests.
- **Scoring consumer** (`src/consumer/main.py`): the *second* consumer loop
  scores each live transaction via the FastAPI service, maintains the rolling
  fan-in feature state (past-only, 24-step window), publishes alert messages to
  `fraud-alerts`, and flushes CloudWatch metrics as **deltas per heartbeat**
  (running totals would double-count at Sum/300 s).

> **Design note:** Bronze, Silver, and Gold live in **disjoint S3 namespaces** —
> a direct consequence of the ghost-deletion incident (§13.1). A medallion
> architecture where the warehouse tool can drop the layer above it is a
> waiting data-loss bug.

---

## 8. Bronze → Gold — Terraform + Athena + dbt

> 📸 **Screenshot placement:** terminal output of `dbt run` + `dbt test`
> showing `PASS=2` models / `PASS=8` tests → `docs/dbt_run_test_output.png`,
> right after this heading.

Terraform provisions the lakehouse: AES-256-encrypted, public-access-blocked S3
buckets with a policy denying non-TLS requests, Glue Data Catalog, an Athena
workgroup (with `EnforceWorkGroupConfiguration` correctly off so CTAS output
goes to `s3_data_dir`, not the results dir), least-privilege IAM, CloudWatch
alarms, SNS, and the dashboard.

- **Bronze** is a **source**, never a dbt model — dbt output is isolated under
  `dbt_data/`. This boundary is the fix for the S3 deletion bug (§13.1).
- **Silver** (`stg_transactions.sql`): reads Bronze via the catalog, computes
  the past-only **24-step window aggregates** (`RANGE` frames partitioned by
  originator/destination — `velocity_orig_*`, `fan_in_dest_count_24h`), applies
  **GDPR masking** (erased accounts render as `ANON_…` aliases via a LEFT JOIN
  to the erasure registry), and casts types. Window semantics were
  **validated against the source** (fan-in counts reproduce; originators
  confirmed single-use).
- **Gold** (`gold_transactions.sql`): the CTAS mart that the model trains on —
  11 numeric columns, no account identifiers, `dbt test` PASS=8.

> **Honest note:** the two `velocity_orig_*` features shipped to Gold were
> later measured at **0.0 gain** (the model never split on them) and dropped,
> leaving the 11-column final Gold. One identifier feature survived —
> `fan_in_dest_count_24h` — and it is the reason Phase 7 scoring is stateful.

---

## 9. The Model — XGBoost

`train/` extracts the fraud-bearing subset (`CASH_OUT` + `TRANSFER`,
**2,770,409 rows, 8,213 fraud**) from the Gold feature store and trains in
stages, each stage catching a real bug (§13.2):

1. **Temporal split** at the 80th percentile of `step`: train 2,217,905 / test
   552,504. The test window holds 4,258 of the 8,213 frauds — a random split
   would have masked how hard the tail really is.
2. **Baseline XGBoost** with `scale_pos_weight = 559.79`, early stopping (stopped
   at iteration 18): **PR-AUC 0.8348**, ROC-AUC 0.9951 — but at the *default
   0.5 cutoff* it reports recall 1.0 with **precision 0.122 (30,545 FP)**:
   an unusable detector if you trust default thresholds.
3. **PR-curve threshold tuning:** the max-F1 operating point cut FP from
   **30,545 → 81** while keeping recall ≈ 0.73.
4. **Enrichment (5b):** the single-feature dominance of
   `error_balance_orig` (97% of gain) drove the 24-step window aggregates;
   after dropping two zero-gain features, the frozen model uses the final
   11-column Gold.

**Frozen 5b artifact** (143 trees, threshold **derived** from the PR curve):

| Metric | Value |
|---|---|
| PR-AUC / ROC-AUC | **0.8550** / 0.9956 |
| Threshold | **0.9866** (max-F1 point) |
| Precision / Recall | 0.9759 / 0.7332 |
| False positives / false negatives | **77 / 1,136** (on 552,504 test rows) |
| Max predicted probability | 0.9996 (calibrated — 5a capped at 0.81) |

**Honest boundary:** the shipped ID feature (`fan_in_dest_count_24h`) requires
a rolling per-destination window at scoring time; the API accepts it optionally
(honest prior 0.0 when omitted) and the streaming consumer feeds it from its
state store. The "carve down the 1,136 false negatives" goal only moved −14 —
the real wins are **PR-AUC +0.020** and healthy calibration.

---

## 10. Real-Time Scoring, Alerts & Dashboard

The scoring service (`src/api/main.py`) loads the frozen `model.json` +
`metadata.json` once at startup, replicates the Gold feature engineering in
exactly `metadata["features"]` order, and passes `feature_names` to the
Booster. Two correctness properties:

- **Out-of-distribution guard (400):** the frozen model was trained only on
  `CASH_OUT`/`TRANSFER`. Probing real `PAYMENT`/`DEBIT`/`CASH_IN` rows scored
  ~0.99 *both ways* — hallucinated probabilities on never-trained event types —
  so the endpoint rejects them with a clear 400 instead of silently scoring
  garbage.
- **Learned boundary, not a rule:** real PaySim *legit* rows carry large
  `error_balance_orig` too (the simulator's balances drift), so the model's
  boundary is not a simple "error ≠ 0" check — tests assert on real
  in-distribution rows only (fraud ≥ 0.90 / legit ≤ 0.05).

**Observability** (`src/consumer/main.py` → CloudWatch → SNS): three alarms —
`ScoredTransactions` and `FraudAlertCount` at 300 s Sum (deltas, not running
totals), and `ConsumerHeartbeat` as a **dead-man's switch** with
`treat_missing_data = "breaching"`. Alerting was verified live end-to-end: a
5,000-score burst of pre-verified real fraud rows through the real API produced
**4,999/5,000 alerts**.

> 📸 **Screenshot placement** — `docs/dashboard-workload.png` (scored
> transactions + fraud alerts + scorer errors), `docs/dashboard-heartbeat.png`
> (consumer liveness), and `docs/dashboard-alarm-status.png` (alarm state
> panel) → right after this section.

Two CloudWatch behaviors are documented here because they surprised us (§13.6):
a fresh metric backfills 3 pre-data periods as *missing*, briefly flaring a
false ALARM (self-heals on the first datapoint — ship a startup heartbeat), and
the alarm evaluates a range larger than `evaluation_periods`, so real death
detection was ~10 min, not 3 (verified live: death 12:28 → ALARM 12:38 →
restart → OK).

---

## 11. GDPR Article 17 — Right to Erasure

> 📸 **Screenshot placement:** terminal output of the `POST /v1/gdpr/erasure`
> request/response (202 + alias + `streaming_purge_published: true`) alongside
> the consumer log line `[GDPR] Purged … from streaming fan-in state.` →
> `docs/gdpr_erasure_verification.png`, right after this heading — this is one
> of the strongest verifiable moments in the whole project, worth a real
> screenshot rather than just the summary table in §12.

The pipeline implements **Article 17 (Right to Erasure)** via a verifiable
cascading-delete path — deliberately worded as "implements Article 17", never
"GDPR-compliant", because a deployed compliance claim is not the deliverable
here. A `POST /v1/gdpr/erasure` endpoint (`src/api/gdpr.py`, part of the
scoring service) accepts `{account_id, request_id}`, validates the PaySim
account-id format, and drives two enforcement layers, both tested on AWS:

1. **Streaming purge** (`src/api/gdpr.py` + `src/consumer/main.py`): the endpoint
   publishes to a `gdpr-deletion-requests` topic; the live consumer reads it
   and drops matching events in flight.
2. **Lakehouse masking + append-only registry** (`scripts/anonymize_batch_gdpr.py`
   + `scripts/seed_gdpr_requests.py`): an erasure request appends an immutable
   row to `gdpr_requests` (Athena), and the dbt Silver layer LEFT-JOINs it so
   any erased account's identifier renders as a **keyed HMAC alias**
   (`ANON_` + 16 hex chars of `HMAC-SHA256(pepper, account_id)`) across both
   the originator and destination columns. The alias is *derived*, so a
   re-query of the same account always returns the same `ANON_…` — verifiable
   and idempotent — while being irreversible without the pepper.

**Verified on AWS:** erasing `C0000000001` produced alias
`ANON_fbad4ba4428e3311`; a full Athena audit found the raw id present in
Bronze (the immutable raw layer, where the erasure request is the lawful basis
for retention), masked at Silver, and **zero raw-identifier leaks** downstream.
The audit log stores only aliases, never raw ids.

---

## 12. Validation & Results

**Gates:** `pytest` — **66 tests pass** (~18 s). `ruff check .` — clean.
`dbt run` PASS=2 models · `dbt test` PASS=8.

| Check | Result |
|---|---|
| Bronze rows / duplicates | 6,362,620 / **0** |
| Gold mart rows | 6,362,620 (11-column) |
| Training subset (CASH_OUT + TRANSFER) | 2,770,409 rows · 8,213 fraud |
| Temporal split | train 2,217,905 / test 552,504 (test holds 52% of fraud) |
| Frozen model | PR-AUC 0.8550 · precision 0.9759 · recall 0.7332 · FP 77 |
| Alert e2e test | 4,999 / 5,000 correct alerts via real API |
| GDPR erasure audit | alias `ANON_fbad4ba4428e3311` · 0 raw-id leaks |
| DQ gate | quarantine fork with reason codes; smoke test incl. negative-amount row |
| Cost standing | 3 CloudWatch alarms + SNS + dashboard ≈ **$0.30/mo** |

---

## 13. Engineering War Stories

The value of this project is as much in what broke and got caught as in what
worked the first time.

### 13.1 Ghost deletions in S3 Bronze — a production-grade bug found and fixed

Synced Bronze data **vanished from S3 twice**. Before suspecting our own tooling,
the full forensics checklist was run: EventBridge rules, Lambda functions,
CloudTrail data events, S3 bucket policies, and IAM roles — **no automation
anywhere was deleting objects**. The two vanishings were eventually traced to
**dbt-athena's relation-replacement semantics**: when dbt replaces a relation
it owns, it deletes the target S3 directory before recreating it, and
`bronze_transactions` had been defined as a *model* whose external location
collided with the manual table's prefix. dbt considered the layer it was about
to overwrite to be its own output — so a routine `dbt run` silently wiped the
immutable raw layer. The fix had two parts: **Bronze is now a Source, never a
model**, and **dbt output is isolated under its own `dbt_data/` prefix**.
**The lesson that shaped the rest of this project: warehouse tooling that holds
DDL powers can delete data you think is immutable — give every medallion layer
a disjoint S3 namespace and treat "our tool couldn't have done that" as a
hypothesis to disprove, not a conclusion.**

### 13.2 The green build that silently produced a degenerate model

Two linked bugs, both invisible to exit codes and loss curves, were caught by
probing the *artifact* before freezing it. First, **off-by-one trees**: XGBoost's
`best_iteration` is 0-indexed, so locking `n_estimators=18` (when the early-stop
was at iteration 18) produced a model whose max probability was 0.799 and that
flagged **0 true positives** — the fix was 19 trees. Second, the **hardcoded
threshold bug**: the float literal `0.8046` sat ~1e-4 above the true max-F1
PR-curve point (`0.8045585…`), silently dropping 102 true positives. The
threshold is now *derived* from `precision_recall_curve`, never hardcoded.
**A green build is not proof of correctness — validate the semantics of the
artifact you actually ship, not the exit code of the job that made it.**

### 13.3 Windows ephemeral-port exhaustion (`WinError 10048`)

The first end-to-end run scored ~6.3M transactions by opening **one fresh TCP
connection per score**. At high throughput the OS exhausted its dynamic port
range: **6,846 connection failures in ~30 s**. The fix was a module-level
`requests.Session()` keep-alive pool reused across all scoring calls. The rule
is now explicit: **any per-event HTTP caller must reuse a Session** — connection
management is a throughput concern, not a plumbing detail.

### 13.4 The SNS email-subscription trap

The SNS email subscription kept **vanishing within seconds of creation**. A
security scanner on the receiving mail service was **pre-fetching the
"Unsubscribe" link** in the confirmation email, which — because SNS auto-confirms
unsubscribe on a valid link — silently purged the subscription before the real
confirm step completed. Terraform's `protocol = "email"` can't track
confirmation state (the API is split-brained on confirmation), leaving state
drift. An SNS→SQS probe proved the *account* was fine. The fix: create the
subscription out-of-band and confirm it with
`aws sns confirm-subscription --subscription-arn <arn> --token <token>` —
**never click the link**. (CLI gotcha: the flag is `--notification-endpoint`.)

### 13.5 Duplicates from an unsafe replay

A consumer restart re-created **~532K duplicate Bronze rows** because the
producer replayed from offset 0 and Spark's checkpoint re-appended them.
Fixed with a persisted producer watermark plus `maxOffsetsPerTrigger`; the
rebuild was verified at **6,362,620 rows / 0 duplicates** — the count, not the
exit code, was the proof.

### 13.6 CloudWatch: false alarms and honest detection latency

A freshly-emitted metric backfills its **first 3 evaluation periods as
"missing"**, briefly flaring a false ALARM+email before the first datapoint
arrives — self-healing, but the fix is to ship a **startup heartbeat**. And the
alarm's evaluation window is larger than `evaluation_periods`, so true
dead-consumer detection was **~10 minutes, not 3** — verified live
(death 12:28 → ALARM 12:38), then documented rather than tuned to a lie.

---

## 14. Security & Cost Notes

- **Data discipline:** raw account identifiers never leave Bronze in model
  features; Gold is 11 numeric columns with no identifiers; the GDPR audit log
  stores only HMAC aliases.
- **S3 hardening (Terraform):** server-side AES-256 encryption, full public
  access blocks, bucket policy denying any non-TLS request.
- **Secrets:** all real keys live in `.env` (gitignored); `.env.example` ships
  documented placeholders only; a full credential scan of every file staged
  for commit is clean (no AWS access keys, no private keys, no account number
  in committed files).
- **Known gap, stated honestly:** the GDPR aliasing pepper has a **dev default
  in code** (used only when `STREAMGUARD_GDPR_PEPPER` is unset) — fine for a
  portfolio, but it must move to an env var / Secrets Manager before any real
  deployment, and HMAC-SHA256 is *pseudonymization* (reversible with the
  pepper), not irreversible anonymization.
- **Cost:** fully serverless in production shape; the standing AWS footprint is
  intentionally tiny — 3 CloudWatch alarms + one SNS topic + one dashboard,
  ≈ **$0.30/mo**.

---

## 15. How to Reproduce

```bash
# 0. Prereqs: Docker (Redpanda), Java 21, uv; AWS profile + Terraform (optional,
#    only needed for the Athena/GDPR/observability layers).

# 1. Streaming (local): broker + console
docker compose up -d
uv run rpk topic create transactions fraud-alerts gdpr-deletion-requests transactions_dq_rejected

# 2. Ingest: replay PaySim into the transactions topic
uv run producer.py

# 3. Bronze: PySpark Structured Streaming consumer (partitioned parquet + DQ quarantine)
uv run pyspark_consumer.py

# 4. Warehouse + model features (needs AWS): provision + dbt
cd terraform && terraform apply
cd ../dbt && dbt run && dbt test

# 5. Model (local): extract → temporal split → train → freeze
uv run train/extract_gold.py
uv run train/temporal_split.py
uv run train/train_enriched.py
uv run scripts/export_phase5b.py

# 6. Serve + score live: scoring API + streaming consumer + observability
uv run uvicorn src.api.main:app --port 8001
uv run src/consumer/main.py

# 7. Gates
uv run pytest -q && uv run ruff check .
```

---

## 16. Roadmap

- Move the GDPR aliasing pepper to AWS Secrets Manager (closes the §14 known gap).
- Add Iceberg table versioning to Bronze so raw-layer erasure is an `UPDATE`
  over history rather than a documented retention carve-out.
- Formal CI: pytest + `dbt test` gating every pull request.
- Datadog/richer dashboards only if streaming moves to AWS (deliberately
  deferred — CloudWatch + SNS already meets the alerting goal).

## Known Limitations & Planned V2

- **State store**: current fan-in tracking uses in-memory state, reset on
  consumer restart. Production version would use Spark's `mapGroupsWithState`
  for durable, checkpointed state.
- **GDPR erasure**: current implementation masks PII at the Silver query
  layer (view-based); a stronger implementation would migrate Bronze to
  Apache Iceberg for genuine physical row deletion, not just downstream masking.
- **Threshold selection**: current threshold is derived from max-F1 on the
  PR curve; a production system would use an asymmetric cost matrix
  (cost of false negative vs. false positive) rather than treating both
  error types as equally weighted.

PRs welcome if anyone wants to take a swing at any of these.