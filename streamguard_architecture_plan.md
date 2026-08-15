# StreamGuard — Architecture & Cost Ceiling
*Written before any code, per the lesson from the destroyed RAG project.*

---

## 1. What this project proves

A real-time-style fraud detection pipeline that scales a validated modeling
approach (university coursework, R, 24K-row PaySim sample) up to the full
6.3M-row PaySim dataset, using genuine streaming ingestion, hybrid local/cloud
processing, and a verifiable GDPR Article 17 (Right to Erasure) endpoint.

**Not claiming:** GDPR compliance, HIPAA anything, production-scale (180M+
row) throughput. **Claiming:** an architecture designed to scale
horizontally without redesign, demonstrated correctly at 6.3M rows.

---

## 2. Architecture

```
PaySim CSV (6.3M rows, local)
        │
        ▼
FastAPI mock vendor feed (reused pattern from RiskLens — API key auth, pagination)
        │
        ▼
Redpanda (Docker, local, $0) — producer streams transactions onto a topic
        │
        ▼
PySpark Structured Streaming (Docker, local, $0 — NOT AWS Glue this time)
  — consumes stream, validates/cleans, writes partitioned Parquet to S3 Bronze
        │
        ▼
S3 Bronze (AWS — small storage cost only)
        │
        ▼
dbt Core + [Snowflake OR Athena — decide at warehouse phase, see §4]
  — staging → intermediate → fact_transactions / dim_accounts
        │
   ┌────┴─────┐
   ▼          ▼
XGBoost      GDPR Article 17 erasure endpoint (FastAPI/Lambda)
fraud model   — cascading delete across S3 + warehouse-registered tables
(local training,
scale_pos_weight
for real ~0.1%
imbalance)
        │
        ▼
CloudWatch + SNS on the streaming job (V1) → Datadog fast-follow once stable
```

---

## 3. Dataset decision — locked

**Full PaySim, 6.3M rows.** Not IBM AML (180M rows) — protects the fixed
$3–5 AWS budget across the many debugging iterations this project will
genuinely need, the same way every RiskLens bug fix required re-running
queries. Bigger dataset = same debugging process, higher cost per iteration.

**R project narrative (inclusion confirmed):** "Validated the modeling
approach on a 24K-row academic subset first — identified fraud occurs
exclusively in TRANSFER/CASH_OUT types, with amount and transaction type as
dominant features — then scaled the same approach to the full 6.3M-row
dataset with real streaming ingestion. Architecture is designed to scale
horizontally (e.g. to IBM AML's 180M+ rows) without redesign."

---

## 4. Warehouse decision — LOCKED: Athena

**Decision (2026-08-12): Athena** — already known (no new trial-clock risk),
pay-per-scan, and the plan's default. Snowflake trial clock stays untouched.

**Path:** prove the S3 sink against **minIO** locally first ($0) → apply
Terraform (S3 + Glue catalog + Athena) → dbt Gold layer reads Bronze via
Athena. Everything behind `terraform destroy` per session, within the $3–5 cap.

---

## 5. Cost model

| Component | Cost | Teardown policy |
|---|---|---|
| Redpanda (Docker) | $0 | Keep running — local |
| PySpark Structured Streaming (Docker, local) | $0 | Keep running — local |
| Datadog (fast-follow) | $0 (free tier) | Keep running — separate billing |
| S3 Bronze/Gold | Small (~cents) | **Destroy after each work session** |
| Athena or Snowflake queries | Small (pay-per-scan / trial credits) | **Destroy AWS resources; Snowflake trial self-expires** |
| Lambda (GDPR endpoint) | Free tier | **Destroy after each session** |

**Hard ceiling: $3–5 total, AWS credits only, no top-up.** `terraform
destroy` after every work session on anything touching AWS. Local/Docker
tools stay up between sessions since they cost nothing.

---

## 6. Compliance framing — locked language

Never "GDPR-compliant." Always: **"Implements GDPR Article 17 (Right to
Erasure) via a verifiable cascading-delete endpoint across S3 and
warehouse-registered tables."** Specific, technical, defensible — no legal
claim implied.

---

## 7. Build order

1. ✅ This document.
2. FastAPI mock feed + Redpanda producer — one transaction flowing end-to-end.
3. PySpark Structured Streaming consumer → S3 Bronze.
4. **Warehouse decision point** (§4) → dbt Gold layer (`fact_transactions`, `dim_accounts`).
5. XGBoost fraud model, real severe-imbalance handling (`scale_pos_weight`), local training.
6. GDPR Article 17 erasure endpoint.
7. CloudWatch + SNS on the streaming job.
8. Datadog integration (fast-follow, once streaming is stable).
9. README, written incrementally per phase — not all at the end.

---

## 8. Documentation discipline

One short log entry per phase, written the same day the phase is finished —
not batched into one big session at the end like RiskLens's README became.
