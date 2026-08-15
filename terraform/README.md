# StreamGuard — Terraform (Phase 4: S3 + Glue catalog + Athena)

AWS resources for the Athena warehouse, with `terraform destroy` after every
work session (plan hard cap: **$3–5, no top-up**).

## What it provisions

| Resource | Name pattern | Notes |
|---|---|---|
| S3 Lakehouse | `<project>-lakehouse-<hex>` | Bronze parquet + Spark checkpoints; **public access blocked** |
| S3 Gold | `<project>-gold-<hex>` | dbt output (`fact_transactions`, `dim_accounts`); public access blocked |
| S3 Athena results | `<project>-athena-results-<hex>` | Query output; auto-expires after 7 days; public access blocked |
| Glue database | `<project>_<env>_warehouse` | Athena catalog |
| Glue crawler | `<project>-<env>-bronze-crawler` | Registers Bronze tables; **manual run** (no schedule = no idle cost) |
| Athena workgroup | `<project>-<env>` | Points query results at the Athena bucket |

`<hex>` is a **random 8-char suffix** (`random_id`) so bucket names are
globally unique — no tfvars required.

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) ≥ 1.6 (dev box: 1.15.8)
- AWS CLI credentials configured (prefer **short-lived** creds — SUGGESTIONS #2)

## Workflow

```powershell
cd terraform

terraform init        # downloads the AWS + random providers
terraform plan        # preview resources (nothing is created yet)
terraform apply       # creates S3 + Glue + Athena (~$0, nothing is running)
terraform destroy     # end of session — every AWS resource gone

# After ingesting to the lakehouse bucket, register the tables once:
aws glue start-crawler --name "$(terraform output -raw glue_crawler)"

# Query via Athena — workgroup: terraform output -raw athena_workgroup
```

## Cost notes (kept deliberately lean)

- Buckets have `force_destroy = true`, so `terraform destroy` always succeeds
  even with data in them.
- Crawler is **not scheduled** — running it manually after each ingest is the
  only Glue cost, and it's tiny.
- Athena results bucket expires after 7 days as a safety net.
- Athena queries are pay-per-scan; keep them filtered on the `type` partition
  until the Gold tables exist.

## Local mock first (minIO)

The S3 path is proven against **minIO** locally before anything here is
applied. Only after the PySpark consumer writes correct partitioned parquet to
an S3-compatible store do we `terraform apply` to real AWS.
