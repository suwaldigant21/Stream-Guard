# Lakehouse: Bronze parquet + Spark checkpoints written by the PySpark
# consumer (partitioned by transaction type, like the local data/bronze
# layout). force_destroy = true lets `terraform destroy` wipe contents
# instantly, keeping the plan's per-session teardown cheap.
resource "aws_s3_bucket" "lakehouse" {
  bucket        = "${var.project}-lakehouse-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lakehouse_public_block" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Gold: dbt output (fact_transactions / dim_accounts).
resource "aws_s3_bucket" "gold" {
  bucket        = "${var.project}-gold-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "gold_public_block" {
  bucket = aws_s3_bucket.gold.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Athena query results (required by Athena to store query output).
# force_destroy keeps `terraform destroy` working; the lifecycle rule cleans
# up abandoned query runs.
resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.project}-athena-results-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "athena_results_public_block" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results_expire" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {}

    expiration {
      days = 7
    }
  }
}
