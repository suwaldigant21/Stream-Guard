resource "aws_glue_catalog_database" "warehouse" {
  name = "${var.project}_${var.environment}_warehouse"
}

# Crawler scans Bronze and registers the partition-by-type tables Athena needs.
# No schedule on purpose: run it manually after each session's ingestion, so
# crawler time (and cost) only happens when data actually changed.
resource "aws_glue_crawler" "bronze" {
  name          = "${var.project}-${var.environment}-bronze-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.warehouse.name
  table_prefix  = "bronze_"

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  s3_target {
    path       = "s3://${aws_s3_bucket.lakehouse.id}"
    exclusions = ["_spark_metadata/", "*.json"]
  }
}
