output "lakehouse_bucket_name" {
  description = "S3 bucket for Bronze Parquet data + Spark checkpoints."
  value       = aws_s3_bucket.lakehouse.id
}

output "gold_bucket_name" {
  description = "S3 bucket for dbt Gold output."
  value       = aws_s3_bucket.gold.id
}

output "athena_results_bucket_name" {
  description = "S3 bucket for Athena query results."
  value       = aws_s3_bucket.athena_results.id
}

output "glue_database" {
  description = "Glue catalog database name."
  value       = aws_glue_catalog_database.warehouse.name
}

output "glue_crawler" {
  description = "Glue crawler name (run manually to register Bronze tables)."
  value       = aws_glue_crawler.bronze.name
}

output "athena_workgroup" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.streamguard.name
}
