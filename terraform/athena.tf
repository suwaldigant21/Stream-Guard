resource "aws_athena_workgroup" "streamguard" {
  name        = "${var.project}-${var.environment}"
  description = "StreamGuard Athena workgroup (Phase 4 warehouse)."

  configuration {
    enforce_workgroup_configuration = true

    engine_version {
      selected_engine_version = "AUTO"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/query-results/"
    }
  }
}
