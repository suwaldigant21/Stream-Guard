# IAM for the Glue crawler: minimal, scoped, least-privilege.
# (The local consumer uses the operator's short-lived AWS credentials, so it
# needs no role of its own — SUGGESTIONS #2.)

data "aws_iam_policy_document" "glue_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler" {
  name               = "${var.project}-${var.environment}-glue-crawler"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_crawler_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Scoped S3 read for the crawler: only the Bronze + Gold buckets.
data "aws_iam_policy_document" "glue_s3" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:GetBucketLocation",
    ]

    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/*",
      aws_s3_bucket.gold.arn,
      "${aws_s3_bucket.gold.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "glue_s3" {
  name   = "${var.project}-${var.environment}-glue-s3-read"
  policy = data.aws_iam_policy_document.glue_s3.json
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = aws_iam_policy.glue_s3.arn
}
