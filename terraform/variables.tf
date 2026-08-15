variable "project" {
  description = "Project name, used for resource tagging and S3 bucket name prefixes."
  type        = string
  default     = "streamguard"
}

variable "environment" {
  description = "Deployment environment (dev / test / prod)."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region for all Phase 4 resources."
  type        = string
  default     = "us-east-1"
}
