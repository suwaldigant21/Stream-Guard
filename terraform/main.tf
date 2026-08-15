terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  # Local state on purpose: this whole stack is `terraform destroy`-ed after
  # every work session (the plan's $3-5 cap), so a remote state bucket would
  # just add cost and leftovers to clean up.
}

provider "aws" {
  region = var.region # us-east-1 by default

  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# Generate a random suffix so S3 bucket names are globally unique (no
# terraform.tfvars needed — `terraform init && terraform apply` just works).
resource "random_id" "bucket_suffix" {
  byte_length = 4
}
