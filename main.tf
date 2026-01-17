terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.5.0"
}

provider "aws" {
  region = "us-east-1"
}

# Create private S3 bucket
resource "random_integer" "bucket_suffix" {
  min = 10000
  max = 99999
}

resource "aws_s3_bucket" "private_bucket" {
  bucket = "connectx-storage-${random_integer.bucket_suffix.result}"

  tags = {
    Name = "private-project-bucket"
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "block_public_access" {
  bucket                  = aws_s3_bucket.private_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create IAM users
variable "user_names" {
  type    = list(string)
  default = ["zhuravlovvy24-creator"]
}

resource "aws_iam_user" "users" {
  for_each      = toset(var.user_names)
  name          = each.value
  force_destroy = true
}

# Create IAM policy for bucket access
data "aws_iam_policy_document" "bucket_rw_policy" {
  statement {
    sid    = "AllowViewSpecificBucket"
    effect = "Allow"

    actions = [
      "s3:ListAllMyBuckets",
      "s3:GetBucketLocation"
    ]

    resources = ["*"]
  }

  statement {
    sid    = "BucketReadWriteAccess"
    effect = "Allow"

    actions = [
      "s3:ListBucket"
    ]

    resources = [
      aws_s3_bucket.private_bucket.arn
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      "${aws_s3_bucket.private_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "bucket_rw_policy" {
  name        = "BucketReadWriteAccess"
  description = "Grants read/write access to the private S3 bucket"
  policy      = data.aws_iam_policy_document.bucket_rw_policy.json
}

# Attach policy and create credentials
resource "aws_iam_user_policy_attachment" "user_policy_attach" {
  for_each = aws_iam_user.users

  user       = each.value.name
  policy_arn = aws_iam_policy.bucket_rw_policy.arn
}

resource "aws_iam_access_key" "user_keys" {
  for_each = aws_iam_user.users
  user     = each.value.name
}

# Output credentials securely
output "user_access_keys" {
  value = {
    for user, creds in aws_iam_access_key.user_keys :
    user => {
      access_key_id     = creds.id
      secret_access_key = creds.secret
    }
  }
  sensitive = true
}

output "bucket_name" {
  value = aws_s3_bucket.private_bucket.bucket
}
