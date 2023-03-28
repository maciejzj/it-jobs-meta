# S3 bucket data lake
resource "aws_iam_policy" "allow_s3_bucket_access" {
  name        = "allow-s3-bucket-data-lake-access-${terraform.workspace}"
  path        = "/"
  description = "Allow "

  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Sid" : "VisualEditor0",
        "Effect" : "Allow",
        "Action" : [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ],
        "Resource" : [
          "arn:aws:s3:::*/*",
          aws_s3_bucket.data_lake_bucket.arn
        ]
      }
    ]
  })

  tags = {
    Name        = "IAM policy to allow access to s3 it-jobs-meta data lake"
    Environment = "${terraform.workspace}"
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake_bucket_access" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  # Keep the bucket private
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls  = true
}

# EC2 web server
resource "aws_iam_role" "iam_role_for_ec2" {
  name = "iam-role-for-ec2-it-jobs-meta-server-${terraform.workspace}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })

  tags = {
    Name        = "IAM role for EC2 it-jobs-meta server"
    Environment = "${terraform.workspace}"
  }
}

resource "aws_iam_instance_profile" "iam_profile_for_ec2" {
  name = "iam-instance-profile-for-ec2-it-jobs-meta-server-${terraform.workspace}"
  role = aws_iam_role.iam_role_for_ec2.name

  tags = {
    Name        = "IAM profile for EC2 it-jobs-meta server"
    Environment = "${terraform.workspace}"
  }
}

# Attach EC2 server profile with data lake policy
resource "aws_iam_role_policy_attachment" "data_lake_bucket_policy_attach" {
  role       = aws_iam_role.iam_role_for_ec2.name
  policy_arn = aws_iam_policy.allow_s3_bucket_access.arn
}


# EC2 keypair for server access
resource "tls_private_key" "ec2_server_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "ec2_server_keypair" {
  key_name   = "keypair-it-jobs-meta-server"
  public_key = tls_private_key.ec2_server_key.public_key_openssh
}
