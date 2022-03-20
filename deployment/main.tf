provider "aws" {
  region     = "eu-central-1"
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

resource "aws_s3_bucket" "data_lake_bucket" {
  bucket        = "it-jobs-meta-data-lake"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "data_lake_bucket_access" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  # Keep the bucket private
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls  = true
}

resource "aws_default_vpc" "default_vpc" {
  tags = {
    Name = "Default AWS VPC"
  }
}

resource "aws_security_group" "allow_web" {
  name        = "allow_web_traffic"
  description = "Allow web trafic for hosting a server"
  vpc_id      = aws_default_vpc.default_vpc.id

  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    description = "HTTP"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_policy" "allow_s3_bucket_access" {
  name        = "allow-s3-bucket-access"
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
}

resource "aws_iam_role" "iam_role_for_ec2" {
  name = "my_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_instance_profile" "iam_profile_for_ec2" {
  name = "some-profile"
  role = aws_iam_role.iam_role_for_ec2.name
}


resource "aws_iam_role_policy_attachment" "data_lake_bucket_policy_attach" {
  role       = aws_iam_role.iam_role_for_ec2.name
  policy_arn = aws_iam_policy.allow_s3_bucket_access.arn
}

resource "aws_instance" "it_jobs_meta_server" {
  # eu-central-1, Ubuntu 20.04 LTS, amd64
  ami                    = "ami-0498a49a15494604f"
  instance_type          = "t2.micro"
  vpc_security_group_ids = [aws_security_group.allow_web.id]

  tags = {
    Name = "It Jobs Meta server"
  }

  iam_instance_profile = aws_iam_instance_profile.iam_profile_for_ec2.id
}
