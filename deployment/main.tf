provider "aws" {
  region = "eu-central-1"
}

resource "aws_s3_bucket" "data_lake_bucket" {
  bucket        = "it-jobs-meta-data-lake-${terraform.workspace}"
  force_destroy = true

  tags = {
    Name        = "S3 bucket for it-jobs-meta data lake"
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

resource "aws_instance" "it_jobs_meta_server" {
  # eu-central-1, Ubuntu 20.04 LTS, amd64
  ami                    = "ami-0498a49a15494604f"
  instance_type          = "t2.micro"
  vpc_security_group_ids = [aws_security_group.allow_web.id]

  key_name             = aws_key_pair.ec2_server_keypair.key_name
  iam_instance_profile = aws_iam_instance_profile.iam_profile_for_ec2.id

  tags = {
    Name        = "EC2 instance for it-jobs-meta server"
    Environment = "${terraform.workspace}"
  }
}

resource "local_sensitive_file" "private_key_pem" {
  filename = "${path.module}/it-jobs-meta-ec2-server.pem"
  content  = tls_private_key.ec2_server_key.private_key_pem
}

locals {
  ansible_inventory = templatefile("${path.module}/templates/inventory.tpl", {
    instance_ip  = aws_instance.it_jobs_meta_server.public_ip
    ssh_key_path = local_sensitive_file.private_key_pem.filename
  })
}

resource "local_file" "ansible_inventory_file" {
  filename = "${path.module}/hosts"
  content  = local.ansible_inventory
}
