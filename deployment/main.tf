provider "aws" {
  region = "eu-central-1"
}

# S3 bucket data lake
resource "aws_s3_bucket" "data_lake_bucket" {
  bucket        = "it-jobs-meta-data-lake-${terraform.workspace}"
  force_destroy = true

  tags = {
    Name        = "S3 bucket for it-jobs-meta data lake"
    Environment = "${terraform.workspace}"
  }
}

# EC2 web server
data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"]
  }

  # Canonical
  owners = ["099720109477"]
}

resource "aws_instance" "it_jobs_meta_server" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t2.micro"
  vpc_security_group_ids = [aws_security_group.allow_web.id]

  key_name             = aws_key_pair.ec2_server_keypair.key_name
  iam_instance_profile = aws_iam_instance_profile.iam_profile_for_ec2.id

  tags = {
    Name        = "EC2 instance for it-jobs-meta server"
    Environment = "${terraform.workspace}"
  }
}

# Relay EC2 configuration to Ansible

resource "local_sensitive_file" "private_key_pem" {
  filename = "${path.module}/artifacts/it-jobs-meta-ec2-server.pem"
  content  = tls_private_key.ec2_server_key.private_key_pem
}

locals {
  ansible_inventory = templatefile("${path.module}/templates/hosts.tpl", {
    instance_ip  = aws_instance.it_jobs_meta_server.public_ip
    ssh_key_path = local_sensitive_file.private_key_pem.filename
  })
}

resource "local_file" "ansible_inventory_file" {
  filename = "${path.module}/artifacts/hosts"
  content  = local.ansible_inventory
}
