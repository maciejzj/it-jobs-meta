provider "aws" {
  region = "eu-central-1"
}

resource "aws_s3_bucket" "data_lake_storage" {
  bucket        = "it-jobs-meta-data-lake-${terraform.workspace}"
  force_destroy = true

  tags = {
    Name        = "S3 bucket for it-jobs-meta data lake"
    Project     = "${var.project_name_tag}"
    Environment = "${terraform.workspace}"
  }
}

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
  ami           = data.aws_ami.ubuntu.id
  instance_type = "t2.micro"

  subnet_id                   = aws_subnet.it_jobs_meta_public.id
  vpc_security_group_ids      = [aws_security_group.allow_web.id]
  associate_public_ip_address = true

  key_name             = aws_key_pair.it_jobs_meta_ec2_server.key_name
  iam_instance_profile = aws_iam_instance_profile.iam_profile_for_ec2.id

  tags = {
    Name        = "EC2 instance for it-jobs-meta server"
    Project     = "${var.project_name_tag}"
    Environment = "${terraform.workspace}"
  }
}
