provider "aws" {
  region     = "eu-central-1"
  access_key = "placeholder"
  secret_key = "placeholder"
}

module "it_jobs_meta_data_pipeline_lambda" {
  source = "terraform-aws-modules/lambda/aws"

  function_name  = "it_jobs_meta_data_pipeline_lambda"
  create_package = false

  image_uri    = module.it_jobs_meta_application_docker.image_uri
  package_type = "Image"

  attach_policy_json = true
  policy_json = <<EOF
{
"Version": "2012-10-17",
"Statement": [
    {
        "Effect": "Allow",
        "Action": [
            "s3:*"
        ],
        "Resource": "arn:aws:s3:::*"
    }
]}
EOF
}

module "it_jobs_meta_application_docker" {
  source           = "terraform-aws-modules/lambda/aws//modules/docker-build"
  source_path      = path.cwd
  docker_file_path = "lambda.dockerfile"

  create_ecr_repo = true
  ecr_repo        = "it_jobs_meta_main_ecr"
}

resource "aws_s3_bucket" "s3bucketitjobsmeta" {
  bucket = "s3bucketitjobsmeta"
}

resource "aws_rds_cluster" "default" {
  cluster_identifier      = "aurora-cluster-demo"
  engine                  = "aurora-mysql"  
  engine_mode             = "serverless"  
  database_name           = "placeholder"
  master_username         = "placeholder"
  master_password         = "placeholder"
  
  skip_final_snapshot     = true
  
  scaling_configuration {
    auto_pause               = true
    min_capacity             = 1    
    max_capacity             = 2
    seconds_until_auto_pause = 300
    timeout_action           = "ForceApplyCapacityChange"
  }  
}



