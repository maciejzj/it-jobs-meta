terraform {
  backend "s3" {
    region = "eu-central-1"
    bucket = "it-jobs-meta-terraform-state-bucket"
    key    = "terraform-backend-statet/terraform.tfstate"
  }
}
