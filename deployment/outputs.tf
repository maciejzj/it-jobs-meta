output "it_jobs_meta_server_public_ip" {
  value = aws_instance.it_jobs_meta_server.public_ip
}

# Relay EC2 configuration to Ansible

resource "local_sensitive_file" "private_key_pem" {
  filename = "${path.module}/artifacts/it-jobs-meta-ec2-server.pem"
  content  = tls_private_key.it_jobs_meta_ec2_server.private_key_pem
}

locals {
  ansible_inventory = templatefile("${path.module}/templates/hosts.tpl", {
    instance_ip  = aws_instance.it_jobs_meta_server.public_ip
    ssh_key_path = local_sensitive_file.private_key_pem.filename
    workspace    = terraform.workspace
  })
}

locals {
  s3_bucket_config = templatefile("${path.module}/templates/s3_bucket_config.yml.tpl", {
    bucket_name = aws_s3_bucket.data_lake_storage.bucket
  })
}

resource "local_file" "ansible_inventory" {
  filename = "${path.module}/artifacts/hosts"
  content  = local.ansible_inventory
}

resource "local_file" "s3_bucket_config" {
  filename = "${path.module}/artifacts/s3_bucket_config.yml"
  content  = local.s3_bucket_config
}
