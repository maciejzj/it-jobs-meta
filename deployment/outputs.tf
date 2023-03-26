output "it_jobs_meta_server_public_ip" {
  value = aws_instance.it_jobs_meta_server.public_ip
}

output "private_key" {
  value     = tls_private_key.ec2_server_key.private_key_pem
  sensitive = true
}

output "ansible_inventory" {
  value = local.ansible_inventory
}
