output "security_group_id" {
  value       = aws_security_group.delta.id
}
output "db_instance_endpoint" {
  value       = aws_db_instance.default.endpoint
}