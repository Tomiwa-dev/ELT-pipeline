variable "aws_secret_key" {
  description = "aws secret key"
  type = string
  sensitive = true

}

variable "aws_access_key" {
  description = "aws access key"
  type = string
  sensitive = true

}

variable "db_password" {
  description = "postgres rds database password"
  type = string
  sensitive = true
}

variable "db_username" {
  description = "postgres rds database username"
  type = string
  sensitive = true
}

variable "aws_region" {
  default = "eu-west-1"
  type = string
}

variable "aws_key_name" {
  description = "ec2 pem key to ssh into emr cluster"
  type = string
  sensitive = true

}