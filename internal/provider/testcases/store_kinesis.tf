provider "deltastream" {}

resource "random_id" "suffix" {
  byte_length = 4
}

variable "kinesis_url" {
  type = string
}

variable "kinesis_region" {
  type = string
}

variable "kinesis_key" {
  type = string
}

variable "kinesis_secret" {
  type = string
}

variable "kinesis_account_id" {
  type = string
}

resource "deltastream_store" "kinesis_creds" {
  name          = "Store_kinesis_with_creds_${random_id.suffix.hex}-东西"
  access_region = var.kinesis_region
  kinesis = {
    uris              = var.kinesis_url
    access_key_id     = var.kinesis_key
    secret_access_key = var.kinesis_secret
    aws_account_id    = var.kinesis_account_id
  }
}

data "deltastream_stores" "all" {
  depends_on = [deltastream_store.kinesis_creds]
}

data "deltastream_store" "kinesis_creds" {
  name = deltastream_store.kinesis_creds.name
}
