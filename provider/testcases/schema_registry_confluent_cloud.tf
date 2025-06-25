provider "deltastream" {}

resource "random_id" "suffix" {
  byte_length = 4
}

variable "pub_msk_iam_uri" {
  type = string
}

variable "pub_msk_iam_role" {
  type = string
}

variable "pub_msk_region" {
  type = string
}

variable "schema_registry_uris" {
  type = string
}

variable "schema_registry_key" {
  type = string
}

variable "schema_registry_secret" {
  type = string
}

resource "deltastream_schema_registry" "confluent_cloud" {
  name          = "Schema_registry_confluent_cloud_${random_id.suffix.hex}-东西"
  confluent_cloud = {
    uris   = var.schema_registry_uris
    key    = var.schema_registry_key
    secret = var.schema_registry_secret
  }
}

resource "deltastream_store" "kafka_with_iam" {
  name          = "schema_registry_${random_id.suffix.hex}"
  kafka = {
    uris               = var.pub_msk_iam_uri
    sasl_hash_function = "AWS_MSK_IAM"
    msk_iam_role_arn   = var.pub_msk_iam_role
    msk_aws_region     = var.pub_msk_region
    schema_registry_name = deltastream_schema_registry.confluent_cloud.name
  }
}

data "deltastream_schema_registries" "all" {
  depends_on = [deltastream_schema_registry.confluent_cloud]
}

data "deltastream_schema_registry" "confluent_cloud" {
  name = deltastream_schema_registry.confluent_cloud.name
}


