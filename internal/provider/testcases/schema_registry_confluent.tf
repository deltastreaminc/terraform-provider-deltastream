provider "deltastream" {}

resource "random_id" "suffix" {
  byte_length = 4
}

variable "schema_registry_uris" {
  type = string
}

variable "schema_registry_username" {
  type = string
}

variable "schema_registry_password" {
  type = string
}

resource "deltastream_schema_registry" "confluent" {
  name          = "Schema_registry_confluent_${random_id.suffix.hex}-东西"
  confluent = {
    uris     = var.schema_registry_uris
    username = var.schema_registry_username
    password = var.schema_registry_password
  }
}

resource "deltastream_schema_registry" "confluent_nopwd" {
  name          = "schema_registry_confluent_nopwd${random_id.suffix.hex}"
  confluent = {
    uris     = var.schema_registry_uris
  }
}

data "deltastream_schema_registries" "all" {
  depends_on = [deltastream_schema_registry.confluent, deltastream_schema_registry.confluent_nopwd]
}

data "deltastream_schema_registry" "confluent" {
  name = deltastream_schema_registry.confluent.name
}

data "deltastream_schema_registry" "confluent_nopwd" {
  name = deltastream_schema_registry.confluent_nopwd.name
}
