provider "deltastream" {}

variable "region" {
  type = string
}

data "deltastream_region" "region" {
  name = var.region
}

resource "random_id" "suffix" {
  byte_length = 4
}

variable "snowflake_uris" {
  type = string
}

variable "snowflake_account_id" {
  type = string
}

variable "snowflake_cloud_region" {
  type = string
}

variable "snowflake_warehouse_name" {
  type = string
}

variable "snowflake_role_name" {
  type = string
}

variable "snowflake_username" {
  type = string
}

variable "snowflake_client_key_file" {
  type = string
}

variable "snowflake_client_key_passphrase" {
  type = string
}

resource "deltastream_store" "snowflake" {
  name          = "store_snowflake_${random_id.suffix.hex}"
  access_region = data.deltastream_region.region.name
  snowflake = {
    uris                  = var.snowflake_uris
    account_id            = var.snowflake_account_id
    cloud_region          = var.snowflake_cloud_region
    warehouse_name        = var.snowflake_warehouse_name
    role_name             = var.snowflake_role_name
    username              = var.snowflake_username
    client_key_file       = var.snowflake_client_key_file
    client_key_passphrase = var.snowflake_client_key_passphrase
  }
}

data "deltastream_stores" "all" {
  depends_on = [deltastream_store.snowflake]
}

data "deltastream_store" "snowflake" {
  name = deltastream_store.snowflake.name
}
