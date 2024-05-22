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

variable "databricks_uri" {
  type = string
}

variable "databricks_app_token" {
  type = string
}

variable "databricks_warehouse_id" {
  type = string
}

variable "databricks_access_key_id" {
  type = string
}

variable "databricks_secret_access_key" {
  type = string
}

variable "databricks_bucket" {
  type = string
}

variable "databricks_bucket_region" {
  type = string
}

resource "deltastream_store" "databricks" {
  name          = "store_databricks_${random_id.suffix.hex}"
  access_region = data.deltastream_region.region.name
  databricks = {
    uris              = var.databricks_uri
    app_token         = var.databricks_app_token
    warehouse_id      = var.databricks_warehouse_id
    access_key_id     = var.databricks_access_key_id
    secret_access_key = var.databricks_secret_access_key
    cloud_s3_bucket   = var.databricks_bucket
    cloud_region      = var.databricks_bucket_region
  }
}

data "deltastream_stores" "all" {
  depends_on = [deltastream_store.databricks]
}

data "deltastream_store" "databricks" {
  name = deltastream_store.databricks.name
}

data "deltastream_entities" "databricks" {
  store = deltastream_store.databricks.name
}
