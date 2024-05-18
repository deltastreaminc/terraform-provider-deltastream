resource "deltastream_store" "kafka_with_sasl" {
  name          = "kafka_with_sasl_${random_id.suffix.hex}"
  access_region = "AWS us-west-2"
  kafka = {
    uris               = var.kafka_url
    sasl_hash_function = "PLAIN"
    sasl_username      = var.kafka_sasl_username
    sasl_password      = var.kafka_sasl_password
  }
}

resource "deltastream_store" "confluent_kafka_with_sasl" {
  name          = "confluent_kafka_with_sasl_${random_id.suffix.hex}"
  access_region = "AWS us-west-2"
  confluent_kafka = {
    uris               = var.kafka_url
    sasl_hash_function = "PLAIN"
    sasl_username      = var.kafka_sasl_username
    sasl_password      = var.kafka_sasl_password
  }
}

resource "deltastream_store" "kafka_with_iam" {
  name          = "kafka_with_iam_${random_id.suffix.hex}"
  access_region = "AWS us-west-2"
  kafka = {
    uris               = var.msk_url
    sasl_hash_function = "AWS_MSK_IAM"
    msk_iam_role_arn   = var.msk_iam_role
    msk_aws_region     = var.msk_region
  }
}

resource "deltastream_store" "kinesis_creds" {
  name          = "kinesis_with_creds_${random_id.suffix.hex}"
  access_region = var.kinesis_region
  kinesis = {
    uris              = var.kinesis_url
    access_key_id     = var.kinesis_key
    secret_access_key = var.kinesis_secret
  }
}

resource "deltastream_store" "databricks" {
  name          = "databricks_${random_id.suffix.hex}"
  access_region = "AWS us-west-2"
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

resource "deltastream_store" "snowflake" {
  name          = "snowflake_${random_id.suffix.hex}"
  access_region = "AWS us-west-2"
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

# resource "deltastream_store" "postgres" {
#   name          = "kinesis_with_creds_${random_id.suffix.hex}"
#   access_region = "AWS us-west-2"
#   postgres = {
#     uris = var.postgres_uris
#     username = var.postgres_username
#     password = var.postgres_password
#   }
# }
