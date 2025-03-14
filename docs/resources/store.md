---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "deltastream_store Resource - deltastream"
subcategory: ""
description: |-
  Store resource
---

# deltastream_store (Resource)

Store resource

## Example Usage

```terraform
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
```

<!-- schema generated by tfplugindocs -->
## Schema

### Required

- `access_region` (String) Specifies the region of the Store. In order to improve latency and reduce data transfer costs, the region should be the same cloud and region that the physical Store is running in.
- `name` (String) Name of the Store

### Optional

- `confluent_kafka` (Attributes) Confluent Kafka specific configuration (see [below for nested schema](#nestedatt--confluent_kafka))
- `databricks` (Attributes) Databricks specific configuration (see [below for nested schema](#nestedatt--databricks))
- `kafka` (Attributes) Kafka specific configuration (see [below for nested schema](#nestedatt--kafka))
- `kinesis` (Attributes) Kinesis specific configuration (see [below for nested schema](#nestedatt--kinesis))
- `owner` (String) Owning role of the Store
- `postgres` (Attributes) Postgres specific configuration (see [below for nested schema](#nestedatt--postgres))
- `snowflake` (Attributes) Snowflake specific configuration (see [below for nested schema](#nestedatt--snowflake))

### Read-Only

- `created_at` (String) Creation date of the Store
- `state` (String) State of the Store
- `type` (String) Type of the Store
- `updated_at` (String) Last update date of the Store

<a id="nestedatt--confluent_kafka"></a>
### Nested Schema for `confluent_kafka`

Required:

- `sasl_hash_function` (String) SASL hash function to use when authenticating with Confluent Kafka brokers
- `sasl_password` (String, Sensitive) Password to use when authenticating with Apache Kafka brokers
- `sasl_username` (String, Sensitive) Username to use when authenticating with Apache Kafka brokers
- `uris` (String) List of host:port URIs to connect to the store


<a id="nestedatt--databricks"></a>
### Nested Schema for `databricks`

Required:

- `access_key_id` (String, Sensitive) AWS access key ID used for writing data to S3
- `app_token` (String) Databricks personal access token used when authenticating with a Databricks workspace
- `cloud_region` (String) The region where the S3 bucket is located
- `cloud_s3_bucket` (String) The name of the S3 bucket where the data will be stored
- `secret_access_key` (String, Sensitive) AWS secret access key used for writing data to S3
- `uris` (String) List of host:port URIs to connect to the store
- `warehouse_id` (String) The identifier for a Databricks SQL Warehouse belonging to a Databricks workspace. This Warehouse will be used to create and query Tables in Databricks


<a id="nestedatt--kafka"></a>
### Nested Schema for `kafka`

Required:

- `sasl_hash_function` (String) SASL hash function to use when authenticating with Apache Kafka brokers
- `uris` (String) List of host:port URIs to connect to the store

Optional:

- `msk_aws_region` (String, Sensitive) AWS region where the Amazon MSK cluster is located
- `msk_iam_role_arn` (String, Sensitive) IAM role ARN to use when authenticating with Amazon MSK
- `sasl_password` (String, Sensitive) Password to use when authenticating with Apache Kafka brokers
- `sasl_username` (String, Sensitive) Username to use when authenticating with Apache Kafka brokers
- `schema_registry_name` (String) Name of the schema registry
- `tls_ca_cert_file` (String) CA certificate in PEM format
- `tls_disabled` (Boolean) Specifies if the store should be accessed over TLS
- `tls_verify_server_hostname` (Boolean) Specifies if the server CNAME should be validated against the certificate


<a id="nestedatt--kinesis"></a>
### Nested Schema for `kinesis`

Required:

- `aws_account_id` (String, Sensitive) AWS account ID to use when authenticating with an Amazon Kinesis service
- `uris` (String) List of host:port URIs to connect to the store

Optional:

- `access_key_id` (String, Sensitive) AWS IAM access key to use when authenticating with an Amazon Kinesis service
- `secret_access_key` (String, Sensitive) AWS IAM secret access key to use when authenticating with an Amazon Kinesis service


<a id="nestedatt--postgres"></a>
### Nested Schema for `postgres`

Required:

- `password` (String, Sensitive) Password to use when authenticating with a Postgres database
- `uris` (String) List of host:port URIs to connect to the store
- `username` (String, Sensitive) Username to use when authenticating with a Postgres database


<a id="nestedatt--snowflake"></a>
### Nested Schema for `snowflake`

Required:

- `account_id` (String) Snowflake account ID
- `client_key_file` (String, Sensitive) Snowflake account's private key in PEM format
- `client_key_passphrase` (String, Sensitive) Passphrase for decrypting the Snowflake account's private key
- `cloud_region` (String) Snowflake cloud region name, where the account resources operate in
- `role_name` (String) Access control role to use for the Store operations after connecting to Snowflake
- `uris` (String) List of host:port URIs to connect to the store
- `username` (String, Sensitive) User login name for the Snowflake account
- `warehouse_name` (String) Warehouse name to use for queries and other store operations that require compute resource
