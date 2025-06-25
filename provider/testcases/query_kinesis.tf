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

resource "deltastream_store" "kafka_with_iam" {
  name          = "query_kinesis_kafka_source_${random_id.suffix.hex}"
  kafka = {
    uris               = var.pub_msk_iam_uri
    sasl_hash_function = "AWS_MSK_IAM"
    msk_iam_role_arn   = var.pub_msk_region
    msk_aws_region     = var.msk_region
  }
}

variable "kinesis_uri" {
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
  name          = "query_kinesis_kinesis_sink_${random_id.suffix.hex}"
  kinesis = {
    uris              = var.kinesis_url
    access_key_id     = var.kinesis_key
    secret_access_key = var.kinesis_secret
    aws_account_id    = var.kinesis_account_id
  }
}

resource "deltastream_database" "db" {
  name = "db_${random_id.suffix.hex}"
}

resource "deltastream_object" "pageviews" {
  database = deltastream_database.db.name
  namespace   = "public"
  store    = deltastream_store.kafka_with_iam.name
  sql      = <<EOF
    CREATE STREAM "Query_kinesis_pageviews_${random_id.suffix.hex}-东西" (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='pageviews', 'value.format'='json');
  EOF
}

resource "deltastream_entity" "pageviews_6" {
  store       = deltastream_store.kinesis_creds.name
  entity_path = ["Query_kinesis_pageviews_6_${random_id.suffix.hex}.-0"]
  kafka_properties = {
    kinesis_shards = 1
  }
}

resource "deltastream_object" "pageviews_6" {
  database = deltastream_database.db.name
  namespace   = "public"
  store    = deltastream_store.kafka_with_iam.name
  sql      = <<EOF
    CREATE STREAM query_kinesis_pageviews_6_${random_id.suffix.hex} (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='${deltastream_entity.pageviews_6.entity_path[0]}', 'value.format'='json');
  EOF
}

resource "deltastream_query" "insert_into_pageviews_6" {
  source_relation_fqns = [deltastream_object.pageviews.fqn]
  sink_relation_fqn    = deltastream_object.pageviews_6.fqn
  sql                  = <<EOF
    INSERT INTO ${deltastream_object.pageviews_6.fqn} SELECT * FROM ${deltastream_object.pageviews.fqn} WHERE userid = 'User_6';
  EOF
}

data "deltastream_entity_data" "pageviews_6" {
  depends_on  = [deltastream_query.insert_into_pageviews_6]
  store       = deltastream_store.kafka_with_iam.name
  entity_path = deltastream_entity.pageviews_6.entity_path
  num_rows    = 3
}
