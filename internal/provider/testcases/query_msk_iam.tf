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
  name = "query_msk_iam_kafka_source_${random_id.suffix.hex}"
  kafka = {
    uris               = var.pub_msk_iam_uri
    sasl_hash_function = "AWS_MSK_IAM"
    msk_iam_role_arn   = var.pub_msk_iam_role
    msk_aws_region     = var.pub_msk_region
  }
}

resource "deltastream_database" "db" {
  name = "query_msk_iam_database_${random_id.suffix.hex}"
}

resource "deltastream_object" "pageviews" {
  database  = deltastream_database.db.name
  namespace = "public"
  store     = deltastream_store.kafka_with_iam.name
  sql       = <<EOF
    CREATE STREAM "Query_msk_iam_pageviews_${random_id.suffix.hex}-东西" (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='ds_pageviews', 'value.format'='json');
  EOF
}

resource "deltastream_entity" "pageviews_6" {
  store       = deltastream_store.kafka_with_iam.name
  entity_path = ["Query_msk_iam_pageviews_6_git_${random_id.suffix.hex}"]
  kafka_properties = {
    topic_partitions = 3
    topic_replicas   = 3
  }
}

resource "deltastream_object" "pageviews_6" {
  database  = deltastream_database.db.name
  namespace = "public"
  store     = deltastream_store.kafka_with_iam.name
  sql       = <<EOF
    CREATE STREAM "Query_msk_iam_pageviews_6_${random_id.suffix.hex}-东西" (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='${deltastream_entity.pageviews_6.entity_path[0]}', 'value.format'='json');
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
  depends_on     = [deltastream_query.insert_into_pageviews_6]
  store          = deltastream_store.kafka_with_iam.name
  entity_path    = deltastream_entity.pageviews_6.entity_path
  num_rows       = 3
  from_beginning = true
}
