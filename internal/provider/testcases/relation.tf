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
  name          = "Relation_kakfa_source_${random_id.suffix.hex}-东西"
  access_region = data.deltastream_region.region.name
  kafka = {
    uris               = var.pub_msk_iam_uri
    sasl_hash_function = "AWS_MSK_IAM"
    msk_iam_role_arn   = var.pub_msk_iam_role
    msk_aws_region     = var.pub_msk_region
  }
}

data "deltastream_entities" "kafka_with_iam" {
  store = deltastream_store.kafka_with_iam.name
}

resource "deltastream_database" "test" {
  name = "relation_test_${random_id.suffix.hex}"
}

resource "deltastream_relation" "pageviews" {
  database = deltastream_database.test.name
  schema = "public"
  store = deltastream_store.kafka_with_iam.name
  sql = <<EOF
    CREATE STREAM relation_pageviews_${random_id.suffix.hex} (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='ds_pageviews', 'value.format'='json');
  EOF
}

resource "deltastream_relation" "pageviews_5" {
  database = deltastream_database.test.name
  schema = "public"
  store = deltastream_store.kafka_with_iam.name
  sql = <<EOF
    CREATE STREAM relation_pageviews_5_${random_id.suffix.hex} (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='ds_pageviews', 'value.format'='json');
  EOF
}

resource "deltastream_relation" "user_last_page" {
  database = deltastream_database.test.name
  schema = "public"
  store = deltastream_store.kafka_with_iam.name
  sql = <<EOF
    CREATE CHANGELOG relation_user_last_page_${random_id.suffix.hex} (viewtime BIGINT, userid VARCHAR, pageid VARCHAR, PRIMARY KEY(userid)) WITH ('topic'='ds_pageviews', 'value.format'='json');
  EOF
}

data "deltastream_relation" "pageviews" {
  database = deltastream_database.test.name
  schema = "public"
  name = deltastream_relation.pageviews.name
}

data "deltastream_relations" "all" {
  depends_on = [ deltastream_relation.pageviews, deltastream_relation.user_last_page]
  database = deltastream_database.test.name
  schema = "public"
}
