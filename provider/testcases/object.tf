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
  name          = "object_kakfa_source_${random_id.suffix.hex}-东西"
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
  name = "object_test_${random_id.suffix.hex}"
}

resource "deltastream_object" "pageviews" {
  database = deltastream_database.test.name
  namespace = "public"
  store = deltastream_store.kafka_with_iam.name
  sql = <<EOF
    CREATE STREAM object_pageviews_${random_id.suffix.hex} (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='ds_pageviews', 'value.format'='json');
  EOF
}

resource "deltastream_object" "pageviews_5" {
  database = deltastream_database.test.name
  namespace = "public"
  store = deltastream_store.kafka_with_iam.name
  sql = <<EOF
    CREATE STREAM object_pageviews_5_${random_id.suffix.hex} (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='ds_pageviews', 'value.format'='json');
  EOF
}

resource "deltastream_object" "user_last_page" {
  database = deltastream_database.test.name
  namespace = "public"
  store = deltastream_store.kafka_with_iam.name
  sql = <<EOF
    CREATE CHANGELOG object_user_last_page_${random_id.suffix.hex} (viewtime BIGINT, userid VARCHAR, pageid VARCHAR, PRIMARY KEY(userid)) WITH ('topic'='ds_pageviews', 'value.format'='json');
  EOF
}

data "deltastream_object" "pageviews" {
  database = deltastream_database.test.name
  namespace = "public"
  name = deltastream_object.pageviews.name
}

data "deltastream_objects" "all" {
  depends_on = [ deltastream_object.pageviews, deltastream_object.user_last_page]
  database = deltastream_database.test.name
  namespace = "public"
}
