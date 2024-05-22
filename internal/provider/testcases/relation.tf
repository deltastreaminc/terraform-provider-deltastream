provider "deltastream" {}

data "deltastream_regions" "all" {}

resource "random_id" "suffix" {
  byte_length = 4
}

variable "msk_url" {
  type = string
}

variable "msk_iam_role" {
  type = string
}

variable "msk_region" {
  type = string
}

resource "deltastream_store" "kafka_with_iam" {
  name          = "kafka_with_iam_${random_id.suffix.hex}"
  access_region = data.deltastream_regions.all.items[0].name
  kafka = {
    uris               = var.msk_url
    sasl_hash_function = "AWS_MSK_IAM"
    msk_iam_role_arn   = var.msk_iam_role
    msk_aws_region     = var.msk_region
  }
}

data "deltastream_entities" "kafka_with_iam" {
  store = deltastream_store.kafka_with_iam.name
}

resource "deltastream_database" "test" {
  name = "test_${random_id.suffix.hex}"
}

resource "deltastream_relation" "pageviews" {
  database = deltastream_database.test.name
  schema = "public"
  store = deltastream_store.kafka_with_iam.name
  sql = <<EOF
    CREATE STREAM PAGEVIEWS_${random_id.suffix.hex} (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='pageviews', 'value.format'='json');
  EOF
}

resource "deltastream_relation" "pageviews_5" {
  database = deltastream_database.test.name
  schema = "public"
  store = deltastream_store.kafka_with_iam.name
  sql = <<EOF
    CREATE STREAM PAGEVIEWS_5_${random_id.suffix.hex} (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='pageviews', 'value.format'='json');
  EOF
}

resource "deltastream_relation" "user_last_page" {
  database = deltastream_database.test.name
  schema = "public"
  store = deltastream_store.kafka_with_iam.name
  sql = <<EOF
    CREATE CHANGELOG user_last_page_${random_id.suffix.hex} (viewtime BIGINT, userid VARCHAR, pageid VARCHAR, PRIMARY KEY(userid)) WITH ('topic'='pageviews', 'value.format'='json');
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

data "deltastream_entity_data" "pageviews" {
  store = deltastream_store.kafka_with_iam.name
  entity_path = ["pageviews"]
  num_rows = 10
}

resource "deltastream_query" "pageviews_5" {
  source_relation_fqns = [deltastream_relation.pageviews.fqn]
  sink_relation_fqn = deltastream_relation.pageviews_5.fqn
  sql = <<EOF
    INSERT INTO ${deltastream_relation.pageviews_5.fqn} SELECT * FROM ${data.deltastream_relation.pageviews.fqn} WHERE userid = 'USER_5';
  EOF
}

