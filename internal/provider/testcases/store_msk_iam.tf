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

data "deltastream_stores" "all" {
  depends_on = [deltastream_store.kafka_with_iam]
}

data "deltastream_store" "kafka_with_iam" {
  name = deltastream_store.kafka_with_iam.name
}

resource "deltastream_entity" "test_topic" {
  store = data.deltastream_store.kafka_with_iam.name
  entity_path = ["test_topic_${random_id.suffix.hex}"]
}

data "deltastream_entities" "all" {
  depends_on = [deltastream_entity.test_topic]
  store = data.deltastream_store.kafka_with_iam.name
}