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
  name          = "Store_msk_iam_${random_id.suffix.hex}-东西"
  access_region = data.deltastream_region.region.name
  kafka = {
    uris               = var.pub_msk_iam_uri
    sasl_hash_function = "AWS_MSK_IAM"
    msk_iam_role_arn   = var.pub_msk_iam_role
    msk_aws_region     = var.pub_msk_region
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
