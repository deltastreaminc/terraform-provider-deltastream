provider "deltastream" {}

data "deltastream_regions" "all" {}

resource "random_id" "suffix" {
  byte_length = 4
}

variable "kafka_url" {
  type = string
}

variable "kafka_sasl_username" {
  type = string
}

variable "kafka_sasl_password" {
  type = string
}

resource "deltastream_store" "kafka_with_sasl" {
  name          = "kafka_with_sasl_${random_id.suffix.hex}"
  access_region = data.deltastream_regions.all.items[0].name
  kafka = {
    uris               = var.kafka_url
    sasl_hash_function = "PLAIN"
    sasl_username      = var.kafka_sasl_username
    sasl_password      = var.kafka_sasl_password
  }
}

resource "deltastream_store" "confluent_kafka_with_sasl" {
  name          = "confluent_kafka_with_sasl_${random_id.suffix.hex}"
  access_region = data.deltastream_regions.all.items[0].name
  confluent_kafka = {
    uris               = var.kafka_url
    sasl_hash_function = "PLAIN"
    sasl_username      = var.kafka_sasl_username
    sasl_password      = var.kafka_sasl_password
  }
}

data "deltastream_stores" "all" {
  depends_on = [deltastream_store.kafka_with_sasl, deltastream_store.confluent_kafka_with_sasl]
}

data "deltastream_store" "kafka_with_sasl" {
  name = deltastream_store.kafka_with_sasl.name
}

data "deltastream_store" "confluent_kafka_with_sasl" {
  name = deltastream_store.confluent_kafka_with_sasl.name
}

data "deltastream_entities" "confluent_kafka_with_sasl" {
  store = deltastream_store.confluent_kafka_with_sasl.name
}
