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

variable "pub_msk_uri" {
  type = string
}

variable "pub_msk_username" {
  type = string
}

variable "pub_msk_password" {
  type = string
}

resource "deltastream_store" "kafka_with_sasl" {
  name          = "Store_kafka_sasl_${random_id.suffix.hex}-东西"
  access_region = data.deltastream_region.region.name
  kafka = {
    uris               = var.pub_msk_uri
    sasl_hash_function = "SHA512"
    sasl_username      = var.pub_msk_username
    sasl_password      = var.pub_msk_password
    config = {
      "cleanup.policy"                      = "compact"
    }
  }
}


data "deltastream_stores" "all" {
  depends_on = [deltastream_store.kafka_with_sasl]
}

data "deltastream_store" "kafka_with_sasl" {
  name = deltastream_store.kafka_with_sasl.name
}

data "deltastream_entities" "kafka_with_sasl" {
  store = deltastream_store.kafka_with_sasl.name
}
