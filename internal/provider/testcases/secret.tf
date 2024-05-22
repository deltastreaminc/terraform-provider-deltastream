provider "deltastream" {}

variable "region" {
  type = string
}

data "deltastream_region" "region" {
  name = var.region
}

resource "random_id" "id1" {
  byte_length = 8
}

resource "random_id" "id2" {
  byte_length = 8
}

resource "deltastream_secret" "secret1" {
  name          = "secret_${random_id.id1.hex}"
  type          = "generic_string"
  description   = "secret description"
  access_region = data.deltastream_region.region.name
  string_value  = "some value"
}

resource "deltastream_secret" "secret2" {
  name          = "secret_${random_id.id2.hex}"
  type          = "generic_string"
  access_region = data.deltastream_region.region.name
  string_value  = "some value"
}

data "deltastream_secret" "secret1" {
  name = deltastream_secret.secret1.name
}

data "deltastream_secrets" "all" {
  depends_on = [deltastream_secret.secret1, deltastream_secret.secret2]
}
