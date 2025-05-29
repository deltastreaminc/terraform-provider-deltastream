provider "deltastream" {}

resource "random_id" "id1" {
  byte_length = 8
}

resource "random_id" "id2" {
  byte_length = 8
}

resource "deltastream_secret" "secret1" {
  name          = "Secret_${random_id.id1.hex}-东西"
  type          = "generic_string"
  description   = "secret description"
  string_value  = "some value"
}

resource "deltastream_secret" "secret2" {
  name          = "secret_${random_id.id2.hex}"
  type          = "generic_string"
  string_value  = "some value"
}

data "deltastream_secret" "secret1" {
  name = deltastream_secret.secret1.name
}

data "deltastream_secrets" "all" {
  depends_on = [deltastream_secret.secret1, deltastream_secret.secret2]
}
