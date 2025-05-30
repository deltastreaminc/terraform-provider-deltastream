provider "deltastream" {}

resource "random_id" "db1" {
  byte_length = 8
}

resource "random_id" "ns1" {
  byte_length = 8
}

resource "random_id" "ns2" {
  byte_length = 8
}

resource "deltastream_database" "db1" {
  name          = "Namespace_${random_id.db1.hex}-东西"
}

resource "deltastream_namespace" "ns1" {
  database      = deltastream_database.db1.name
  name          = "namespace_${random_id.ns1.hex}"
}

resource "deltastream_namespace" "ns2" {
  database      = deltastream_database.db1.name
  name          = "namespace_${random_id.ns2.hex}"
}

data "deltastream_namespace" "ns1" {
  database      = deltastream_database.db1.name
  name          = deltastream_namespace.ns1.name
}

data "deltastream_namespaces" "all" {
  database      = deltastream_database.db1.name
  depends_on    = [deltastream_namespace.ns1, deltastream_namespace.ns2]
}
