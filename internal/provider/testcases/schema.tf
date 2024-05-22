provider "deltastream" {}

resource "random_id" "db1" {
  byte_length = 8
}

resource "random_id" "sch1" {
  byte_length = 8
}

resource "random_id" "sch2" {
  byte_length = 8
}

resource "deltastream_database" "db1" {
  name          = "schema_${random_id.db1.hex}"
}

resource "deltastream_schema" "sch1" {
  database      = deltastream_database.db1.name
  name          = "schema_${random_id.sch1.hex}"
}

resource "deltastream_schema" "sch2" {
  database      = deltastream_database.db1.name
  name          = "schema_${random_id.sch2.hex}"
}

data "deltastream_schema" "sch1" {
  database      = deltastream_database.db1.name
  name          = deltastream_schema.sch1.name
}

data "deltastream_schemas" "all" {
  database      = deltastream_database.db1.name
  depends_on    = [deltastream_schema.sch1, deltastream_schema.sch2]
}
