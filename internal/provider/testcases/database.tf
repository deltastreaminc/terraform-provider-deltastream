provider "deltastream" {}

resource "random_id" "id1" {
  byte_length = 8
}

resource "random_id" "id2" {
  byte_length = 8
}

resource "deltastream_database" "db1" {
  name          = "Database_${random_id.id1.hex}-东西"
}

resource "deltastream_database" "db2" {
  name          = "database_${random_id.id2.hex}"
}

data "deltastream_database" "db1" {
  name = deltastream_database.db1.name
}

data "deltastream_databases" "all" {
  depends_on = [deltastream_database.db1, deltastream_database.db2]
}
