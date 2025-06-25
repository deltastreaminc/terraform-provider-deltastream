resource "deltastream_database" "example" {
  name = "example_database"
}

resource "deltastream_namespace" "example" {
  database = deltastream_database.example.name
  name     = "example_namespace"
}
