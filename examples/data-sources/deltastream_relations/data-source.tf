data "deltastream_relations" "all_in_example_public" {
  database = deltastream_database.example.name
  schema   = "public"
}
