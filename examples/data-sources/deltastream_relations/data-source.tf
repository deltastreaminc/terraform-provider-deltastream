data "deltastream_objects" "all_in_example_public" {
  database  = deltastream_database.example.name
  namespace = "public"
}
