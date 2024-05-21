data "deltastream_relation" "pageviews" {
  database = deltastream_database.example.name
  schema   = "public"
  name     = "pageviews"
}
