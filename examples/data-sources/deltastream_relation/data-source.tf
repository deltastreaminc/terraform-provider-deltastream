data "deltastream_object" "pageviews" {
  database  = deltastream_database.example.name
  namespace = "public"
  name      = "pageviews"
}
