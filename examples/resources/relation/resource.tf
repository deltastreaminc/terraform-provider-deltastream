resource "deltastream_relation" "pageviews" {
  name = "pageviews"
  database = deltastream_database.example.name
  schema = "public"
  store = deltastream_store.default.name
  
  dsql = "CREATE STREAM PAGEVIEWS (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='pageviews', 'value.format'='json');"
}
