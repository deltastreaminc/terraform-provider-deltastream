resource "deltastream_object" "pageviews" {
  database  = deltastream_database.example.name
  namespace = "public"
  store     = deltastream_store.kafka.name
  sql       = <<EOF
    CREATE STREAM PAGEVIEWS (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='pageviews', 'value.format'='json');
  EOF
}

resource "deltastream_object" "user_last_page" {
  database  = deltastream_database.example.name
  namespace = "public"
  store     = deltastream_store.kafka_with_iam.name
  sql       = <<EOF
    CREATE CHANGELOG user_last_page (viewtime BIGINT, userid VARCHAR, pageid VARCHAR, PRIMARY KEY(userid)) WITH ('topic'='pageviews', 'value.format'='json');
  EOF
}
