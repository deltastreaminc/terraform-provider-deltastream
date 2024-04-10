terraform {
  required_providers {
    deltastream = {
      source = "registry.terraform.io/hashicorp/deltastream"
    }
  }
}

provider "deltastream" {
  api_key = "eyJhbGciOiJFUzI1NiIsImtpZCI6MSwidHlwIjoiSldUIn0.eyJpc3MiOiJkZWx0YXN0cmVhbS5pbyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTE3NTAwMjY3NjczMzMwODIxNzk0IiwiZXhwIjoxNzIwMTE4OTk2LCJpYXQiOjE3MTIzNDI5OTYsImp0aSI6IjQ4ODIxNWU2LTA3NTEtNDgzZS1hMGZiLWExNjM4MGM4YmU0YiIsInN0YXRlbWVudElEIjoiMDAwMDAwMDAtMDAwMC0wMDAwLTAwMDAtMDAwMDAwMDAwMDAwIn0.X_LshsYG0PIMFlH3XVw9KERScdg8TPJU1LxX3snD62IRpxCPC6WlB3tmiHU9_oH8_MLkXCxm2ZqwRKZGybqAcQ"
  server = "https://api.local.deltastream.io/v2"
  insecure_skip_verify = true
  organization = "o1"
  role = "sysadmin"
}

resource "deltastream_database" "analytics" {
  name = "analytics"
}

resource "deltastream_store" "default" {
  name = "test"
  type = "kafka"
  properties = {
    "access_region" = "K3D us-east-1"
    "uris" = "kafka-dp.docker.local:9092"
    "tls_disabled" = true
  }
}

resource "deltastream_relation" "pageviews" {
  name = "pageviews"
  database = deltastream_database.analytics.name
  schema = "public"
  store = deltastream_store.default.name
  
  dsql = "CREATE STREAM PAGEVIEWS (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='pageviews', 'value.format'='json');"
}

resource "deltastream_relation" "pageviews_6" {
  name = "pageviews"
  database = deltastream_database.analytics.name
  schema = "public"
  store = deltastream_store.default.name
  
  dsql = "CREATE STREAM PAGEVIEWS_6 (viewtime BIGINT, userid VARCHAR, pageid VARCHAR) WITH ('topic'='pageviews', 'value.format'='json');"
}

resource "deltastream_query" "query1" {
  name = "query1"
  relations = [deltastream_stream.pageviews.name, deltastream_stream.pageviews_6.name]

  sql = "INSERT INTO pageviews_6 SELECT * from pageviews;"
}
