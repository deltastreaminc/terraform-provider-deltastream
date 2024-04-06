resource "deltastream_store" "default" {
  name = "default"
  type = "kafka"
  properties = {
    "access_region" = "AWS us-east-1"
    "uris" = "b-1.mskbroker.awscloud.com:9098,b-2.mskbroker.awscloud.com:9098,b-3.mskbroker.awscloud.com:9098"
    "tls_disabled" = false
  }
}
