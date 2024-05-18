resource "deltastream_secret" "example" {
  name          = "example_secret"
  type          = "generic_string"
  description   = "secret description"
  access_region = "AWS us-east-1"
  string_value  = "some value"
}
