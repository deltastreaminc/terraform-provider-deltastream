resource "deltastream_secret" "example" {
  name         = "example_secret"
  type         = "generic_string"
  description  = "secret description"
  string_value = "some value"
}
