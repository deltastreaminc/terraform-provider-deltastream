---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "deltastream_secret Resource - deltastream"
subcategory: ""
description: |-
  Secret resource
---

# deltastream_secret (Resource)

Secret resource

## Example Usage

```terraform
resource "deltastream_secret" "example" {
  name         = "example_secret"
  type         = "generic_string"
  description  = "secret description"
  string_value = "some value"
}
```

<!-- schema generated by tfplugindocs -->
## Schema

### Required

- `name` (String) Name of the Secret
- `type` (String) Secret type. (Valid values: generic_string)

### Optional

- `custom_properties` (Map of String) Custom properties of the Secret
- `description` (String) Description of the Secret
- `owner` (String) Owning role of the Secret
- `string_value` (String) Secret value

### Read-Only

- `created_at` (String) Creation date of the Secret
- `status` (String) Status of the Secret
- `updated_at` (String) Last update date of the Secret
