---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "deltastream_database Data Source - deltastream"
subcategory: ""
description: |-
  Database resource
---

# deltastream_database (Data Source)

Database resource

## Example Usage

```terraform
data "deltastream_database" "example" {
  name = "example_database"
}
```

<!-- schema generated by tfplugindocs -->
## Schema

### Required

- `name` (String) Name of the Database

### Optional

- `owner` (String) Owning role of the Database

### Read-Only

- `created_at` (String) Creation date of the Database