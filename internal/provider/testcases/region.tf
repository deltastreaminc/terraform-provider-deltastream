provider "deltastream" {}

data "deltastream_regions" "all" {
}

data "deltastream_region" "region1" {
  name = data.deltastream_regions.all.items[0].name
}
