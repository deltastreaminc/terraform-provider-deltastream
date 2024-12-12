# DeltaStream Terraform Provider

**Disclaimer: This project is still in alpha with a version of 0.x.x. Please refer to the [Go versioning guidelines](https://go.dev/doc/modules/version-numbers#v0-number) for more information on versioning.**

----

This project provides a Terraform provider for managing [DeltaStream](https://deltastream.io) resources.

## Getting started

Install the DeltaStream Terraform provider by adding a requirement and provider blocks:
```hcl
terraform {
  required_providers {
    deltastream = {
      source  = "deltastreaminc/deltastream"
      version = "~> 0.0.1"
    }
  }
}

provider "deltastream" {
  auth_token   = "your_auth_token_here"
  organization = "your_organization_name_here"
  role         = "sysadmin"
}
```

For more information on provider configuration see the [provider docs on the Terraform registry](https://registry.terraform.io/providers/deltastreaminc/deltastream/latest/docs).
