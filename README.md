# DeltaStream Terraform Provider

----

This project provides a Terraform provider for managing [DeltaStream](https://deltastream.io) resources.

## Getting started

Install the DeltaStream Terraform provider by adding a requirement and provider blocks:
```hcl
terraform {
  required_providers {
    deltastream = {
      source  = "deltastreaminc/deltastream"
      version = "~> 1.0.0"
    }
  }
}

provider "deltastream" {
  auth_token   = "your_auth_token_here"
}
```

For more information on provider configuration see the [provider docs on the Terraform registry](https://registry.terraform.io/providers/deltastreaminc/deltastream/latest/docs).
