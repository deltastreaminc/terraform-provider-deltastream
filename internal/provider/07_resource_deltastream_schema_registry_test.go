// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"github.com/hashicorp/terraform-plugin-testing/config"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestAccDeltaSchemaRegistry(t *testing.T) {
	creds, err := util.LoadTestEnv()
	if err != nil {
		t.Fatalf("Failed to load test environment: %v", err)
	}

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		Steps: []resource.TestStep{{
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/schema_registry_confluent_cloud.tf"),
			ConfigVariables: config.Variables{
				"region":           config.StringVariable(creds["region"]),
				"pub_msk_iam_uri":  config.StringVariable(creds["pub_msk_iam_uri"]),
				"pub_msk_iam_role": config.StringVariable(creds["pub_msk_iam_role"]),
				"pub_msk_region":   config.StringVariable(creds["pub_msk_region"]),

				"schema_registry_uris":   config.StringVariable(creds["schema_registry_uris"]),
				"schema_registry_key":    config.StringVariable(creds["schema_registry_key"]),
				"schema_registry_secret": config.StringVariable(creds["schema_registry_secret"]),
			},
			Check: resource.ComposeTestCheckFunc(
				resource.TestCheckResourceAttr("deltastream_schema_registry.confluent_cloud", "owner", "sysadmin"),
				resource.TestCheckResourceAttr("deltastream_schema_registry.confluent_cloud", "type", "ConfluentCloud"),
				resource.TestCheckResourceAttr("deltastream_schema_registry.confluent_cloud", "state", "ready"),

				resource.TestCheckResourceAttrPair("deltastream_schema_registry.confluent_cloud", "owner", "data.deltastream_schema_registry.confluent_cloud", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_schema_registry.confluent_cloud", "type", "data.deltastream_schema_registry.confluent_cloud", "type"),
				resource.TestCheckResourceAttrPair("deltastream_schema_registry.confluent_cloud", "state", "data.deltastream_schema_registry.confluent_cloud", "state"),

				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					srName := s.RootModule().Resources["deltastream_schema_registry.confluent_cloud"].Primary.Attributes["name"]
					srNames := []string{srName}

					listNames := []string{}
					r := regexp.MustCompile("items.[0-9]+.name")
					for k, v := range s.RootModule().Resources["data.deltastream_schema_registries.all"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							listNames = append(listNames, v)
						}
					}

					if !util.ArrayContains(srNames, listNames) {
						return fmt.Errorf("Schema registry names %v not found in list: %v", srNames, listNames)
					}

					return nil
				}),
			),
		}},
	})
}
