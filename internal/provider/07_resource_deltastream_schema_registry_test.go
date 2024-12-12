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
		}, {
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/schema_registry_confluent.tf"),
			ConfigVariables: config.Variables{
				"region":                   config.StringVariable(creds["region"]),
				"schema_registry_uris":     config.StringVariable(creds["schema_registry_uris"]),
				"schema_registry_username": config.StringVariable("some_username"),
				"schema_registry_password": config.StringVariable("some_password"),
			},
			Check: resource.ComposeTestCheckFunc(
				resource.TestCheckResourceAttr("deltastream_schema_registry.confluent", "owner", "sysadmin"),
				resource.TestCheckResourceAttr("deltastream_schema_registry.confluent", "type", "Confluent"),
				resource.TestCheckResourceAttr("deltastream_schema_registry.confluent", "state", "ready"),

				resource.TestCheckResourceAttr("deltastream_schema_registry.confluent_nopwd", "owner", "sysadmin"),
				resource.TestCheckResourceAttr("deltastream_schema_registry.confluent_nopwd", "type", "Confluent"),
				resource.TestCheckResourceAttr("deltastream_schema_registry.confluent_nopwd", "state", "ready"),

				resource.TestCheckResourceAttrPair("deltastream_schema_registry.confluent", "owner", "data.deltastream_schema_registry.confluent", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_schema_registry.confluent", "type", "data.deltastream_schema_registry.confluent", "type"),
				resource.TestCheckResourceAttrPair("deltastream_schema_registry.confluent", "state", "data.deltastream_schema_registry.confluent", "state"),

				resource.TestCheckResourceAttrPair("deltastream_schema_registry.confluent_nopwd", "owner", "data.deltastream_schema_registry.confluent_nopwd", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_schema_registry.confluent_nopwd", "type", "data.deltastream_schema_registry.confluent_nopwd", "type"),
				resource.TestCheckResourceAttrPair("deltastream_schema_registry.confluent_nopwd", "state", "data.deltastream_schema_registry.confluent_nopwd", "state"),

				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					srName1 := s.RootModule().Resources["deltastream_schema_registry.confluent"].Primary.Attributes["name"]
					srName2 := s.RootModule().Resources["deltastream_schema_registry.confluent_nopwd"].Primary.Attributes["name"]
					srNames := []string{srName1, srName2}

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
