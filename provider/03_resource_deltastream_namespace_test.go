// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/deltastreaminc/terraform-provider-deltastream/util"
	"github.com/hashicorp/terraform-plugin-testing/config"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestAccDeltaStreamnamespace(t *testing.T) {
	_, err := util.LoadTestEnv()
	if err != nil {
		t.Fatalf("Failed to load test environment: %v", err)
	}

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		Steps: []resource.TestStep{{
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/namespace.tf"),
			Check: resource.ComposeTestCheckFunc(
				resource.TestCheckResourceAttrPair("deltastream_namespace.ns1", "owner", "data.deltastream_namespace.ns1", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_namespace.ns1", "created_at", "data.deltastream_namespace.ns1", "created_at"),
				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					ns1Name := s.RootModule().Resources["deltastream_namespace.ns1"].Primary.Attributes["name"]
					ns2Name := s.RootModule().Resources["deltastream_namespace.ns2"].Primary.Attributes["name"]
					schNames := []string{ns1Name, ns2Name}

					listNames := []string{}
					r := regexp.MustCompile("items.[0-9]+.name")
					for k, v := range s.RootModule().Resources["data.deltastream_namespaces.all"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							listNames = append(listNames, v)
						}
					}

					if !util.ArrayContains(schNames, listNames) {
						return fmt.Errorf("namespace names not found in list: %v", listNames)
					}

					return nil
				}),
			),
		}},
	})
}
