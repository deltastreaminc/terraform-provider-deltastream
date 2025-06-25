// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"fmt"
	"regexp"
	"sort"
	"testing"

	"github.com/deltastreaminc/terraform-provider-deltastream/util"
	"github.com/hashicorp/terraform-plugin-testing/config"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestAccDeltaStreamSecret(t *testing.T) {
	creds, err := util.LoadTestEnv()
	if err != nil {
		t.Fatalf("Failed to load test environment: %v", err)
	}

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		Steps: []resource.TestStep{{
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/secret.tf"),
			ConfigVariables: config.Variables{
				"region": config.StringVariable(creds["region"]),
			},
			Check: resource.ComposeTestCheckFunc(
				resource.TestCheckResourceAttrPair("deltastream_secret.secret1", "type", "data.deltastream_secret.secret1", "type"),
				resource.TestCheckResourceAttrPair("deltastream_secret.secret1", "description", "data.deltastream_secret.secret1", "description"),
				resource.TestCheckResourceAttrPair("deltastream_secret.secret1", "created_at", "data.deltastream_secret.secret1", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_secret.secret1", "updated_at", "data.deltastream_secret.secret1", "updated_at"),
				resource.TestCheckResourceAttr("data.deltastream_secret.secret1", "status", "ready"),
				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					s1Name := s.RootModule().Resources["deltastream_secret.secret1"].Primary.Attributes["name"]
					s2Name := s.RootModule().Resources["deltastream_secret.secret2"].Primary.Attributes["name"]
					sNames := []string{s1Name, s2Name}
					sort.Strings(sNames)

					listNames := []string{}
					r := regexp.MustCompile("items.[0-9]+.name")
					for k, v := range s.RootModule().Resources["data.deltastream_secrets.all"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							listNames = append(listNames, v)
						}
					}

					if !util.ArrayContains(sNames, listNames) {
						return fmt.Errorf("Secret names not found in list: %v", listNames)
					}

					return nil
				}),
			),
		}},
	})
}
