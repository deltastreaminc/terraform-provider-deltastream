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

func TestAccDeltaStreamDatabase(t *testing.T) {
	_, err := util.LoadTestEnv()
	if err != nil {
		t.Fatalf("Failed to load test environment: %v", err)
	}

	resource.UnitTest(t, resource.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		Steps: []resource.TestStep{{
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/database.tf"),
			Check: resource.ComposeTestCheckFunc(
				resource.TestCheckResourceAttrPair("deltastream_database.db1", "owner", "data.deltastream_database.db1", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_database.db1", "created_at", "data.deltastream_database.db1", "created_at"),
				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					db1Name := s.RootModule().Resources["deltastream_database.db1"].Primary.Attributes["name"]
					db2Name := s.RootModule().Resources["deltastream_database.db2"].Primary.Attributes["name"]
					dbNames := []string{db1Name, db2Name}

					listNames := []string{}
					r := regexp.MustCompile("items.[0-9]+.name")
					for k, v := range s.RootModule().Resources["data.deltastream_databases.all"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							listNames = append(listNames, v)
						}
					}

					if !util.ArrayContains(dbNames, listNames) {
						return fmt.Errorf("Database names not found in list: %v", listNames)
					}

					return nil
				}),
			),
		}},
	})
}
