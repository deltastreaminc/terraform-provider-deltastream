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

func TestAccDeltaRelationStore(t *testing.T) {
	creds, err := util.LoadTestEnv()
	if err != nil {
		t.Fatalf("Failed to load test environment: %v", err)
	}

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		Steps: []resource.TestStep{{
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/relation.tf"),
			ConfigVariables: config.Variables{
				"region":           config.StringVariable(creds["region"]),
				"pub_msk_iam_uri":  config.StringVariable(creds["pub_msk_iam_uri"]),
				"pub_msk_iam_role": config.StringVariable(creds["pub_msk_iam_role"]),
				"pub_msk_region":   config.StringVariable(creds["pub_msk_region"]),
			},
			Check: resource.ComposeTestCheckFunc(
				resource.TestCheckResourceAttr("deltastream_relation.pageviews", "owner", "sysadmin"),
				resource.TestCheckResourceAttr("deltastream_relation.pageviews", "type", "stream"),
				resource.TestCheckResourceAttr("deltastream_relation.pageviews", "state", "created"),

				resource.TestCheckResourceAttr("deltastream_relation.user_last_page", "owner", "sysadmin"),
				resource.TestCheckResourceAttr("deltastream_relation.user_last_page", "type", "changelog"),
				resource.TestCheckResourceAttr("deltastream_relation.user_last_page", "state", "created"),

				resource.TestCheckResourceAttrPair("deltastream_relation.pageviews", "owner", "data.deltastream_relation.pageviews", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_relation.pageviews", "type", "data.deltastream_relation.pageviews", "type"),
				resource.TestCheckResourceAttrPair("deltastream_relation.pageviews", "state", "data.deltastream_relation.pageviews", "state"),
				resource.TestCheckResourceAttrPair("deltastream_relation.pageviews", "created_at", "data.deltastream_relation.pageviews", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_relation.pageviews", "updated_at", "data.deltastream_relation.pageviews", "updated_at"),

				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					rel1Name := s.RootModule().Resources["deltastream_relation.pageviews"].Primary.Attributes["fqn"]
					rel2Name := s.RootModule().Resources["deltastream_relation.user_last_page"].Primary.Attributes["fqn"]
					relNames := []string{rel1Name, rel2Name}

					listNames := []string{}
					r := regexp.MustCompile("relations.[0-9]+.fqn")
					for k, v := range s.RootModule().Resources["data.deltastream_relations.all"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							listNames = append(listNames, v)
						}
					}

					if !util.ArrayContains(relNames, listNames) {
						return fmt.Errorf("Relation names not found in list: %v", listNames)
					}

					return nil
				}),
			),
		}},
	})
}
