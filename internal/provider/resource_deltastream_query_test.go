// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"testing"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"github.com/hashicorp/terraform-plugin-testing/config"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccDeltaStreamQuery(t *testing.T) {
	creds, err := util.LoadTestEnv()
	if err != nil {
		t.Fatalf("Failed to load test environment: %v", err)
	}

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		Steps: []resource.TestStep{{
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/query_msk_iam.tf"),
			ConfigVariables: config.Variables{
				"msk_url":      config.StringVariable(creds["msk-uri"]),
				"msk_iam_role": config.StringVariable(creds["msk-iam-role"]),
				"msk_region":   config.StringVariable(creds["msk-region"]),
			},
			Check: resource.ComposeTestCheckFunc(
				// resources
				resource.TestCheckResourceAttr("deltastream_store.kafka_with_iam", "state", "ready"),

				// datasource
				// resource.TestCheckResourceAttr("data.deltastream_entity_data.pageviews_6", "rows.#", "3"),
			),
			// }, {
			// 	ProtoV6ProviderFactories: testAccProviders,
			// 	ConfigFile:               config.StaticFile("testcases/query_kinesis.tf"),
			// 	ConfigVariables: config.Variables{
			// 		"msk_url":      config.StringVariable(creds["msk-uri"]),
			// 		"msk_iam_role": config.StringVariable(creds["msk-iam-role"]),
			// 		"msk_region":   config.StringVariable(creds["msk-region"]),

			// 		"kinesis_url":    config.StringVariable(creds["kinesis-uri"]),
			// 		"kinesis_region": config.StringVariable(creds["kinesis-az"]),
			// 		"kinesis_key":    config.StringVariable(creds["kinesis-key-id"]),
			// 		"kinesis_secret": config.StringVariable(creds["kinesis-access-key"]),
			// 	},
			// 	Check: resource.ComposeTestCheckFunc(
			// 		// resources
			// 		resource.TestCheckResourceAttr("deltastream_store.kafka_with_iam", "state", "ready"),
			// 		resource.TestCheckResourceAttr("deltastream_store.kinesis_creds", "state", "ready"),

			// 		// datasource
			// 		resource.TestCheckResourceAttr("data.deltastream_entity_data.pageviews_6", "rows.#", "3"),
			// 	),
		}},
	})
}
