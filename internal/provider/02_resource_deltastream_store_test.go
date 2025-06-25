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

func TestAccDeltaStreamStore(t *testing.T) {
	creds, err := util.LoadTestEnv()
	if err != nil {
		t.Fatalf("Failed to load test environment: %v", err)
	}

	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		Steps: []resource.TestStep{{
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/store_kafka_sasl.tf"),
			ConfigVariables: config.Variables{
				"region":           config.StringVariable(creds["region"]),
				"pub_msk_uri":      config.StringVariable(creds["pub_msk_uri"]),
				"pub_msk_username": config.StringVariable(creds["pub_msk_username"]),
				"pub_msk_password": config.StringVariable(creds["pub_msk_password"]),
			},
			Check: resource.ComposeTestCheckFunc(
				// resources
				resource.TestCheckResourceAttr("deltastream_store.kafka_with_sasl", "state", "ready"),
				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					s1Name := s.RootModule().Resources["deltastream_store.kafka_with_sasl"].Primary.Attributes["name"]
					sNames := []string{s1Name}

					listNames := []string{}
					r := regexp.MustCompile("items.[0-9]+.name")
					for k, v := range s.RootModule().Resources["data.deltastream_stores.all"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							listNames = append(listNames, v)
						}
					}

					if !util.ArrayContains(sNames, listNames) {
						return fmt.Errorf("Store names %v not found in list: %v", sNames, listNames)
					}

					return nil
				}),

				// datasource
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "type", "data.deltastream_store.kafka_with_sasl", "type"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "owner", "data.deltastream_store.kafka_with_sasl", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "state", "data.deltastream_store.kafka_with_sasl", "state"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "updated_at", "data.deltastream_store.kafka_with_sasl", "updated_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "created_at", "data.deltastream_store.kafka_with_sasl", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "kafka.uris", "data.deltastream_store.kafka_with_sasl", "kafka.uris"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "kafka.tls_disabled", "data.deltastream_store.kafka_with_sasl", "kafka.tls_disabled"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "kafka.tls_verify_server_hostname", "data.deltastream_store.kafka_with_sasl", "kafka.tls_verify_server_hostname"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "kafka.schema_registry_name", "data.deltastream_store.kafka_with_sasl", "kafka.schema_registry_name"),

				// child entities
				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					topicNames := []string{}
					r := regexp.MustCompile("child_entities.[0-9]+")
					for k, v := range s.RootModule().Resources["data.deltastream_entities.kafka_with_sasl"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							topicNames = append(topicNames, v)
						}
					}

					expectedTopics := []string{"ds_pageviews"}
					if !util.ArrayContains(expectedTopics, topicNames) {
						return fmt.Errorf("Topic names %v not found in list: %v", expectedTopics, topicNames)
					}

					return nil
				}),
			),
		}, {
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/store_msk_iam.tf"),
			ConfigVariables: config.Variables{
				"region":           config.StringVariable(creds["region"]),
				"pub_msk_iam_uri":  config.StringVariable(creds["pub_msk_iam_uri"]),
				"pub_msk_iam_role": config.StringVariable(creds["pub_msk_iam_role"]),
				"pub_msk_region":   config.StringVariable(creds["pub_msk_region"]),
			},
			Check: resource.ComposeTestCheckFunc(
				// resources
				resource.TestCheckResourceAttr("deltastream_store.kafka_with_iam", "state", "ready"),
				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					s3Name := s.RootModule().Resources["deltastream_store.kafka_with_iam"].Primary.Attributes["name"]
					sNames := []string{s3Name}

					listNames := []string{}
					r := regexp.MustCompile("items.[0-9]+.name")
					for k, v := range s.RootModule().Resources["data.deltastream_stores.all"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							listNames = append(listNames, v)
						}
					}

					if !util.ArrayContains(sNames, listNames) {
						return fmt.Errorf("Store names %v not found in list: %v", sNames, listNames)
					}

					return nil
				}),

				// datasource
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "type", "data.deltastream_store.kafka_with_iam", "type"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "owner", "data.deltastream_store.kafka_with_iam", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "state", "data.deltastream_store.kafka_with_iam", "state"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "updated_at", "data.deltastream_store.kafka_with_iam", "updated_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "created_at", "data.deltastream_store.kafka_with_iam", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "kafka.uris", "data.deltastream_store.kafka_with_iam", "kafka.uris"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "kafka.tls_disabled", "data.deltastream_store.kafka_with_iam", "kafka.tls_disabled"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "kafka.tls_verify_server_hostname", "data.deltastream_store.kafka_with_iam", "kafka.tls_verify_server_hostname"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "kafka.schema_registry_name", "data.deltastream_store.kafka_with_iam", "kafka.schema_registry_name"),

				// create topic
				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					entNames := []string{}
					r := regexp.MustCompile("child_entities.[0-9]+")
					for k, v := range s.RootModule().Resources["data.deltastream_entities.all"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							entNames = append(entNames, v)
						}
					}

					expectedEnt := []string{s.RootModule().Resources["deltastream_entity.test_topic"].Primary.Attributes["entity_path.0"]}
					if !util.ArrayContains(expectedEnt, entNames) {
						return fmt.Errorf("Test topic name %v not found in list: %v", expectedEnt, entNames)
					}

					return nil
				}),
			),
		}, {
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/store_kinesis.tf"),
			ConfigVariables: config.Variables{
				"region":             config.StringVariable(creds["region"]),
				"kinesis_url":        config.StringVariable(creds["kinesis_uri"]),
				"kinesis_region":     config.StringVariable(creds["kinesis_region"]),
				"kinesis_key":        config.StringVariable(creds["kinesis_key"]),
				"kinesis_secret":     config.StringVariable(creds["kinesis_secret"]),
				"kinesis_account_id": config.StringVariable(creds["kinesis_account_id"]),
			},
			Check: resource.ComposeTestCheckFunc(
				// resources
				resource.TestCheckResourceAttr("deltastream_store.kinesis_creds", "state", "ready"),
				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					s4Name := s.RootModule().Resources["deltastream_store.kinesis_creds"].Primary.Attributes["name"]
					sNames := []string{s4Name}

					listNames := []string{}
					r := regexp.MustCompile("items.[0-9]+.name")
					for k, v := range s.RootModule().Resources["data.deltastream_stores.all"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							listNames = append(listNames, v)
						}
					}

					if !util.ArrayContains(sNames, listNames) {
						return fmt.Errorf("Store names %v not found in list: %v", sNames, listNames)
					}

					return nil
				}),

				// datasource
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "type", "data.deltastream_store.kinesis_creds", "type"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "owner", "data.deltastream_store.kinesis_creds", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "state", "data.deltastream_store.kinesis_creds", "state"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "updated_at", "data.deltastream_store.kinesis_creds", "updated_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "created_at", "data.deltastream_store.kinesis_creds", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "kinesis.uris", "data.deltastream_store.kinesis_creds", "kinesis.uris"),
			),
			// }, {
			// 	ProtoV6ProviderFactories: testAccProviders,
			// 	ConfigFile:               config.StaticFile("testcases/store_snowflake.tf"),
			// 	ConfigVariables: config.Variables{
			// 		"region":                          config.StringVariable(creds["region"]),
			// 		"snowflake_uris":                  config.StringVariable(creds["snowflake_uri"]),
			// 		"snowflake_account_id":            config.StringVariable(creds["snowflake_account_id"]),
			// 		"snowflake_cloud_region":          config.StringVariable(creds["snowflake_cloud_region"]),
			// 		"snowflake_role_name":             config.StringVariable(creds["snowflake_role_name"]),
			// 		"snowflake_username":              config.StringVariable(creds["snowflake_username"]),
			// 		"snowflake_warehouse_name":        config.StringVariable(creds["snowflake_warehouse_name"]),
			// 		"snowflake_client_key_file":       config.StringVariable(string(util.Must(base64.StdEncoding.DecodeString(creds["snowflake_client_key_file"])))),
			// 		"snowflake_client_key_passphrase": config.StringVariable(""),
			// 	},
			// 	Check: resource.ComposeTestCheckFunc(
			// 		// resources
			// 		resource.TestCheckResourceAttr("deltastream_store.snowflake", "state", "ready"),
			// 		resource.ComposeTestCheckFunc(func(s *terraform.State) error {
			// 			s6Name := s.RootModule().Resources["deltastream_store.snowflake"].Primary.Attributes["name"]
			// 			sNames := []string{s6Name}

			// 			listNames := []string{}
			// 			r := regexp.MustCompile("items.[0-9]+.name")
			// 			for k, v := range s.RootModule().Resources["data.deltastream_stores.all"].Primary.Attributes {
			// 				if ok := r.MatchString(k); ok {
			// 					listNames = append(listNames, v)
			// 				}
			// 			}

			// 			if !util.ArrayContains(sNames, listNames) {
			// 				return fmt.Errorf("Store names %v not found in list: %v", sNames, listNames)
			// 			}

			// 			return nil
			// 		}),

			// 		// datasource
			// 		resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "type", "data.deltastream_store.snowflake", "type"),
			// 		resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "owner", "data.deltastream_store.snowflake", "owner"),
			// 		resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "state", "data.deltastream_store.snowflake", "state"),
			// 		resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "updated_at", "data.deltastream_store.snowflake", "updated_at"),
			// 		resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "created_at", "data.deltastream_store.snowflake", "created_at"),
			// 		resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "snowflake.uris", "data.deltastream_store.snowflake", "snowflake.uris"),
			// 		resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "snowflake.account_id", "data.deltastream_store.snowflake", "snowflake.account_id"),
			// 		resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "snowflake.warehouse_name", "data.deltastream_store.snowflake", "snowflake.warehouse_name"),
			// 		resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "snowflake.role_name", "data.deltastream_store.snowflake", "snowflake.role_name"),
			// 	),
		}},
	})
}
