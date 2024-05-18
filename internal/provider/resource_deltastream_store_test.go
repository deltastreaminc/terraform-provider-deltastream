// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"encoding/base64"
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

	resource.UnitTest(t, resource.TestCase{
		PreCheck: func() { testAccPreCheck(t) },
		Steps: []resource.TestStep{{
			ProtoV6ProviderFactories: testAccProviders,
			ConfigFile:               config.StaticFile("testcases/store.tf"),
			ConfigVariables: config.Variables{
				"kafka_url":           config.StringVariable(creds["confluent-kafka-uri"]),
				"kafka_sasl_username": config.StringVariable(creds["confluent-kafka-username"]),
				"kafka_sasl_password": config.StringVariable(creds["confluent-kafka-password"]),

				"msk_url":      config.StringVariable(creds["msk-uri"]),
				"msk_iam_role": config.StringVariable(creds["msk-iam-role"]),
				"msk_region":   config.StringVariable(creds["msk-region"]),

				"kinesis_url":    config.StringVariable(creds["kinesis-uri"]),
				"kinesis_region": config.StringVariable(creds["kinesis-az"]),
				"kinesis_key":    config.StringVariable(creds["kinesis-key-id"]),
				"kinesis_secret": config.StringVariable(creds["kinesis-access-key"]),

				"databricks_uri":               config.StringVariable(creds["databricks-uri"]),
				"databricks_app_token":         config.StringVariable(creds["databricks-app-token"]),
				"databricks_warehouse_id":      config.StringVariable(creds["databricks-warehouse-id"]),
				"databricks_access_key_id":     config.StringVariable(creds["databricks-access-key-id"]),
				"databricks_secret_access_key": config.StringVariable(creds["databricks-secret-access-key"]),
				"databricks_bucket":            config.StringVariable(creds["databricks-bucket"]),
				"databricks_bucket_region":     config.StringVariable(creds["databricks-bucket-region"]),

				"snowflake_uris":                  config.StringVariable(creds["snowflake-uri"]),
				"snowflake_account_id":            config.StringVariable(creds["snowflake-account-id"]),
				"snowflake_cloud_region":          config.StringVariable(creds["snowflake-cloud-region"]),
				"snowflake_warehouse_name":        config.StringVariable(creds["snowflake-warehouse-name"]),
				"snowflake_role_name":             config.StringVariable(creds["snowflake-role-name"]),
				"snowflake_username":              config.StringVariable(creds["snowflake-username"]),
				"snowflake_client_key_file":       config.StringVariable(string(util.Must(base64.StdEncoding.DecodeString(creds["snowflake-client-key-file"])))),
				"snowflake_client_key_passphrase": config.StringVariable(""),

				"postgres_uris":     config.StringVariable(""),
				"postgres_username": config.StringVariable(""),
				"postgres_password": config.StringVariable(""),
			},
			Check: resource.ComposeTestCheckFunc(
				// resources
				resource.TestCheckResourceAttr("deltastream_store.kafka_with_sasl", "state", "ready"),
				resource.TestCheckResourceAttr("deltastream_store.confluent_kafka_with_sasl", "state", "ready"),
				resource.TestCheckResourceAttr("deltastream_store.kafka_with_iam", "state", "ready"),
				resource.TestCheckResourceAttr("deltastream_store.kinesis_creds", "state", "ready"),
				resource.TestCheckResourceAttr("deltastream_store.databricks", "state", "ready"),
				resource.TestCheckResourceAttr("deltastream_store.snowflake", "state", "ready"),
				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					s1Name := s.RootModule().Resources["deltastream_store.kafka_with_sasl"].Primary.Attributes["name"]
					s2Name := s.RootModule().Resources["deltastream_store.confluent_kafka_with_sasl"].Primary.Attributes["name"]
					s3Name := s.RootModule().Resources["deltastream_store.kafka_with_iam"].Primary.Attributes["name"]
					s4Name := s.RootModule().Resources["deltastream_store.kinesis_creds"].Primary.Attributes["name"]
					s5Name := s.RootModule().Resources["deltastream_store.databricks"].Primary.Attributes["name"]
					s6Name := s.RootModule().Resources["deltastream_store.snowflake"].Primary.Attributes["name"]
					sNames := []string{s1Name, s2Name, s3Name, s4Name, s5Name, s6Name}

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
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "access_region", "data.deltastream_store.kafka_with_sasl", "access_region"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "type", "data.deltastream_store.kafka_with_sasl", "type"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "owner", "data.deltastream_store.kafka_with_sasl", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "state", "data.deltastream_store.kafka_with_sasl", "state"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "updated_at", "data.deltastream_store.kafka_with_sasl", "updated_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "created_at", "data.deltastream_store.kafka_with_sasl", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "kafka.uris", "data.deltastream_store.kafka_with_sasl", "kafka.uris"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "kafka.tls_disabled", "data.deltastream_store.kafka_with_sasl", "kafka.tls_disabled"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "kafka.tls_verify_server_hostname", "data.deltastream_store.kafka_with_sasl", "kafka.tls_verify_server_hostname"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_sasl", "kafka.schema_registry_name", "data.deltastream_store.kafka_with_sasl", "kafka.schema_registry_name"),

				resource.TestCheckResourceAttrPair("deltastream_store.confluent_kafka_with_sasl", "access_region", "data.deltastream_store.confluent_kafka_with_sasl", "access_region"),
				resource.TestCheckResourceAttrPair("deltastream_store.confluent_kafka_with_sasl", "type", "data.deltastream_store.confluent_kafka_with_sasl", "type"),
				resource.TestCheckResourceAttrPair("deltastream_store.confluent_kafka_with_sasl", "owner", "data.deltastream_store.confluent_kafka_with_sasl", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_store.confluent_kafka_with_sasl", "state", "data.deltastream_store.confluent_kafka_with_sasl", "state"),
				resource.TestCheckResourceAttrPair("deltastream_store.confluent_kafka_with_sasl", "updated_at", "data.deltastream_store.confluent_kafka_with_sasl", "updated_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.confluent_kafka_with_sasl", "created_at", "data.deltastream_store.confluent_kafka_with_sasl", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.confluent_kafka_with_sasl", "confluent_kafka.uris", "data.deltastream_store.confluent_kafka_with_sasl", "confluent_kafka.uris"),
				resource.TestCheckResourceAttrPair("deltastream_store.confluent_kafka_with_sasl", "confluent_kafka.schema_registry_name", "data.deltastream_store.confluent_kafka_with_sasl", "confluent_kafka.schema_registry_name"),

				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "access_region", "data.deltastream_store.kafka_with_iam", "access_region"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "type", "data.deltastream_store.kafka_with_iam", "type"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "owner", "data.deltastream_store.kafka_with_iam", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "state", "data.deltastream_store.kafka_with_iam", "state"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "updated_at", "data.deltastream_store.kafka_with_iam", "updated_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "created_at", "data.deltastream_store.kafka_with_iam", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "kafka.uris", "data.deltastream_store.kafka_with_iam", "kafka.uris"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "kafka.tls_disabled", "data.deltastream_store.kafka_with_iam", "kafka.tls_disabled"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "kafka.tls_verify_server_hostname", "data.deltastream_store.kafka_with_iam", "kafka.tls_verify_server_hostname"),
				resource.TestCheckResourceAttrPair("deltastream_store.kafka_with_iam", "kafka.schema_registry_name", "data.deltastream_store.kafka_with_iam", "kafka.schema_registry_name"),

				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "access_region", "data.deltastream_store.kinesis_creds", "access_region"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "type", "data.deltastream_store.kinesis_creds", "type"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "owner", "data.deltastream_store.kinesis_creds", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "state", "data.deltastream_store.kinesis_creds", "state"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "updated_at", "data.deltastream_store.kinesis_creds", "updated_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "created_at", "data.deltastream_store.kinesis_creds", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.kinesis_creds", "kinesis.uris", "data.deltastream_store.kinesis_creds", "kinesis.uris"),

				resource.TestCheckResourceAttrPair("deltastream_store.databricks", "access_region", "data.deltastream_store.databricks", "access_region"),
				resource.TestCheckResourceAttrPair("deltastream_store.databricks", "type", "data.deltastream_store.databricks", "type"),
				resource.TestCheckResourceAttrPair("deltastream_store.databricks", "owner", "data.deltastream_store.databricks", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_store.databricks", "state", "data.deltastream_store.databricks", "state"),
				resource.TestCheckResourceAttrPair("deltastream_store.databricks", "updated_at", "data.deltastream_store.databricks", "updated_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.databricks", "created_at", "data.deltastream_store.databricks", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.databricks", "databricks.uris", "data.deltastream_store.databricks", "databricks.uris"),
				resource.TestCheckResourceAttrPair("deltastream_store.databricks", "databricks.warehouse_id", "data.deltastream_store.databricks", "databricks.warehouse_id"),
				resource.TestCheckResourceAttrPair("deltastream_store.databricks", "databricks.cloud_s3_bucket", "data.deltastream_store.databricks", "databricks.cloud_s3_bucket"),

				resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "access_region", "data.deltastream_store.snowflake", "access_region"),
				resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "type", "data.deltastream_store.snowflake", "type"),
				resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "owner", "data.deltastream_store.snowflake", "owner"),
				resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "state", "data.deltastream_store.snowflake", "state"),
				resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "updated_at", "data.deltastream_store.snowflake", "updated_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "created_at", "data.deltastream_store.snowflake", "created_at"),
				resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "snowflake.uris", "data.deltastream_store.snowflake", "snowflake.uris"),
				resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "snowflake.account_id", "data.deltastream_store.snowflake", "snowflake.account_id"),
				resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "snowflake.warehouse_name", "data.deltastream_store.snowflake", "snowflake.warehouse_name"),
				resource.TestCheckResourceAttrPair("deltastream_store.snowflake", "snowflake.role_name", "data.deltastream_store.snowflake", "snowflake.role_name"),

				// child entities
				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					topicNames := []string{}
					r := regexp.MustCompile("child_entities.[0-9]+")
					for k, v := range s.RootModule().Resources["data.deltastream_entities.confluent_kafka_with_sasl"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							topicNames = append(topicNames, v)
						}
					}

					expectedTopics := []string{"pageviews"}
					if !util.ArrayContains(expectedTopics, topicNames) {
						return fmt.Errorf("Topic names %v not found in list: %v", expectedTopics, topicNames)
					}

					return nil
				}),

				resource.ComposeTestCheckFunc(func(s *terraform.State) error {
					entNames := []string{}
					r := regexp.MustCompile("child_entities.[0-9]+")
					for k, v := range s.RootModule().Resources["data.deltastream_entities.databricks"].Primary.Attributes {
						if ok := r.MatchString(k); ok {
							entNames = append(entNames, v)
						}
					}

					expectedEnt := []string{"system"}
					if !util.ArrayContains(expectedEnt, entNames) {
						return fmt.Errorf("Entity names %v not found in list: %v", expectedEnt, entNames)
					}

					return nil
				}),
			),
		}},
	})
}
