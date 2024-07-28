// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"sigs.k8s.io/yaml"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var _ datasource.DataSource = &StoreDataSource{}
var _ datasource.DataSourceWithConfigure = &StoreDataSource{}

func NewStoreDataSource() datasource.DataSource {
	return &StoreDataSource{}
}

type StoreDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

type KafkaDatasourceProperties struct {
	Uris                    types.String `tfsdk:"uris"`
	SchemaRegistryName      types.String `tfsdk:"schema_registry_name"`
	TlsDisabled             types.Bool   `tfsdk:"tls_disabled"`
	TlsVerifyServerHostname types.Bool   `tfsdk:"tls_verify_server_hostname"`
}

func (KafkaDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":                       types.StringType,
		"schema_registry_name":       types.StringType,
		"tls_disabled":               types.BoolType,
		"tls_verify_server_hostname": types.BoolType,
	}
}

type ConfluentKafkaDatasourceProperties struct {
	Uris               types.String `tfsdk:"uris"`
	SchemaRegistryName types.String `tfsdk:"schema_registry_name"`
}

func (ConfluentKafkaDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":                 types.StringType,
		"schema_registry_name": types.StringType,
	}
}

type KinesisDatasourceProperties struct {
	Uris               types.String `tfsdk:"uris"`
	SchemaRegistryName types.String `tfsdk:"schema_registry_name"`
}

func (KinesisDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":                 types.StringType,
		"schema_registry_name": types.StringType,
	}
}

type SnowflakeDatasourceProperties struct {
	Uris          types.String `tfsdk:"uris"`
	AccountId     types.String `tfsdk:"account_id"`
	WarehouseName types.String `tfsdk:"warehouse_name"`
	RoleName      types.String `tfsdk:"role_name"`
}

func (SnowflakeDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":           types.StringType,
		"account_id":     types.StringType,
		"warehouse_name": types.StringType,
		"role_name":      types.StringType,
	}
}

type DatabricksDatasourceProperties struct {
	Uris          types.String `tfsdk:"uris"`
	WarehouseId   types.String `tfsdk:"warehouse_id"`
	CloudS3Bucket types.String `tfsdk:"cloud_s3_bucket"`
	CloudRegion   types.String `tfsdk:"cloud_region"`
}

func (DatabricksDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":            types.StringType,
		"warehouse_id":    types.StringType,
		"cloud_s3_bucket": types.StringType,
		"cloud_region":    types.StringType,
	}
}

type PostgresDatasourceProperties struct {
	Uris types.String `tfsdk:"uris"`
}

func (PostgresDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris": types.StringType,
	}
}

type StoreDatasourceData struct {
	Name           types.String `tfsdk:"name"`
	AccessRegion   types.String `tfsdk:"access_region"`
	Type           types.String `tfsdk:"type"`
	Owner          types.String `tfsdk:"owner"`
	State          types.String `tfsdk:"state"`
	Kafka          types.Object `tfsdk:"kafka"`
	ConfluentKafka types.Object `tfsdk:"confluent_kafka"`
	Kinesis        types.Object `tfsdk:"kinesis"`
	Snowflake      types.Object `tfsdk:"snowflake"`
	Databricks     types.Object `tfsdk:"databricks"`
	Postgres       types.Object `tfsdk:"postgres"`
	UpdatedAt      types.String `tfsdk:"updated_at"`
	CreatedAt      types.String `tfsdk:"created_at"`
}

func (d *StoreDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	cfg, ok := req.ProviderData.(*config.DeltaStreamProviderCfg)
	if !ok {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "provider error", fmt.Errorf("invalid provider data"))
		return
	}

	d.cfg = cfg
}

func (d *StoreDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Store resource",

		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the Store",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"access_region": schema.StringAttribute{
				Description: "Specifies the region of the Store. In order to improve latency and reduce data transfer costs, the region should be the same cloud and region that the physical Store is running in.",
				Computed:    true,
			},
			"type": schema.StringAttribute{
				Description: "Type of the Store",
				Computed:    true,
			},

			"kafka": schema.SingleNestedAttribute{
				Description: "Kafka specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Computed:    true,
					},
					"schema_registry_name": schema.StringAttribute{
						Description: "Name of the schema registry",
						Computed:    true,
					},
					"tls_disabled": schema.BoolAttribute{
						Description: "Specifies if the store should be accessed over TLS",
						Computed:    true,
					},
					"tls_verify_server_hostname": schema.BoolAttribute{
						Description: "Specifies if the server CNAME should be validated against the certificate",
						Computed:    true,
					},
				},
				Optional: true,
			},

			"confluent_kafka": schema.SingleNestedAttribute{
				Description: "Confluent Kafka specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Computed:    true,
					},
					"schema_registry_name": schema.StringAttribute{
						Description: "Name of the schema registry",
						Computed:    true,
					},
				},
				Optional: true,
			},

			"kinesis": schema.SingleNestedAttribute{
				Description: "Kinesis specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Computed:    true,
					},
					"schema_registry_name": schema.StringAttribute{
						Description: "Name of the schema registry",
						Computed:    true,
					},
				},
				Optional: true,
			},

			"snowflake": schema.SingleNestedAttribute{
				Description: "Snowflake specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Computed:    true,
					},
					"account_id": schema.StringAttribute{
						Description: "Snowflake account ID",
						Computed:    true,
					},
					"warehouse_name": schema.StringAttribute{
						Description: "Warehouse name to use for queries and other store operations that require compute resource",
						Computed:    true,
					},
					"role_name": schema.StringAttribute{
						Description: "Access control role to use for the Store operations after connecting to Snowflake",
						Computed:    true,
					},
				},
				Optional: true,
			},

			"databricks": schema.SingleNestedAttribute{
				Description: "Databricks specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Computed:    true,
					},
					"warehouse_id": schema.StringAttribute{
						Description: "The identifier for a Databricks SQL Warehouse belonging to a Databricks workspace. This Warehouse will be used to create and query Tables in Databricks",
						Computed:    true,
					},
					"cloud_s3_bucket": schema.StringAttribute{
						Description: "The name of the S3 bucket where the data will be stored",
						Computed:    true,
					},
					"cloud_region": schema.StringAttribute{
						Description: "The region where the S3 bucket is located",
						Computed:    true,
					},
				},
				Optional: true,
			},

			"postgres": schema.SingleNestedAttribute{
				Description: "Postgres specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Computed:    true,
					},
				},
				Optional: true,
			},

			"owner": schema.StringAttribute{
				Description: "Owning role of the Store",
				Computed:    true,
			},
			"state": schema.StringAttribute{
				Description: "State of the Store",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the Store",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Last update date of the Store",
				Computed:    true,
			},
		},
	}
}

func (d *StoreDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_store"
}

func (d *StoreDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	store := StoreDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &store)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, `LIST STORES;`)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read stores", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var discard any
		var name string
		var accessRegion string
		var kind string
		var state string
		var owner string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&name, &kind, &accessRegion, &state, &discard, &owner, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read stores", err)
			return
		}
		if name == store.Name.ValueString() {
			store.Type = types.StringValue(kind)
			store.AccessRegion = types.StringValue(accessRegion)
			store.State = types.StringValue(state)
			store.Owner = types.StringValue(owner)
			store.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
			store.UpdatedAt = types.StringValue(updatedAt.Format(time.RFC3339))
			break
		}
	}

	row := conn.QueryRowContext(ctx, fmt.Sprintf(`DESCRIBE STORE "%s";`, store.Name.ValueString()))
	var metadataJSON string
	var uri string
	var detailsJSON string
	var tlsEnabled bool
	var verifyHostname bool
	var schemaRegistryName *string
	if err := row.Scan(&metadataJSON, &uri, &detailsJSON, &tlsEnabled, &verifyHostname, &schemaRegistryName); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read store details", err)
		return
	}

	var dg diag.Diagnostics
	switch strings.ToLower(store.Type.ValueString()) {
	case "kafka":
		store.Kafka, dg = types.ObjectValueFrom(ctx, KafkaDatasourceProperties{}.AttributeTypes(), KafkaDatasourceProperties{
			Uris:                    types.StringValue(uri),
			SchemaRegistryName:      types.StringPointerValue(schemaRegistryName),
			TlsDisabled:             types.BoolValue(!tlsEnabled),
			TlsVerifyServerHostname: types.BoolValue(verifyHostname),
		})
	case "confluentkafka":
		store.ConfluentKafka, dg = types.ObjectValueFrom(ctx, ConfluentKafkaDatasourceProperties{}.AttributeTypes(), ConfluentKafkaDatasourceProperties{
			Uris:               types.StringValue(uri),
			SchemaRegistryName: types.StringPointerValue(schemaRegistryName),
		})
	case "kinesis":
		store.Kinesis, dg = types.ObjectValueFrom(ctx, KinesisDatasourceProperties{}.AttributeTypes(), KinesisDatasourceProperties{
			Uris:               types.StringValue(uri),
			SchemaRegistryName: types.StringPointerValue(schemaRegistryName),
		})
	case "snowflake":
		details := map[string]any{}
		if err := yaml.Unmarshal([]byte(detailsJSON), &details); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to unmarshal databricks details", err)
			return
		}

		store.Snowflake, dg = types.ObjectValueFrom(ctx, SnowflakeDatasourceProperties{}.AttributeTypes(), SnowflakeDatasourceProperties{
			Uris:          types.StringValue(uri),
			AccountId:     types.StringValue(details["account_id"].(string)),
			WarehouseName: types.StringValue(details["warehouse_name"].(string)),
			RoleName:      types.StringValue(details["role_name"].(string)),
		})
	case "databricks":
		details := map[string]any{}
		if err := yaml.Unmarshal([]byte(detailsJSON), &details); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to unmarshal databricks details", err)
			return
		}

		store.Databricks, dg = types.ObjectValueFrom(ctx, DatabricksDatasourceProperties{}.AttributeTypes(), DatabricksDatasourceProperties{
			Uris:          types.StringValue(uri),
			WarehouseId:   types.StringValue(details["sql_warehouse_id"].(string)),
			CloudS3Bucket: types.StringValue(details["cloud_provider_bucket"].(string)),
			CloudRegion:   types.StringValue(details["cloud_provider_region"].(string)),
		})
	case "postgres":
		store.Postgres, dg = types.ObjectValueFrom(ctx, PostgresDatasourceProperties{}.AttributeTypes(), PostgresDatasourceProperties{
			Uris: types.StringValue(uri),
		})
	}
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &store)...)
}
