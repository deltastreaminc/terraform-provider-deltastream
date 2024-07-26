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
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
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
	Uris                    basetypes.StringValue `tfsdk:"uris"`
	SchemaRegistryName      basetypes.StringValue `tfsdk:"schema_registry_name"`
	TlsDisabled             basetypes.BoolValue   `tfsdk:"tls_disabled"`
	TlsVerifyServerHostname basetypes.BoolValue   `tfsdk:"tls_verify_server_hostname"`
}

func (_ KafkaDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":                       types.StringType,
		"schema_registry_name":       types.StringType,
		"tls_disabled":               types.BoolType,
		"tls_verify_server_hostname": types.BoolType,
	}
}

type ConfluentKafkaDatasourceProperties struct {
	Uris               basetypes.StringValue `tfsdk:"uris"`
	SchemaRegistryName basetypes.StringValue `tfsdk:"schema_registry_name"`
}

func (_ ConfluentKafkaDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":                 types.StringType,
		"schema_registry_name": types.StringType,
	}
}

type KinesisDatasourceProperties struct {
	Uris               basetypes.StringValue `tfsdk:"uris"`
	SchemaRegistryName basetypes.StringValue `tfsdk:"schema_registry_name"`
}

func (_ KinesisDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":                 types.StringType,
		"schema_registry_name": types.StringType,
	}
}

type SnowflakeDatasourceProperties struct {
	Uris          basetypes.StringValue `tfsdk:"uris"`
	AccountId     basetypes.StringValue `tfsdk:"account_id"`
	WarehouseName basetypes.StringValue `tfsdk:"warehouse_name"`
	RoleName      basetypes.StringValue `tfsdk:"role_name"`
}

func (_ SnowflakeDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":           types.StringType,
		"account_id":     types.StringType,
		"warehouse_name": types.StringType,
		"role_name":      types.StringType,
	}
}

type DatabricksDatasourceProperties struct {
	Uris          basetypes.StringValue `tfsdk:"uris"`
	WarehouseId   basetypes.StringValue `tfsdk:"warehouse_id"`
	CloudS3Bucket basetypes.StringValue `tfsdk:"cloud_s3_bucket"`
	CloudRegion   basetypes.StringValue `tfsdk:"cloud_region"`
}

func (_ DatabricksDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":            types.StringType,
		"warehouse_id":    types.StringType,
		"cloud_s3_bucket": types.StringType,
		"cloud_region":    types.StringType,
	}
}

type PostgresDatasourceProperties struct {
	Uris basetypes.StringValue `tfsdk:"uris"`
}

func (_ PostgresDatasourceProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris": types.StringType,
	}
}

type StoreDatasourceData struct {
	Name           basetypes.StringValue `tfsdk:"name"`
	AccessRegion   basetypes.StringValue `tfsdk:"access_region"`
	Type           basetypes.StringValue `tfsdk:"type"`
	Owner          basetypes.StringValue `tfsdk:"owner"`
	State          basetypes.StringValue `tfsdk:"state"`
	Kafka          basetypes.ObjectValue `tfsdk:"kafka"`
	ConfluentKafka basetypes.ObjectValue `tfsdk:"confluent_kafka"`
	Kinesis        basetypes.ObjectValue `tfsdk:"kinesis"`
	Snowflake      basetypes.ObjectValue `tfsdk:"snowflake"`
	Databricks     basetypes.ObjectValue `tfsdk:"databricks"`
	Postgres       basetypes.ObjectValue `tfsdk:"postgres"`
	UpdatedAt      basetypes.StringValue `tfsdk:"updated_at"`
	CreatedAt      basetypes.StringValue `tfsdk:"created_at"`
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
			store.Type = basetypes.NewStringValue(kind)
			store.AccessRegion = basetypes.NewStringValue(accessRegion)
			store.State = basetypes.NewStringValue(state)
			store.Owner = basetypes.NewStringValue(owner)
			store.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			store.UpdatedAt = basetypes.NewStringValue(updatedAt.Format(time.RFC3339))
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
		store.Kafka, dg = basetypes.NewObjectValueFrom(ctx, KafkaDatasourceProperties{}.AttributeTypes(), KafkaDatasourceProperties{
			Uris:                    basetypes.NewStringValue(uri),
			SchemaRegistryName:      basetypes.NewStringPointerValue(schemaRegistryName),
			TlsDisabled:             basetypes.NewBoolValue(!tlsEnabled),
			TlsVerifyServerHostname: basetypes.NewBoolValue(verifyHostname),
		})
	case "confluentkafka":
		store.ConfluentKafka, dg = basetypes.NewObjectValueFrom(ctx, ConfluentKafkaDatasourceProperties{}.AttributeTypes(), ConfluentKafkaDatasourceProperties{
			Uris:               basetypes.NewStringValue(uri),
			SchemaRegistryName: basetypes.NewStringPointerValue(schemaRegistryName),
		})
	case "kinesis":
		store.Kinesis, dg = basetypes.NewObjectValueFrom(ctx, KinesisDatasourceProperties{}.AttributeTypes(), KinesisDatasourceProperties{
			Uris:               basetypes.NewStringValue(uri),
			SchemaRegistryName: basetypes.NewStringPointerValue(schemaRegistryName),
		})
	case "snowflake":
		details := map[string]any{}
		if err := yaml.Unmarshal([]byte(detailsJSON), &details); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to unmarshal databricks details", err)
			return
		}

		store.Snowflake, dg = basetypes.NewObjectValueFrom(ctx, SnowflakeDatasourceProperties{}.AttributeTypes(), SnowflakeDatasourceProperties{
			Uris:          basetypes.NewStringValue(uri),
			AccountId:     basetypes.NewStringValue(details["account_id"].(string)),
			WarehouseName: basetypes.NewStringValue(details["warehouse_name"].(string)),
			RoleName:      basetypes.NewStringValue(details["role_name"].(string)),
		})
	case "databricks":
		details := map[string]any{}
		if err := yaml.Unmarshal([]byte(detailsJSON), &details); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to unmarshal databricks details", err)
			return
		}

		store.Databricks, dg = basetypes.NewObjectValueFrom(ctx, DatabricksDatasourceProperties{}.AttributeTypes(), DatabricksDatasourceProperties{
			Uris:          basetypes.NewStringValue(uri),
			WarehouseId:   basetypes.NewStringValue(details["sql_warehouse_id"].(string)),
			CloudS3Bucket: basetypes.NewStringValue(details["cloud_provider_bucket"].(string)),
			CloudRegion:   basetypes.NewStringValue(details["cloud_provider_region"].(string)),
		})
	case "postgres":
		store.Postgres, dg = basetypes.NewObjectValueFrom(ctx, PostgresDatasourceProperties{}.AttributeTypes(), PostgresDatasourceProperties{
			Uris: basetypes.NewStringValue(uri),
		})
	}
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &store)...)
}
