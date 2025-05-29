// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	gods "github.com/deltastreaminc/go-deltastream"
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
	Type           types.String `tfsdk:"type"`
	Owner          types.String `tfsdk:"owner"`
	State          types.String `tfsdk:"state"`
	Kafka          types.Object `tfsdk:"kafka"`
	ConfluentKafka types.Object `tfsdk:"confluent_kafka"`
	Kinesis        types.Object `tfsdk:"kinesis"`
	Snowflake      types.Object `tfsdk:"snowflake"`
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

	dsql, err := util.ExecTemplate(lookupStoreTmpl, map[string]any{
		"Name": store.Name.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
	}
	row := conn.QueryRowContext(ctx, dsql)
	if row.Err() != nil {
		if errors.Is(row.Err(), sql.ErrNoRows) {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read store details", &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidStore})
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read store details", row.Err())
		return
	}

	var kind string
	var state string
	var owner string
	var createdAt time.Time
	var updatedAt time.Time
	if err := row.Scan(&kind, &state, &owner, &createdAt, &updatedAt); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read store details", err)
		return
	}

	store.Type = types.StringValue(kind)
	store.State = types.StringValue(state)
	store.Owner = types.StringValue(owner)
	store.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	store.UpdatedAt = types.StringValue(updatedAt.Format(time.RFC3339))
	store.Owner = types.StringValue(owner)
	store.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))

	dsql, err = util.ExecTemplate(describeStoreTmpl, map[string]any{
		"Name": store.Name.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
	}
	row = conn.QueryRowContext(ctx, dsql)
	var metadataJSON string
	var uri string
	var detailsJSON string
	var tlsEnabled bool
	var verifyHostname bool
	var schemaRegistryName *string
	var storePath string
	if err := row.Scan(&metadataJSON, &uri, &detailsJSON, &tlsEnabled, &verifyHostname, &schemaRegistryName, &storePath); err != nil {
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
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to unmarshal snowflake details", err)
			return
		}

		store.Snowflake, dg = types.ObjectValueFrom(ctx, SnowflakeDatasourceProperties{}.AttributeTypes(), SnowflakeDatasourceProperties{
			Uris:          types.StringValue(uri),
			AccountId:     types.StringValue(details["account_id"].(string)),
			WarehouseName: types.StringValue(details["warehouse_name"].(string)),
			RoleName:      types.StringValue(details["role_name"].(string)),
		})
	case "postgres":
		store.Postgres, dg = types.ObjectValueFrom(ctx, PostgresDatasourceProperties{}.AttributeTypes(), PostgresDatasourceProperties{
			Uris: types.StringValue(uri),
		})
	}
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &store)...)
}
