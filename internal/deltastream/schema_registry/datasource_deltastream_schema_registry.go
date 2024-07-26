// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package schemaregistry

import (
	"context"
	"fmt"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
)

var _ datasource.DataSource = &SchemaRegistryDataSource{}
var _ datasource.DataSourceWithConfigure = &SchemaRegistryDataSource{}

func NewSchemaRegistryDataSource() datasource.DataSource {
	return &SchemaRegistryDataSource{}
}

type SchemaRegistryDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *SchemaRegistryDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	cfg, ok := req.ProviderData.(*config.DeltaStreamProviderCfg)
	if !ok {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "internal error", fmt.Errorf("invalid provider data"))
		return
	}

	d.cfg = cfg
}

func (d *SchemaRegistryDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Schema registry datasource",

		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the schema registry",
				Required:    true,
			},
			"type": schema.StringAttribute{
				Description: "Type of the schema registry",
				Computed:    true,
			},
			// "access_region": schema.StringAttribute{
			// 	Description: "Specifies the region of the schema registry",
			// 	Computed:    true,
			// },
			"state": schema.StringAttribute{
				Description: "State of the schema registry",
				Computed:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the schema registry",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the schema registry",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Last update date of the schema registry",
				Computed:    true,
			},
		},
	}
}

func (d *SchemaRegistryDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_schema_registry"
}

func (d *SchemaRegistryDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	sr := SchemaRegistryDatasourceDataItem{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &sr)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	if err := util.SetSqlContext(ctx, conn, &d.cfg.Role, nil, nil, nil); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	rows, err := conn.QueryContext(ctx, `LIST SCHEMA_REGISTRIES;`)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list schema registry", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var discard any
		var name string
		// var accessRegion string
		var kind string
		var state string
		var owner string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&name, &kind, &state, &discard, &owner, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read schema registry", err)
			return
		}
		if name != sr.Name.ValueString() {
			continue
		}

		sr.Type = basetypes.NewStringValue(kind)
		sr.State = basetypes.NewStringValue(state)
		// sr.AccessRegion= basetypes.NewStringValue(accessRegion)
		sr.Owner = basetypes.NewStringValue(owner)
		sr.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
		sr.UpdatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
		break
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &sr)...)
}
