// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package relation

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

var _ datasource.DataSource = &RelationDataSource{}
var _ datasource.DataSourceWithConfigure = &RelationDataSource{}

func NewRelationDataSource() datasource.DataSource {
	return &RelationDataSource{}
}

type RelationDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *RelationDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

type RelationDataSourceData struct {
	Database  basetypes.StringValue `tfsdk:"database"`
	Schema    basetypes.StringValue `tfsdk:"schema"`
	Name      basetypes.StringValue `tfsdk:"name"`
	FQN       basetypes.StringValue `tfsdk:"fqn"`
	Owner     basetypes.StringValue `tfsdk:"owner"`
	Type      basetypes.StringValue `tfsdk:"type"`
	State     basetypes.StringValue `tfsdk:"state"`
	CreatedAt basetypes.StringValue `tfsdk:"created_at"`
	UpdatedAt basetypes.StringValue `tfsdk:"updated_at"`
}

func (d *RelationDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Relation resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
			},
			"schema": schema.StringAttribute{
				Description: "Name of the Schema",
				Required:    true,
			},
			"name": schema.StringAttribute{
				Description: "Name of the Schema",
				Required:    true,
			},
			"fqn": schema.StringAttribute{
				Description: "Fully qualified name of the Relation",
				Computed:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the relation",
				Computed:    true,
			},
			"type": schema.StringAttribute{
				Description: "Type of the Relation",
				Computed:    true,
			},
			"state": schema.StringAttribute{
				Description: "State of the Relation",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the relation",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Creation date of the relation",
				Computed:    true,
			},
		},
	}
}

func (d *RelationDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_relation"
}

func (d *RelationDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	rel := RelationDataSourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &rel)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`LIST RELATIONS IN SCHEMA "%s"."%s";`, rel.Database.ValueString(), rel.Schema.ValueString()))
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list schemas", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name           string
			kind           string
			owner          string
			state          string
			propertiesJSON string
			createdAt      time.Time
			updatedAt      time.Time
		)

		if err := rows.Scan(&name, &kind, &owner, &state, &propertiesJSON, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read relation", err)
			return
		}
		if name == rel.Name.ValueString() {
			rel.FQN = basetypes.NewStringValue(fmt.Sprintf("%s.%s.%s", rel.Database.ValueString(), rel.Schema.ValueString(), name))
			rel.Owner = basetypes.NewStringValue(owner)
			rel.Type = basetypes.NewStringValue(kind)
			rel.State = basetypes.NewStringValue(state)
			rel.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			rel.UpdatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			break
		}
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &rel)...)
}
