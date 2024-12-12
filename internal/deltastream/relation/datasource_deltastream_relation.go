// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package relation

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
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
	Database  types.String `tfsdk:"database"`
	Schema    types.String `tfsdk:"schema"`
	Name      types.String `tfsdk:"name"`
	FQN       types.String `tfsdk:"fqn"`
	Owner     types.String `tfsdk:"owner"`
	Type      types.String `tfsdk:"type"`
	State     types.String `tfsdk:"state"`
	CreatedAt types.String `tfsdk:"created_at"`
	UpdatedAt types.String `tfsdk:"updated_at"`
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

	row := conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT relation_type, "owner", "state", created_at, updated_at FROM deltastream.sys."relations" WHERE database_name = '%s' AND schema_name = '%s' AND name = '%s';`, rel.Database.ValueString(), rel.Schema.ValueString(), rel.Name.ValueString()))
	if err := row.Err(); err != nil {
		if err == sql.ErrNoRows {
			resp.Diagnostics.AddError("error loading relation", "relation not found")
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read relation", err)
		return
	}

	var (
		kind      string
		owner     string
		state     string
		createdAt time.Time
		updatedAt time.Time
	)
	if err := row.Scan(&kind, &owner, &state, &createdAt, &updatedAt); err != nil {
	}
	rel.FQN = types.StringValue(fmt.Sprintf("%s.%s.%s", rel.Database.ValueString(), rel.Schema.ValueString(), rel.Name.ValueString()))
	rel.Owner = types.StringValue(owner)
	rel.Type = types.StringValue(kind)
	rel.State = types.StringValue(state)
	rel.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	rel.UpdatedAt = types.StringValue(createdAt.Format(time.RFC3339))

	resp.Diagnostics.Append(resp.State.Set(ctx, &rel)...)
}
