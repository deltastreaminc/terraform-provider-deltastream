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
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
)

var _ datasource.DataSource = &RelationsDataSource{}
var _ datasource.DataSourceWithConfigure = &RelationsDataSource{}

func NewRelationsDataSource() datasource.DataSource {
	return &RelationsDataSource{}
}

type RelationsDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *RelationsDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

type RelationsDataSourceData struct {
	Database  types.String `tfsdk:"database"`
	Schema    types.String `tfsdk:"schema"`
	Relations types.List   `tfsdk:"relations"`
}

func (d *RelationsDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
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
			"relations": schema.ListNestedAttribute{
				Description: "List of schemas",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"database": schema.StringAttribute{
							Description: "Name of the Database",
							Computed:    true,
						},
						"schema": schema.StringAttribute{
							Description: "Name of the Schema",
							Computed:    true,
						},
						"name": schema.StringAttribute{
							Description: "Name of the Schema",
							Computed:    true,
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
				},
			},
		},
	}
}

func (d *RelationsDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_relations"
}

func (d *RelationsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	rels := RelationsDataSourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &rels)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(listRelationsTmpl, map[string]any{
		"DatabaseName": rels.Database.ValueString(),
		"SchemaName":   rels.Schema.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	rows, err := conn.QueryContext(ctx, dsql)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to load relations", err)
		return
	}
	defer rows.Close()

	var (
		name      string
		fqn       string
		kind      string
		owner     string
		state     string
		createdAt time.Time
		updatedAt time.Time
	)

	relList := []RelationDataSourceData{}
	for rows.Next() {
		rel := RelationDataSourceData{
			Database: rels.Database,
			Schema:   rels.Schema,
		}
		if err := rows.Scan(&name, &fqn, &kind, &owner, &state, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read relation", err)
			return
		}

		rel.Name = types.StringValue(name)
		rel.FQN = types.StringValue(fqn)
		rel.Owner = types.StringValue(owner)
		rel.Type = types.StringValue(kind)
		rel.State = types.StringValue(state)
		rel.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
		rel.UpdatedAt = types.StringValue(createdAt.Format(time.RFC3339))
		relList = append(relList, rel)
	}

	var dg diag.Diagnostics
	rels.Relations, dg = basetypes.NewListValueFrom(ctx, rels.Relations.ElementType(ctx), relList)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &rels)...)
}
